package pgmodelparse

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/alexrjones/pgmodelparse/collections"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

type Compiler struct {
	SearchPath   string
	Catalog      *Catalog
	TypeRegistry *TypeRegistry
}

func NewCompiler() *Compiler {
	c := &Compiler{
		SearchPath: "public",
		Catalog: &Catalog{
			Schemas: collections.NewOrderedMap[string, *Schema](),
			PgConstraint: &PgConstraint{
				ByColumn:   collections.NewMultimap[*Column, *Constraint](),
				Constrains: collections.NewMultimap[*Column, *Constraint](),
				Refers:     collections.NewMultimap[*Column, *Constraint](),
				ByName:     make(map[string]*Constraint),
			},
		},
		TypeRegistry: NewTypeRegistry(),
	}
	defaultSchema := &Schema{
		Name:   "public",
		Tables: collections.NewOrderedMap[string, *Table](),
	}
	c.Catalog.Schemas.Add(defaultSchema.Name, defaultSchema)
	return c
}

func (c *Compiler) ParseRaw(sqlFile string) error {

	parse, err := pg_query.Parse(sqlFile)
	if err != nil {
		return err
	}
	return c.ParseStatements(parse)
}

func (c *Compiler) ParseStatements(parse *pg_query.ParseResult) error {

	for _, stmt := range parse.Stmts {
		switch p := stmt.Stmt.Node.(type) {
		case *pg_query.Node_CreateSchemaStmt:
			{
				err := c.CreateSchema(p.CreateSchemaStmt)
				if err != nil {
					return fmt.Errorf("while creating schema: %w", err)
				}
			}
		case *pg_query.Node_CreateStmt:
			{
				err := c.CreateTable(p.CreateStmt)
				if err != nil {
					return fmt.Errorf("while creating table: %w", err)
				}
			}
		case *pg_query.Node_AlterTableStmt:
			{
				err := c.AlterTable(p.AlterTableStmt)
				if err != nil {
					return fmt.Errorf("while altering table: %w", err)
				}
			}
		case *pg_query.Node_DropStmt:
			{
				dropBehaviour := DropBehaviourRestrict
				if p.DropStmt.Behavior == pg_query.DropBehavior_DROP_CASCADE {
					dropBehaviour = DropBehaviourCascade
				}
				switch p.DropStmt.RemoveType {
				case pg_query.ObjectType_OBJECT_TABLE:
					{
						for _, tgt := range p.DropStmt.Objects {
							l := tgt.Node.(*pg_query.Node_List)
							schema, table := ObjectNameFromList(l.List)
							err := c.DropTable(schema, table, dropBehaviour)
							if err != nil {
								return err
							}
						}
					}
				}
			}
		case *pg_query.Node_RenameStmt:
			{
				tab, err := c.FindTableFromRangeVar(p.RenameStmt.Relation)
				if err != nil {
					return err
				}
				err = c.RenameTable(tab, p.RenameStmt.Newname)
				if err != nil {
					return err
				}
			}
		case *pg_query.Node_CreateEnumStmt:
			{
				err := c.CreateEnum(p.CreateEnumStmt)
				if err != nil {
					return err
				}
			}
		default:
			//fmt.Printf("unknown how to process type %T\n", p)
		}
	}

	return nil
}

func (c *Compiler) CreateSchema(stmt *pg_query.CreateSchemaStmt) error {
	_, exists := c.Catalog.Schemas.Get(stmt.Schemaname)
	if exists && !stmt.IfNotExists {
		return fmt.Errorf("schema already exists")
	} else if exists && stmt.IfNotExists {
		return nil
	}
	sch := &Schema{
		Name:   stmt.Schemaname,
		Tables: collections.NewOrderedMap[string, *Table](),
	}
	c.Catalog.Schemas.Add(sch.Name, sch)
	return nil
}

func (c *Compiler) CreateTable(stmt *pg_query.CreateStmt) error {
	name := stmt.Relation.Relname
	schemaName := c.SchemaOrSearchPath(stmt.Relation.Schemaname)
	table := NewTable(name, schemaName)
	err := c.Catalog.AddTable(table)
	if err != nil {
		return err
	}
	for _, n := range stmt.TableElts {
		switch p := n.Node.(type) {
		case *pg_query.Node_ColumnDef:
			{
				err = c.DefineColumn(table, p.ColumnDef)
				if err != nil {
					return fmt.Errorf("defining column on table %s.%s: %w", table.Schema, table.Name, err)
				}
			}
		case *pg_query.Node_Constraint:
			{
				err = c.DefineConstraint(table, "", p.Constraint)
				if err != nil {
					return fmt.Errorf("defining constraint on table %s.%s: %w", table.Schema, table.Name, err)
				}
			}
		}
	}
	return nil
}

func (c *Compiler) DropTable(schema, table string, behav DropBehaviour) error {

	tab, err := c.FindTableFromSchemaAndName(schema, table)
	if err != nil {
		return err
	}
	consToRemove := make(Constraints, 0, 5)
	for _, col := range tab.Columns.List() {
		cons, ok := c.Catalog.PgConstraint.ByColumn.Get(col)
		if !ok {
			continue
		}
		for _, con := range cons {
			if con.Type == ConstraintTypeForeignKey &&
				slices.Contains(con.Refers, col) &&
				behav != DropBehaviourCascade {
				return fmt.Errorf("can't drop table %s because constraint %s refers to it and cascade was not specified",
					tab.Name, con.Name)
			}
			consToRemove = append(consToRemove, con)
		}
	}
	for _, con := range consToRemove {
		c.Catalog.PgConstraint.RemoveConstraint(con)
	}
	sch, _ := c.Catalog.Schemas.Get(tab.Schema) // Must be ok
	sch.Tables.Remove(tab.Name)
	return nil
}

func (c *Compiler) AlterTable(stmt *pg_query.AlterTableStmt) error {

	tab, err := c.FindTableFromRangeVar(stmt.Relation)
	if err != nil {
		return err
	}

	for _, cmd := range stmt.Cmds {
		atc, ok := cmd.Node.(*pg_query.Node_AlterTableCmd)
		if !ok {
			return fmt.Errorf("expected AlterTableCmd but got %T", cmd.Node)
		}
		switch atc.AlterTableCmd.Subtype {
		case pg_query.AlterTableType_AT_AddColumn:
			{
				col, ok := atc.AlterTableCmd.Def.Node.(*pg_query.Node_ColumnDef)
				if !ok {
					return fmt.Errorf("expected ColumnDef but got %T", atc.AlterTableCmd.Def.Node)
				}
				err = c.DefineColumn(tab, col.ColumnDef)
				if err != nil {
					return err
				}
			}
		case pg_query.AlterTableType_AT_DropColumn:
			{
				err = c.DropColumn(tab, atc.AlterTableCmd.Name, atc.AlterTableCmd.Behavior)
				if err != nil {
					return err
				}
			}
		case pg_query.AlterTableType_AT_AlterColumnType:
			{
				err = c.AlterColumnType(tab, atc.AlterTableCmd.Name, atc.AlterTableCmd.Def.Node.(*pg_query.Node_ColumnDef).ColumnDef)
				if err != nil {
					return err
				}
			}
		case pg_query.AlterTableType_AT_AddConstraint:
			{
				conDef, ok := atc.AlterTableCmd.Def.Node.(*pg_query.Node_Constraint)
				if !ok {
					return fmt.Errorf("expected Constraint but got %T", atc.AlterTableCmd.Def.Node)
				}
				err = c.DefineConstraint(tab, "", conDef.Constraint)
				if err != nil {
					return err
				}
			}
		case pg_query.AlterTableType_AT_ColumnDefault:
			{
				if atc.AlterTableCmd.Def == nil {
					err = c.AlterColumnDropDefault(tab, atc.AlterTableCmd.Name)
					if err != nil {
						return err
					}
				}
			}
		case pg_query.AlterTableType_AT_DropConstraint:
			{
				fqname := ConstraintFQName(tab, atc.AlterTableCmd.Name)
				cons, ok := c.Catalog.PgConstraint.ByName[fqname]
				if !ok {
					return fmt.Errorf("while dropping constraint: constraint %s not found", fqname)
				}
				c.Catalog.PgConstraint.RemoveConstraint(cons)
			}
		case pg_query.AlterTableType_AT_DropNotNull:
			{
				col, err := ColumnFromColName(tab, atc.AlterTableCmd.Name)
				if err != nil {
					return err
				}
				if col.Attrs.Pkey {
					return fmt.Errorf("can't drop not null constraint from primary key column %s.%s", tab.Name, col.Name)
				}
				if !col.Attrs.NotNull {
					return fmt.Errorf("can't drop not null constraint from nullable column %s.%s", tab.Name, col.Name)
				}
				col.Attrs.NotNull = false
			}
		case pg_query.AlterTableType_AT_SetNotNull:
			{
				col, err := ColumnFromColName(tab, atc.AlterTableCmd.Name)
				if err != nil {
					return err
				}
				col.Attrs.NotNull = true
			}
		}
	}
	return nil
}

func (c *Compiler) DefineColumn(t *Table, def *pg_query.ColumnDef) error {
	name := def.Colname
	pgType := c.TypeFromNode(def.TypeName)
	seqName := c.DetermineAutomaticSequenceName(t.Name, name, pgType)
	err := t.AddColumn(&Column{
		Table: t,
		Name:  name,
		Type:  pgType,
		Attrs: &ColumnAttributes{HasSequence: pgType.IsSerial, SequenceName: seqName},
	})
	if err != nil {
		return err
	}
	err = c.DefineConstraints(t, name, def.Constraints)
	if err != nil {
		return err
	}
	return nil
}

func (c *Compiler) RenameTable(t *Table, newName string) error {

	sch, ok := c.Catalog.Schemas.Get(t.Schema)
	if !ok {
		return fmt.Errorf("did not find schema %s", t.Schema)
	}
	if _, ok = sch.Tables.Get(newName); ok {
		return fmt.Errorf("schema %s already has table %s", t.Schema, newName)
	}
	sch.Tables.Remove(t.Name)
	t.Name = newName
	sch.Tables.Add(newName, t)
	return nil
}

func (c *Compiler) DetermineAutomaticSequenceName(tabName, colName string, typ *PostgresType) string {

	if !typ.IsSerial {
		return ""
	}
	return strings.Join([]string{tabName, colName, "seq"}, "_")
}

func (c *Compiler) AlterColumnDropDefault(t *Table, colName string) error {

	v, ok := t.Columns.Get(colName)
	if !ok {
		return fmt.Errorf("column %s not found on table %s", colName, t.FQName())
	}
	if !(v.Attrs.HasSequence || v.Attrs.HasExplicitDefault) {
		return fmt.Errorf("column %s on table %s does not have a default to drop", colName, t.FQName())
	}
	if v.Attrs.HasSequence {
		v.Attrs.HasSequence = false
		v.Attrs.SequenceName = ""
		if v.Type.IsSerial {
			v.Type = v.Type.NonSerialType
		}
		return nil
	}
	if v.Attrs.HasExplicitDefault {
		v.Attrs.HasExplicitDefault = false
		v.Attrs.ColumnDefault = ""
	}
	return nil
}

func (c *Compiler) DropColumn(t *Table, colName string, behavior pg_query.DropBehavior) error {

	col, ok := t.Columns.Get(colName)
	if !ok {
		return fmt.Errorf("column %s does not exist", colName)
	}
	depends, _ := c.Catalog.PgConstraint.ByColumn.Get(col)
	var funcs []func()
	for _, con := range depends {
		if con.DropBehaviour == DropBehaviourRestrict {
			if behavior != pg_query.DropBehavior_DROP_CASCADE && !con.Constrains.IsExactlyColumn(col) {
				return fmt.Errorf("can't drop %s because %s depends on it", col.Name, con.Name)
			}
		}
		funcs = append(funcs, func() {
			c.Catalog.PgConstraint.RemoveConstraint(con)
		})
	}

	for _, fn := range funcs {
		fn()
	}
	c.Catalog.PgConstraint.ByColumn.Remove(col)
	t.Columns.Remove(col.Name)
	return nil
}

func (c *Compiler) AlterColumnType(t *Table, colName string, def *pg_query.ColumnDef) error {
	col, ok := t.Columns.Get(colName)
	if !ok {
		return fmt.Errorf("column %s not found on table %s", colName, t.FQName())
	}
	newType := c.TypeFromNode(def.TypeName)
	if !CanCast(col.Type, newType) {
		return fmt.Errorf("can't alter column type: can't cast from type %s to type %s (or not implemented)", col.Type.Name, newType.Name)
	}
	col.Type = newType
	return nil
}

// FindTableFromRangeVar looks up an existing table from the provided RangeVar.
func (c *Compiler) FindTableFromRangeVar(r *pg_query.RangeVar) (*Table, error) {

	name := r.Relname
	schemaName := r.Schemaname
	return c.FindTableFromSchemaAndName(schemaName, name)
}

func (c *Compiler) FindTableFromSchemaAndName(schemaName, name string) (*Table, error) {
	schemaName = c.SchemaOrSearchPath(schemaName)
	sch, ok := c.Catalog.Schemas.Get(schemaName)
	if !ok {
		return nil, fmt.Errorf("couldn't find schema %s", schemaName)
	}
	tab, ok := sch.Tables.Get(name)
	if !ok {
		return nil, fmt.Errorf("couldn't find table %s", name)
	}
	return tab, nil
}

func (c *Compiler) TypeFromNode(tn *pg_query.TypeName) *PostgresType {

	var parts []string
	for _, n := range tn.Names {
		val := StringOrPanic(n)
		if val == "pg_catalog" { // TODO is this correct
			continue
		}
		parts = append(parts, val)
	}
	return c.TypeRegistry.MatchType(strings.Join(parts, "."))
}

func (c *Compiler) DefineConstraints(t *Table, colName string, constraints []*pg_query.Node) error {
	for _, n := range constraints {
		v, ok := n.Node.(*pg_query.Node_Constraint)
		if !ok {
			panic("unknown how to parse node " + n.String())
		}
		err := c.DefineConstraint(t, colName, v.Constraint)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Compiler) DefineConstraint(t *Table, colName string, v *pg_query.Constraint) error {

	switch v.Contype {
	case pg_query.ConstrType_CONSTR_PRIMARY:
		{
			var cols Columns
			for _, k := range v.Keys {
				col, err := ColumnFromColName(t, StringOrPanic(k))
				if err != nil {
					return err
				}
				cols = append(cols, col)
			}
			if len(cols) == 0 {
				col, err := ColumnFromColName(t, colName)
				if err != nil {
					return err
				}
				cols = append(cols, col)
			}
			name := v.Conname
			if name == "" {
				name = t.Name + "_" + "pkey"
			}
			con := &Constraint{Table: t, Name: name, Type: ConstraintTypePrimary, Constrains: cols}
			c.Catalog.PgConstraint.AddConstraint(con)
			return nil
		}
	case pg_query.ConstrType_CONSTR_NOTNULL:
		{
			col, err := ColumnFromColName(t, colName)
			if err != nil {
				return err
			}
			col.Attrs.NotNull = true
			return nil
		}
	case pg_query.ConstrType_CONSTR_DEFAULT:
		{
			col, err := ColumnFromColName(t, colName)
			if err != nil {
				return err
			}
			expr, err := c.ExprToString(v.RawExpr)
			if err != nil {
				return err
			}
			col.Attrs.HasExplicitDefault = true
			col.Attrs.ColumnDefault = expr
			return nil
		}
	case pg_query.ConstrType_CONSTR_UNIQUE:
		{
			constrainsCols := make(Columns, 0, 1)
			if colName != "" {
				col, err := ColumnFromColName(t, colName)
				if err != nil {
					return err
				}
				constrainsCols = append(constrainsCols, col)
			} else {
				for _, colRef := range v.Keys {
					colName := StringOrPanic(colRef)
					col, ok := t.Columns.Get(colName)
					if !ok {
						return fmt.Errorf("column %s not found", colName)
					}
					constrainsCols = append(constrainsCols, col)
				}
			}
			name := v.Conname
			if name == "" {
				name = strings.Join([]string{t.Name, constrainsCols.JoinColumnNames("_"), "key"}, "_")
			}
			c.Catalog.PgConstraint.AddConstraint(&Constraint{Table: t,
				Name:       name,
				Type:       ConstraintTypeUnique,
				Constrains: constrainsCols,
			})
			return nil
		}
	case pg_query.ConstrType_CONSTR_FOREIGN:
		{
			var refers []*Column
			schema := v.Pktable.Schemaname
			table := v.Pktable.Relname
			tableObj, err := c.FindTable(schema, table)
			name := v.Conname
			if err != nil {
				return err
			}
			for _, colRef := range v.PkAttrs {
				colName := StringOrPanic(colRef)
				col, err := c.FindColumn(schema, table, colName)
				if err != nil {
					return fmt.Errorf("couldn't find column '%s' in table '%s'", colName, table)
				}
				refers = append(refers, col)
			}
			if len(v.PkAttrs) == 0 {
				// used by eg 'FOREIGN KEY (my_field) REFERENCES schema.table';
				// in this instance the FK refers to the PK of the other table
				cols, err := c.FindPrimaryKeyColumns(schema, table)
				if err != nil {
					return err
				}
				refers = cols
			}
			constrainsCols := make(Columns, 0, len(refers))
			for _, colRef := range v.FkAttrs {
				colName := StringOrPanic(colRef)
				col, ok := t.Columns.Get(colName)
				if !ok {
					return fmt.Errorf("column %s not found", colName)
				}
				constrainsCols = append(constrainsCols, col)
			}
			if len(constrainsCols) == 0 {
				// For syntax like:
				// CREATE TABLE example (user_id INTEGER REFERENCES users(id));
				// the name of the constrained column is not in the node, and is provided
				// by the caller instead
				col, err := ColumnFromColName(t, colName)
				if err != nil {
					return err
				}
				constrainsCols = append(constrainsCols, col)
			}
			if name == "" && len(constrainsCols) > 0 {
				name = strings.Join([]string{t.Name, constrainsCols.JoinColumnNames("_"), "fkey"}, "_")
			}
			c.Catalog.PgConstraint.AddConstraint(&Constraint{
				Table:         t,
				Type:          ConstraintTypeForeignKey,
				DropBehaviour: DropBehaviourRestrict,
				Name:          name,
				RefersTable:   tableObj,
				Refers:        refers,
				Constrains:    constrainsCols,
			})
			return nil
		}
	case pg_query.ConstrType_CONSTR_NULL:
		{
			// Nothing to do? That's the default
			return nil
		}
	case pg_query.ConstrType_CONSTR_CHECK, pg_query.ConstrType_CONSTR_ATTR_DEFERRABLE:
		{
			// Nothing to do for checked constraints yet
			return nil
		}
	case pg_query.ConstrType_CONSTR_IDENTITY:
		{
			constrainsCols := make(Columns, 0, len(v.FkAttrs))
			for _, colRef := range v.FkAttrs {
				colName := StringOrPanic(colRef)
				col, ok := t.Columns.Get(colName)
				if !ok {
					return fmt.Errorf("column %s not found", colName)
				}
				constrainsCols = append(constrainsCols, col)
			}
			if len(constrainsCols) == 0 {
				// For syntax like:
				// CREATE TABLE example (user_id INTEGER REFERENCES users(id));
				// the name of the constrained column is not in the node, and is provided
				// by the caller instead
				col, err := ColumnFromColName(t, colName)
				if err != nil {
					return err
				}
				constrainsCols = append(constrainsCols, col)
			}
			name := strings.Join([]string{t.Name, constrainsCols.JoinColumnNames("_"), "identity"}, "_")
			c.Catalog.PgConstraint.AddConstraint(&Constraint{
				Table: t,
				// TODO: What does postgres call it?
				Name:          name,
				Type:          ConstraintTypeIdentity,
				Constrains:    constrainsCols,
				DropBehaviour: DropBehaviourCascade,
			})
			return nil
		}
	default:
		return fmt.Errorf("not yet able to process constraint type %v", v.Contype)
	}
}

func (c *Compiler) FindTable(schema, table string) (*Table, error) {

	schema = c.SchemaOrSearchPath(schema)
	s, ok := c.Catalog.Schemas.Get(schema)
	if !ok {
		return nil, fmt.Errorf("schema %s not found", schema)
	}
	t, ok := s.Tables.Get(table)
	if !ok {
		return nil, fmt.Errorf("table %s not found", table)
	}
	return t, nil
}

func (c *Compiler) FindColumn(schema, table, name string) (*Column, error) {

	t, err := c.FindTable(schema, table)
	if err != nil {
		return nil, err
	}
	col, ok := t.Columns.Get(name)
	if !ok {
		return nil, fmt.Errorf("column %s not found", name)
	}
	return col, nil
}

func (c *Compiler) FindPrimaryKeyColumns(schema, table string) ([]*Column, error) {
	schema = c.SchemaOrSearchPath(schema)
	s, ok := c.Catalog.Schemas.Get(schema)
	if !ok {
		return nil, fmt.Errorf("schema %s not found", schema)
	}
	t, ok := s.Tables.Get(table)
	if !ok {
		return nil, fmt.Errorf("table %s not found", table)
	}
	ret := make([]*Column, 0, 1)
	for _, col := range t.Columns.List() {
		if col.Attrs.Pkey {
			ret = append(ret, col)
		}
	}
	return ret, nil
}

func (c *Compiler) ExprToString(n *pg_query.Node) (string, error) {

	switch x := n.Node.(type) {
	case *pg_query.Node_SqlvalueFunction:
		{
			// A value function is e.g. CURRENT_TIMESTAMP -
			// looks like a value but behaves like a function
			return strings.TrimPrefix(x.SqlvalueFunction.Op.String(), "SVFOP_"), nil
		}
	case *pg_query.Node_FuncCall:
		{
			// Function invocation e.g. NOW()
			//fmt.Println(StringsOrPanic(x.FuncCall.Funcname))
			// TODO: x.FuncCall.Args...
			return fmt.Sprintf("%s(%s)", strings.Join(StringsOrPanic(x.FuncCall.Funcname), "."), strings.Join(StringsOrPanic(x.FuncCall.Args), ", ")), nil
		}
	case *pg_query.Node_TypeCast:
		{
			typeName := strings.Join(StringsOrPanic(x.TypeCast.TypeName.Names), ".")
			aConst, ok := x.TypeCast.Arg.Node.(*pg_query.Node_AConst)
			if !ok {
				return "", nil
			}
			val, err := c.ConstantAsString(aConst.AConst)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%s::%s", val, typeName), nil
		}
	case *pg_query.Node_AConst:
		{
			return c.ConstantAsString(x.AConst)
		}
	}

	return "", nil
}

func (c *Compiler) ConstantAsString(aConst *pg_query.A_Const) (string, error) {

	if aConst.Isnull {
		return "NULL", nil
	}

	// Constant default value
	switch sv := aConst.Val.(type) {
	case *pg_query.A_Const_Sval:
		{
			// string value
			return fmt.Sprintf("\"%s\"", sv.Sval.Sval), nil
		}
	case *pg_query.A_Const_Boolval:
		{
			// bool val
			return strconv.FormatBool(sv.Boolval.Boolval), nil
		}
	case *pg_query.A_Const_Ival:
		{
			return strconv.FormatInt(int64(sv.Ival.Ival), 10), nil
		}
	case *pg_query.A_Const_Fval:
		{
			return sv.Fval.Fval, nil
		}
	case *pg_query.A_Const_Bsval:
		{

			return sv.Bsval.Bsval, nil
		}
	default:
		{
			return "", fmt.Errorf("unknown how to parse constant %s", aConst)
		}
	}
}

func (c *Compiler) CreateEnum(ces *pg_query.CreateEnumStmt) error {

	schema, name := ObjectNameFromNodeList(ces.TypeName)
	typeName := name
	if schema != "" {
		typeName = schema + "." + typeName
	}
	vals := StringsOrPanic(ces.Vals)
	typ := &PostgresType{
		Name:           typeName,
		Schema:         schema,
		IsSerial:       false,
		NonSerialType:  nil,
		SimpleMatches:  []string{typeName},
		PatternMatches: nil,
		EnumValues:     vals,
	}
	return c.TypeRegistry.RegisterType(typ)
}

func (c *Compiler) SchemaOrSearchPath(schema string) string {
	if schema == "" {
		return c.SearchPath
	}
	return schema
}

func ObjectNameFromList(l *pg_query.List) (schema string, object string) {

	return ObjectNameFromNodeList(l.Items)
}

func ObjectNameFromNodeList(l []*pg_query.Node) (schema string, object string) {

	if len(l) == 1 {
		object = StringOrPanic(l[0])
		return
	}
	if len(l) == 2 {
		schema = StringOrPanic(l[0])
		object = StringOrPanic(l[1])
	}
	return
}

func StringsOrPanic(ns []*pg_query.Node) []string {

	ret := make([]string, 0, len(ns))
	for _, n := range ns {
		ret = append(ret, StringOrPanic(n))
	}
	return ret
}

func StringOrPanic(n *pg_query.Node) string {

	s, ok := n.Node.(*pg_query.Node_String_)
	if !ok {
		panic("unknown how to parse node " + n.String())
	}
	return s.String_.Sval
}

func ColumnFromColName(t *Table, name string) (*Column, error) {
	col, ok := t.Columns.Get(name)
	if !ok {
		return nil, fmt.Errorf("column %s not found", name)
	}
	return col, nil
}

func ColumnsFromColNames(t *Table, names []string) (Columns, error) {
	ret := make(Columns, 0, len(names))
	for _, name := range names {
		col, err := ColumnFromColName(t, name)
		if err != nil {
			return nil, err
		}
		ret = append(ret, col)
	}
	return ret, nil
}
