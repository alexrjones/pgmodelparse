package main

import (
	"fmt"
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"pgmodelgen/collections"
	"strings"
)

type Compiler struct {
	SearchPath string
	Catalog    *Catalog
}

func NewCompiler() *Compiler {
	c := &Compiler{
		SearchPath: "public",
		Catalog: &Catalog{
			Schemas: collections.NewOrderedMap[string, *Schema](),
			Depends: &Depends{
				ConstraintsByColumn: collections.NewMultimap[*Column, *Constraint](),
				ConstraintsByName:   make(map[string]*Constraint),
			},
		},
	}
	defaultSchema := &Schema{
		Name:   "public",
		Tables: collections.NewOrderedMap[string, *Table](),
	}
	c.Catalog.Schemas.Add(defaultSchema.Name, defaultSchema)
	return c
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
	schemaName := stmt.Relation.Schemaname
	if schemaName == "" {
		schemaName = c.SearchPath
	}
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
					return err
				}
			}
		case *pg_query.Node_Constraint:
			{
				constraint, err := c.ParseConstraint(table, nil, p.Constraint)
				if err != nil {
					return err
				}
				c.Catalog.Depends.AddConstraint(constraint)
			}
		}
	}
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
		case pg_query.AlterTableType_AT_AddConstraint:
			{
				conDef, ok := atc.AlterTableCmd.Def.Node.(*pg_query.Node_Constraint)
				if !ok {
					return fmt.Errorf("expected Constraint but got %T", atc.AlterTableCmd.Def.Node)
				}
				constraint, err := c.ParseConstraint(tab, nil, conDef.Constraint)
				if err != nil {
					return err
				}
				c.Catalog.Depends.AddConstraint(constraint)
			}
		case pg_query.AlterTableType_AT_ColumnDefault:
			{

			}
		case pg_query.AlterTableType_AT_DropConstraint:
			{
			}
		}
	}
	return nil
}

func (c *Compiler) DefineColumn(t *Table, def *pg_query.ColumnDef) error {
	name := def.Colname
	pgType := c.TypeFromNode(def.TypeName)
	err := t.AddColumn(&Column{
		Table: t,
		Name:  name,
		Type:  pgType,
	})
	if err != nil {
		return err
	}
	constraints, err := c.ParseConstraints(t, name, def.Constraints)
	if err != nil {
		return err
	}
	for _, cons := range constraints {
		c.Catalog.Depends.AddConstraint(cons)
	}
	return nil
}

func (c *Compiler) DropColumn(t *Table, colName string, behavior pg_query.DropBehavior) error {

	col, ok := t.Columns.Get(colName)
	if !ok {
		return fmt.Errorf("column %s does not exist", colName)
	}
	depends, _ := c.Catalog.Depends.ConstraintsByColumn.Get(col)
	var funcs []func()
	for _, con := range depends {
		if con.DropBehaviour == DropBehaviourRestrict {
			if behavior != pg_query.DropBehavior_DROP_CASCADE {
				return fmt.Errorf("can't drop %s because %s depends on it", col.Name, con.Name)
			}
			funcs = append(funcs, func() {
				c.Catalog.Depends.RemoveConstraint(con)
			})
		}
	}

	for _, fn := range funcs {
		fn()
	}
	c.Catalog.Depends.ConstraintsByColumn.Remove(col)
	t.Columns.Remove(col.Name)
	return nil
}

// FindTableFromRangeVar looks up an existing table from the provided RangeVar.
func (c *Compiler) FindTableFromRangeVar(r *pg_query.RangeVar) (*Table, error) {

	name := r.Relname
	schemaName := r.Schemaname
	if schemaName == "" {
		schemaName = c.SearchPath
	}
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
	return MatchType(strings.Join(parts, "."))
}

func (c *Compiler) ParseConstraints(t *Table, colName string, constraints []*pg_query.Node) (Constraints, error) {
	ret := make(Constraints, 0, len(constraints))
	for _, n := range constraints {
		v, ok := n.Node.(*pg_query.Node_Constraint)
		if !ok {
			panic("unknown how to parse node " + n.String())
		}
		con, err := c.ParseConstraint(t, []string{colName}, v.Constraint)
		if err != nil {
			return nil, err
		}
		ret = append(ret, con)
	}
	return ret, nil
}

func (c *Compiler) ParseConstraint(t *Table, colNames []string, v *pg_query.Constraint) (*Constraint, error) {

	switch v.Contype {
	case pg_query.ConstrType_CONSTR_PRIMARY:
		{
			name := v.Conname
			if name == "" {
				name = t.Name + "_" + "pkey"
			}
			cols, err := ColumnsFromColNames(t, colNames)
			if err != nil {
				return nil, err
			}
			return &Constraint{Table: t, Name: name, Type: ConstraintTypePrimary, Constrains: cols}, nil
		}
	case pg_query.ConstrType_CONSTR_NOTNULL:
		{
			cols, err := ColumnsFromColNames(t, colNames)
			if err != nil {
				return nil, err
			}
			return &Constraint{Table: t,
				Name: strings.Join([]string{t.Name, cols.JoinColumnNames("_"), "notnull"}, "_"),
				Type: ConstraintTypeNotNull, Constrains: cols}, nil
		}
	case pg_query.ConstrType_CONSTR_DEFAULT:
		{
			cols, err := ColumnsFromColNames(t, colNames)
			if err != nil {
				return nil, err
			}
			// TODO: parse default value supplier expressions
			return &Constraint{Table: t,
				Name: strings.Join([]string{t.Name, cols.JoinColumnNames("_"), "default"}, "_"),
				Type: ConstraintTypeDefault, Constrains: cols}, nil
		}
	case pg_query.ConstrType_CONSTR_UNIQUE:
		{
			constrainsCols := make(Columns, 0, 5)
			if len(colNames) > 0 {
				cols, err := ColumnsFromColNames(t, colNames)
				if err != nil {
					return nil, err
				}
				constrainsCols = cols
			} else {
				for _, colRef := range v.Keys {
					colName := StringOrPanic(colRef)
					col, ok := t.Columns.Get(colName)
					if !ok {
						return nil, fmt.Errorf("column %s not found", colName)
					}
					constrainsCols = append(constrainsCols, col)
				}
			}
			name := v.Conname
			if name == "" {
				name = strings.Join([]string{t.Name, constrainsCols.JoinColumnNames("_"), "key"}, "_")
			}
			return &Constraint{Table: t,
				Name:       name,
				Type:       ConstraintTypeUnique,
				Constrains: constrainsCols,
			}, nil
		}
	case pg_query.ConstrType_CONSTR_FOREIGN:
		{
			var refers []*Column
			schema := v.Pktable.Schemaname
			table := v.Pktable.Relname
			for _, colRef := range v.PkAttrs {
				colName := StringOrPanic(colRef)
				col, err := c.FindColumn(schema, table, colName)
				if err != nil {
					return nil, fmt.Errorf("couldn't find column '%s' in table '%s'", colName, table)
				}
				refers = append(refers, col)
			}
			constrainsCols := make(Columns, 0, len(v.FkAttrs))
			for _, colRef := range v.FkAttrs {
				colName := StringOrPanic(colRef)
				col, ok := t.Columns.Get(colName)
				if !ok {
					return nil, fmt.Errorf("column %s not found", colName)
				}
				constrainsCols = append(constrainsCols, col)
			}
			if len(constrainsCols) == 0 {
				// For syntax like:
				// CREATE TABLE example (user_id INTEGER REFERENCES users(id));
				// the name of the constrained column is not in the node, and is provided
				// by the caller instead
				var err error
				constrainsCols, err = ColumnsFromColNames(t, colNames)
				if err != nil {
					return nil, err
				}
			}
			name := v.Conname
			if name == "" && len(constrainsCols) > 0 {
				name = strings.Join([]string{t.Name, constrainsCols.JoinColumnNames("_"), "fkey"}, "_")
			}
			return &Constraint{Table: t, Type: ConstraintTypeForeignKey, DropBehaviour: DropBehaviourRestrict, Name: name, Refers: refers, Constrains: constrainsCols}, nil
		}
	}
	return nil, fmt.Errorf("not yet able to process constraint type %v", v.Contype)
}

func (c *Compiler) FindColumn(schema, table, name string) (*Column, error) {

	if schema == "" {
		schema = c.SearchPath
	}
	s, ok := c.Catalog.Schemas.Get(schema)
	if !ok {
		return nil, fmt.Errorf("schema %s not found", schema)
	}
	t, ok := s.Tables.Get(table)
	if !ok {
		return nil, fmt.Errorf("table %s not found", table)
	}
	col, ok := t.Columns.Get(name)
	if !ok {
		return nil, fmt.Errorf("column %s not found", name)
	}
	return col, nil
}

func StringOrPanic(n *pg_query.Node) string {

	s, ok := n.Node.(*pg_query.Node_String_)
	if !ok {
		panic("unknown how to parse node " + n.String())
	}
	return s.String_.Sval
}

func ColumnsFromColNames(t *Table, names []string) (Columns, error) {
	ret := make(Columns, 0, len(names))
	for _, name := range names {
		col, ok := t.Columns.Get(name)
		if !ok {
			return nil, fmt.Errorf("column %s not found", name)
		}
		ret = append(ret, col)
	}
	return ret, nil
}
