package main

import (
	_ "embed"
	"fmt"
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"pgmodelgen/collections"
	"strings"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Println("Usage: pgmodelgen <file>")
		os.Exit(1)
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	compiler := NewCompiler()

	parse, err := pg_query.Parse(string(b))
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	for _, stmt := range parse.Stmts {
		switch p := stmt.Stmt.Node.(type) {
		case *pg_query.Node_CreateSchemaStmt:
			{
				err = compiler.CreateSchema(p.CreateSchemaStmt)
			}
		case *pg_query.Node_CreateStmt:
			{
				err = compiler.CreateTable(p.CreateStmt)
				if err != nil {
					log.Fatal().Err(err).Msg("Error creating table")
				}
			}
		}
	}
	t := MatchType("bigserial")
	fmt.Println(t)
	fmt.Println(parse)
}

type Compiler struct {
	SearchPath string
	Catalog    *Catalog
}

func NewCompiler() *Compiler {
	c := &Compiler{
		SearchPath: "public",
		Catalog: &Catalog{
			Schemas: collections.NewOrderedMap[string, *Schema](),
		},
	}
	defaultSchema := &Schema{
		Name:   "public",
		Tables: collections.NewOrderedMap[string, *Table](),
	}
	c.Catalog.Schemas.Add(defaultSchema.Name, defaultSchema)
	return c
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
				constraint, err := c.ParseConstraint(table, p)
				if err != nil {
					return err
				}
				table.Constraints = append(table.Constraints, constraint)
			}
		}
	}
	return nil
}

func (c *Compiler) DefineColumn(t *Table, def *pg_query.ColumnDef) error {
	name := def.Colname
	pgType := c.TypeFromNode(def.TypeName)
	constraints, err := c.ParseConstraints(t, def.Constraints)
	if err != nil {
		return err
	}
	return t.AddColumn(&Column{
		Table:       t,
		Name:        name,
		Type:        pgType,
		Nullable:    constraints.Nullable(),
		Constraints: constraints,
	})
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

func (c *Compiler) ParseConstraints(t *Table, constraints []*pg_query.Node) (Constraints, error) {
	ret := make(Constraints, 0, len(constraints))
	for _, n := range constraints {
		v, ok := n.Node.(*pg_query.Node_Constraint)
		if !ok {
			panic("unknown how to parse node " + n.String())
		}
		con, err := c.ParseConstraint(t, v)
		if err != nil {
			return nil, err
		}
		ret = append(ret, con)
	}
	return ret, nil
}

func (c *Compiler) ParseConstraint(t *Table, v *pg_query.Node_Constraint) (*Constraint, error) {

	switch v.Constraint.Contype {
	case pg_query.ConstrType_CONSTR_PRIMARY:
		{
			return &Constraint{Type: ConstraintTypePrimary}, nil
		}
	case pg_query.ConstrType_CONSTR_NOTNULL:
		{
			return &Constraint{Type: ConstraintTypeNotNull}, nil
		}
	case pg_query.ConstrType_CONSTR_DEFAULT:
		{
			return &Constraint{Type: ConstraintTypeDefault}, nil
		}
	case pg_query.ConstrType_CONSTR_FOREIGN:
		{
			var refers []*Column
			schema := v.Constraint.Pktable.Schemaname
			table := v.Constraint.Pktable.Relname
			for _, colRef := range v.Constraint.PkAttrs {
				colName := StringOrPanic(colRef)
				col, err := c.FindColumn(schema, table, colName)
				if err != nil {
					return nil, fmt.Errorf("couldn't find column '%s' in table '%s'", colName, table)
				}
				refers = append(refers, col)
			}
			var constrainsCols []*Column
			for _, colRef := range v.Constraint.FkAttrs {
				colName := StringOrPanic(colRef)
				col, ok := t.Columns.Get(colName)
				if !ok {
					return nil, fmt.Errorf("column %s not found", colName)
				}
				constrainsCols = append(constrainsCols, col)
			}
			return &Constraint{Type: ConstraintTypeForeignKey, Refers: refers, Constrains: constrainsCols}, nil
		}
	}
	return nil, fmt.Errorf("not yet able to process constraint type %v", v.Constraint.Contype)
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

type Catalog struct {
	Schemas *collections.OrderedMap[string, *Schema]
}

func (c *Catalog) AddTable(t *Table) error {

	schema, ok := c.Schemas.Get(t.Schema)
	if !ok {
		return fmt.Errorf("no such schema: %s", t.Schema)
	}
	return schema.AddTable(t)
}

type Schema struct {
	Name   string
	Tables *collections.OrderedMap[string, *Table]
}

func (s *Schema) AddTable(t *Table) error {
	_, ok := s.Tables.Get(t.Name)
	if ok {
		return fmt.Errorf("table already exists: %s", t.Name)
	}
	s.Tables.Add(t.Name, t)
	return nil
}

type Table struct {
	Name        string
	Schema      string
	Columns     *collections.OrderedMap[string, *Column]
	Constraints Constraints
}

func NewTable(name, schema string) *Table {
	return &Table{
		Name:    name,
		Schema:  schema,
		Columns: collections.NewOrderedMap[string, *Column](),
	}
}

func (t *Table) AddColumn(c *Column) error {
	_, ok := t.Columns.Get(c.Name)
	if ok {
		return fmt.Errorf("column already exists: %s", c.Name)
	}
	t.Columns.Add(c.Name, c)
	return nil
}

type Column struct {
	Table       *Table
	Name        string
	Type        *PostgresType
	Nullable    bool
	Constraints Constraints
}

type Constraints []*Constraint

func (cs Constraints) Nullable() bool {
	for _, c := range cs {
		if c.Type == ConstraintTypeNotNull || c.Type == ConstraintTypePrimary {
			return false
		}
	}
	return true
}

type Constraint struct {
	Type       ConstraintType // Primary, FK, etc
	Refers     []*Column
	Constrains []*Column
}

type ConstraintType int

const (
	ConstraintTypePrimary ConstraintType = iota
	ConstraintTypeUnique
	ConstraintTypeForeignKey
	ConstraintTypeNotNull
	ConstraintTypeDefault
)

func StringOrPanic(n *pg_query.Node) string {

	s, ok := n.Node.(*pg_query.Node_String_)
	if !ok {
		panic("unknown how to parse node " + n.String())
	}
	return s.String_.Sval
}
