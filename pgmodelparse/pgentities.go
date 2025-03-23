package pgmodelparse

import (
	"fmt"
	"slices"
	"strings"

	"github.com/henges/pgmodelparse/collections"
)

type Catalog struct {
	Schemas      *collections.OrderedMap[string, *Schema]
	PgConstraint *PgConstraint
}

type PgConstraint struct {
	// ByColumn holds all constraints that reference a given column
	// either as referee or referent
	ByColumn *collections.Multimap[*Column, *Constraint]
	// Constraints holds all constraints that apply to a column (column is referee)
	Constrains *collections.Multimap[*Column, *Constraint]
	// Refers holds all constraints that refer to a column (column is referent)
	Refers *collections.Multimap[*Column, *Constraint]
	// ByName holds all constraints by their fully-qualified name
	ByName map[string]*Constraint
}

func (d *PgConstraint) AddConstraint(cons *Constraint) {

	for _, col := range cons.Constrains {
		d.ByColumn.Add(col, cons)
		d.Constrains.Add(col, cons)
	}
	for _, col := range cons.Refers {
		d.ByColumn.Add(col, cons)
		d.Refers.Add(col, cons)
	}
	d.ByName[cons.FQName()] = cons
	cons.OnCreate()
}

func (d *PgConstraint) RemoveConstraint(cons *Constraint) {
	for _, col := range cons.Constrains {
		d.ByColumn.RemoveValue(col, cons)
		d.Constrains.RemoveValue(col, cons)
	}
	for _, col := range cons.Refers {
		d.ByColumn.RemoveValue(col, cons)
		d.Refers.RemoveValue(col, cons)
	}
	delete(d.ByName, cons.FQName())
	cons.OnRemove()
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
	Name    string
	Schema  string
	Columns *collections.OrderedMap[string, *Column]
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

func (t *Table) FQName() string {

	if t.Schema == "" {
		return t.Name
	}
	return fmt.Sprintf("%s.%s", t.Schema, t.Name)
}

type Column struct {
	Table *Table
	Name  string
	Type  *PostgresType
	Attrs *ColumnAttributes
}

func (c *Column) FQName() string {

	return c.Table.Schema + "." + c.Table.Name + "." + c.Name
}

type ColumnAttributes struct {
	NotNull            bool
	Pkey               bool
	HasSequence        bool
	HasExplicitDefault bool
	ColumnDefault      string
	//ColumnDefault *pg_query.Node // TODO: parse to native type
	// Other values include: char max length for varchar,
	// decimal and timezone precision, etc...
}

func (ca ColumnAttributes) IsNotNull() bool {

	return ca.NotNull || ca.Pkey
}

func (ca ColumnAttributes) IsRequired() bool {

	return ca.IsNotNull() && !(ca.HasExplicitDefault || ca.HasSequence)
}

type Columns []*Column

func (c Columns) Names() []string {
	names := make([]string, 0, len(c))
	for _, col := range c {
		names = append(names, col.Name)
	}
	return names
}

func (c Columns) FQNames() []string {
	names := make([]string, 0, len(c))
	for _, col := range c {
		names = append(names, col.FQName())
	}
	return names
}

func (c Columns) JoinFQNames(sep string) string {

	return strings.Join(c.FQNames(), sep)
}

func (c Columns) JoinColumnNames(sep string) string {

	return strings.Join(c.Names(), sep)
}

func (c Columns) SingleElementOrPanic() *Column {

	if len(c) != 1 {
		panic(fmt.Errorf("wrong number of columns: expected 1, got %d", len(c)))
	}
	return c[0]
}

type Constraint struct {
	Table       *Table
	Name        string
	Type        ConstraintType // Primary, FK, etc
	RefersTable *Table
	Refers      Columns
	Constrains  Columns
	// DropBehaviour explains how this constraint should behave
	// when one of its dependencies is dropped.
	DropBehaviour DropBehaviour
}

func (c *Constraint) FQName() string {

	return c.Table.Schema + "." + c.Name
}

func (c *Constraint) OnCreate() {

	switch c.Type {
	case ConstraintTypePrimary:
		{
			for _, col := range c.Constrains {
				col.Attrs.Pkey = true
			}
		}
	}
}

func (c *Constraint) OnRemove() {

	switch c.Type {
	case ConstraintTypePrimary:
		{
			for _, col := range c.Constrains {
				col.Attrs.Pkey = false
			}
		}
	}
}

func (c *Constraint) Depends() Columns {

	return slices.Concat(c.Constrains, c.Refers)
}

type Constraints []*Constraint

type DropBehaviour int

const (
	// DropBehaviourCascade causes the object to also be dropped.
	// This is the behaviour for most constraints.
	DropBehaviourCascade DropBehaviour = iota
	// DropBehaviourRestrict prevents dropping the referred object
	// until the referring object is also removed.
	// For example, preventing the foreign key columns of a constraint
	// from being dropped unless the constraint is removed or the
	// CASCADE keyword is used.
	DropBehaviourRestrict
)

type ConstraintType int

const (
	ConstraintTypePrimary ConstraintType = iota
	ConstraintTypeUnique
	ConstraintTypeForeignKey
)

func (c ConstraintType) String() string {

	switch c {
	case ConstraintTypePrimary:
		return "Primary"
	case ConstraintTypeUnique:
		return "Unique"
	case ConstraintTypeForeignKey:
		return "Foreign Key"
	}
	panic(c)

}
