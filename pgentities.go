package main

import (
	"fmt"
	"pgmodelgen/collections"
	"slices"
	"strings"
)

type Catalog struct {
	Schemas *collections.OrderedMap[string, *Schema]
	Depends *Depends
}

type Depends struct {
	ConstraintsByColumn *collections.Multimap[*Column, *Constraint]
	ConstraintsByName   map[string]*Constraint
}

func (d *Depends) AddConstraint(cons *Constraint) {

	for _, col := range cons.Depends() {
		d.ConstraintsByColumn.Add(col, cons)
	}
	d.ConstraintsByName[cons.Name] = cons
}

func (d *Depends) RemoveConstraint(cons *Constraint) {
	for _, col := range cons.Depends() {
		d.ConstraintsByColumn.Remove(col)
	}
	delete(d.ConstraintsByName, cons.Name)
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

type Column struct {
	Table *Table
	Name  string
	Type  *PostgresType
}

type Columns []*Column

func (c Columns) Names() []string {
	names := make([]string, 0, len(c))
	for _, col := range c {
		names = append(names, col.Name)
	}
	return names
}

func (c Columns) JoinColumnNames(sep string) string {

	return strings.Join(c.Names(), sep)
}

type Constraint struct {
	Table      *Table
	Name       string
	Type       ConstraintType // Primary, FK, etc
	Refers     Columns
	Constrains Columns
	// DropBehaviour explains how this constraint should behave
	// when one of its dependencies is dropped.
	DropBehaviour DropBehaviour
}

func (c *Constraint) Depends() Columns {

	return slices.Concat(c.Constrains, c.Refers)
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
	ConstraintTypeNotNull
	ConstraintTypeDefault
)
