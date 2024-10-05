package main

import (
	"fmt"
	"pgmodelgen/collections"
	"slices"
)

type Catalog struct {
	Schemas *collections.OrderedMap[string, *Schema]
	Depends *collections.Multimap[*Column, *Constraint]
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
	Constraints Constraints
}

func (c *Column) RemoveConstraint(con *Constraint) {

	for i, v := range c.Constraints {
		if v == con {
			c.Constraints = append(c.Constraints[:i], c.Constraints[min(i+1, len(c.Constraints)):]...)
			return
		}
	}
}

type Constraint struct {
	Table      *Table
	Name       string
	Type       ConstraintType // Primary, FK, etc
	Refers     []*Column
	Constrains []*Column
	// DropBehaviour explains how this constraint should behave
	// when one of its dependencies is dropped.
	DropBehaviour DropBehaviour
}

func (c *Constraint) Depends() []*Column {

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
