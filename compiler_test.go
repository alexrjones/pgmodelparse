package main

import (
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func assertTable(t *testing.T, c *Compiler, path string) *Table {

	schemaName := c.SearchPath
	tableName := path
	split := strings.Split(path, ".")
	if len(split) > 1 {
		schemaName = split[0]
		tableName = split[1]
	}
	schema, ok := c.Catalog.Schemas.Get(schemaName)
	require.True(t, ok)
	table, ok := schema.Tables.Get(tableName)
	require.True(t, ok)
	return table
}

func assertColumn(t *testing.T, tab *Table, name string, pgtype *PostgresType) *Column {
	col, ok := tab.Columns.Get(name)
	require.True(t, ok)
	assert.Equal(t, col.Type, pgtype)
	return col
}

func assertConstraints(t *testing.T, c *Compiler, col *Column, cons ...Constraint) {

	actualCons, ok := c.Catalog.Depends.ConstraintsByColumn.Get(col)
	assert.True(t, ok)
	expectedConstraints := make(Constraints, 0, len(cons))
	for _, con := range cons {
		actualConstraint, ok := c.Catalog.Depends.ConstraintsByName[con.Name]
		assert.True(t, ok)
		assert.Equal(t, con, *actualConstraint)
		expectedConstraints = append(expectedConstraints, actualConstraint)
	}
	assert.ElementsMatch(t, expectedConstraints, actualCons)
}

func TestCompiler_CreateTable(t *testing.T) {

	const inputSchema = `
	CREATE SCHEMA test;

	CREATE table test.test (
		id BIGSERIAL PRIMARY KEY,
		name TEXT NOT NULL,
		created_time TIMESTAMPTZ NOT NULL
	);
	`
	parse, err := pg_query.Parse(inputSchema)
	require.Nil(t, err)
	c := NewCompiler()
	err = c.ParseStatements(parse)
	assert.Nil(t, err)

	table := assertTable(t, c, "test.test")
	{
		col := assertColumn(t, table, "id", Bigserial)
		assertConstraints(t, c, col, Constraint{
			Table:         table,
			Name:          "test_pkey",
			Type:          ConstraintTypePrimary,
			Constrains:    Columns{col},
			DropBehaviour: DropBehaviourCascade,
		})
	}
	{
		col := assertColumn(t, table, "name", Text)
		assertConstraints(t, c, col, Constraint{
			Table:         table,
			Name:          "test_name_notnull",
			Type:          ConstraintTypeNotNull,
			Constrains:    Columns{col},
			DropBehaviour: DropBehaviourCascade,
		})
	}
	{
		col := assertColumn(t, table, "created_time", Timestamptz)
		assertConstraints(t, c, col, Constraint{
			Table:         table,
			Name:          "test_created_time_notnull",
			Type:          ConstraintTypeNotNull,
			Constrains:    Columns{col},
			DropBehaviour: DropBehaviourCascade,
		})
	}
}

//func TestCompiler_AlterTable(t *testing.T) {
//
//	const inputSchema = `
//	CREATE SCHEMA test;
//
//	CREATE table test.test (
//		id BIGSERIAL PRIMARY KEY,
//		name TEXT NOT NULL,
//		created_time TIMESTAMPTZ NOT NULL
//	);
//
//	ALTER TABLE test.test DROP COLUMN name;
//	`
//	parse, err := pg_query.Parse(inputSchema)
//	require.Nil(t, err)
//	c := NewCompiler()
//	err = c.ParseStatements(parse)
//	assert.Nil(t, err)
//
//	table := assertTable(t, c, "test.test")
//	nameCol, ok := table.Columns.Get("name")
//	require.True(t, ok)
//	assert.Equal(t, idCol.Type, Bigserial)
//	idConstraints, ok := c.Catalog.Depends.ConstraintsByColumn.Get(idCol)
//	assert.True(t, ok)
//	pKeyConstraint := Constraint{
//		Table:         table,
//		Name:          "test_pkey",
//		Type:          ConstraintTypePrimary,
//		Constrains:    Columns{idCol},
//		DropBehaviour: DropBehaviourCascade,
//	}
//	actualConstraint, ok := c.Catalog.Depends.ConstraintsByName[pKeyConstraint.Name]
//	assert.True(t, ok)
//	assert.Equal(t, pKeyConstraint, *actualConstraint)
//
//	expectedConstraints := Constraints{actualConstraint}
//	assert.ElementsMatch(t, expectedConstraints, idConstraints)
//}
