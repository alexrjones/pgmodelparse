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
	idCol, ok := table.Columns.Get("id")
	require.True(t, ok)
	assert.Equal(t, idCol.Type, Bigserial)
	idConstraints, ok := c.Catalog.Depends.Get(idCol)
	assert.True(t, ok)
	expectedConstraints := Constraints{{
		Table:         table,
		Name:          "test_pkey",
		Type:          ConstraintTypePrimary,
		Refers:        nil,
		Constrains:    []*Column{idCol},
		DropBehaviour: DropBehaviourRestrict,
	}}
	assert.ElementsMatch(t, expectedConstraints, idConstraints)
}
