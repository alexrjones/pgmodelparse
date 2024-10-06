package main

import (
	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func assertTable(t *testing.T, c *Compiler, path string) *Table {
	t.Helper()

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
	t.Helper()

	col, ok := tab.Columns.Get(name)
	require.True(t, ok)
	assert.Equal(t, col.Type, pgtype)
	return col
}

func assertConstraints(t *testing.T, c *Compiler, col *Column, cons ...Constraint) {
	t.Helper()

	actualCons, ok := c.Catalog.Depends.ConstraintsByColumn.Get(col)
	if len(cons) == 0 {
		assert.Len(t, actualCons, 0)
		return
	}
	require.True(t, ok)
	consNames := lo.Map(actualCons, func(item *Constraint, index int) string {
		return item.Name
	})
	expectedConstraints := make(Constraints, 0, len(cons))
	for _, con := range cons {
		var actualConstraint *Constraint
		actualConstraint, ok = c.Catalog.Depends.ConstraintsByName[con.Name]
		require.True(t, ok, "no constraint with name %s found, actual constraints are: %v", con.Name, consNames)
		assert.Equal(t, con, *actualConstraint)
		expectedConstraints = append(expectedConstraints, actualConstraint)
	}
	assert.ElementsMatch(t, expectedConstraints, actualCons)
}

const createUsersTable = `
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
`

func TestCompiler_CreateTable(t *testing.T) {
	parse, err := pg_query.Parse(createUsersTable)
	require.Nil(t, err)
	c := NewCompiler()
	err = c.ParseStatements(parse)
	assert.Nil(t, err)

	table := assertTable(t, c, "users")
	{
		col := assertColumn(t, table, "id", Serial)
		assertConstraints(t, c, col, Constraint{
			Table:      table,
			Name:       "users_pkey",
			Type:       ConstraintTypePrimary,
			Constrains: Columns{col},
		})
	}
	{
		col := assertColumn(t, table, "username", CharacterVarying)
		assertConstraints(t, c, col, Constraint{
			Table:      table,
			Name:       "users_username_notnull",
			Type:       ConstraintTypeNotNull,
			Constrains: Columns{col},
		}, Constraint{
			Table:      table,
			Name:       "users_username_key",
			Type:       ConstraintTypeUnique,
			Constrains: Columns{col},
		})
	}
	{
		col := assertColumn(t, table, "email", CharacterVarying)
		assertConstraints(t, c, col, Constraint{
			Table:      table,
			Name:       "users_email_notnull",
			Type:       ConstraintTypeNotNull,
			Constrains: Columns{col},
		})
	}
	{
		col := assertColumn(t, table, "created_at", Timestamp)
		assertConstraints(t, c, col, Constraint{
			Table:      table,
			Name:       "users_created_at_default",
			Type:       ConstraintTypeDefault,
			Constrains: Columns{col},
		})
	}
}

func joinNewline(s ...string) string {

	return strings.Join(s, "\n")
}

const addColumn = `
ALTER TABLE users ADD COLUMN last_login TIMESTAMP;
`

func TestCompiler_AlterTable(t *testing.T) {
	parse, err := pg_query.Parse(joinNewline(createUsersTable, addColumn))
	require.Nil(t, err)
	c := NewCompiler()
	err = c.ParseStatements(parse)
	assert.Nil(t, err)

	table := assertTable(t, c, "users")
	{
		col := assertColumn(t, table, "last_login", Timestamp)
		assertConstraints(t, c, col)
	}
}

const addForeignKey = `
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
`

func TestCompiler_ForeignKey(t *testing.T) {
	parse, err := pg_query.Parse(joinNewline(createUsersTable, addColumn, addForeignKey))
	require.Nil(t, err)
	c := NewCompiler()
	err = c.ParseStatements(parse)
	assert.Nil(t, err)

	assertTable(t, c, "users")
	table := assertTable(t, c, "orders")
	{
		assertColumn(t, table, "id", Serial)
		// TODO
		//assertConstraints(t, c, col)
	}
}
