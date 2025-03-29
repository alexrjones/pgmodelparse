package pgmodelparse

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertParse(t *testing.T, stmts string) *Compiler {
	parse, err := pg_query.Parse(stmts)
	require.Nil(t, err)
	c := NewCompiler()
	err = c.ParseStatements(parse)
	assert.Nil(t, err)
	return c
}

func assertParseError(t *testing.T, stmts string, messageContains ...string) {
	parse, err := pg_query.Parse(stmts)
	require.Nil(t, err)
	c := NewCompiler()
	err = c.ParseStatements(parse)
	assert.NotNil(t, err)
	msg := err.Error()
	for _, m := range messageContains {
		assert.Contains(t, msg, m)
	}
}

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

func assertColumn(t *testing.T, tab *Table, name string, pgtype *PostgresType, attrs ColumnAttributes) *Column {
	t.Helper()

	col, ok := tab.Columns.Get(name)
	require.True(t, ok)
	assert.Equal(t, col.Type, pgtype)
	assert.Equal(t, attrs, *col.Attrs)
	return col
}

func assertConstraints(t *testing.T, c *Compiler, col *Column, cons ...Constraint) {
	t.Helper()

	actualCons, ok := c.Catalog.PgConstraint.ByColumn.Get(col)
	if len(cons) == 0 {
		assert.Len(t, actualCons, 0, "expected 0 constraints, but got %d", len(actualCons))
		return
	}
	require.True(t, ok)
	consNames := lo.Map(actualCons, func(item *Constraint, index int) string {
		return item.Name
	})
	expectedConstraints := make(Constraints, 0, len(cons))
	for _, con := range cons {
		var actualConstraint *Constraint
		actualConstraint, ok = c.Catalog.PgConstraint.ByName[con.FQName()]
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
	c := assertParse(t, createUsersTable)

	table := assertTable(t, c, "users")
	{
		col := assertColumn(t, table, "id", Serial, ColumnAttributes{Pkey: true})
		assertConstraints(t, c, col, Constraint{
			Table:      table,
			Name:       "users_pkey",
			Type:       ConstraintTypePrimary,
			Constrains: Columns{col},
		})
	}
	{
		col := assertColumn(t, table, "username", CharacterVarying, ColumnAttributes{NotNull: true})
		assertConstraints(t, c, col, Constraint{
			Table:      table,
			Name:       "users_username_key",
			Type:       ConstraintTypeUnique,
			Constrains: Columns{col},
		})
	}
	{
		col := assertColumn(t, table, "email", CharacterVarying, ColumnAttributes{NotNull: true})
		assertConstraints(t, c, col)
	}
	{
		col := assertColumn(t, table, "created_at", Timestamp, ColumnAttributes{})
		assertConstraints(t, c, col)
	}
}

func joinNewline(s ...string) string {

	return strings.Join(s, "\n")
}

const addColumn = `
ALTER TABLE users ADD COLUMN last_login TIMESTAMP;
`

func TestCompiler_AlterTable(t *testing.T) {
	c := assertParse(t, joinNewline(createUsersTable, addColumn))
	table := assertTable(t, c, "users")
	{
		col := assertColumn(t, table, "last_login", Timestamp, ColumnAttributes{})
		assertConstraints(t, c, col)
	}
}

func TestCompiler_CreateTable_ForeignKey(t *testing.T) {
	const sql = `
	CREATE TABLE base (
		id bigserial primary key
	);
	
	CREATE TABLE referrer (
		id bigint references base(id)
	);
	`

	c := assertParse(t, sql)
	base := assertTable(t, c, "base")
	baseId := assertColumn(t, base, "id", Bigserial, ColumnAttributes{Pkey: true})

	tab := assertTable(t, c, "referrer")
	refersId := assertColumn(t, tab, "id", Bigint, ColumnAttributes{})
	assertConstraints(t, c, refersId, Constraint{
		Table:         tab,
		Name:          "referrer_id_fkey",
		Type:          ConstraintTypeForeignKey,
		RefersTable:   base,
		Refers:        Columns{baseId},
		Constrains:    Columns{refersId},
		DropBehaviour: DropBehaviourRestrict,
	})
}

func TestCompiler_CreateTable_MultiColumnForeignKey(t *testing.T) {
	const sql = `
	CREATE TABLE base (
		id bigserial primary key,
		val text not null
	);
	
	CREATE TABLE referrer (
		id bigint primary key,
		val text not null,
		CONSTRAINT fk_base_id_val FOREIGN KEY (id, val) REFERENCES base (id, val)
	);
	`

	c := assertParse(t, sql)
	base := assertTable(t, c, "base")
	baseId := assertColumn(t, base, "id", Bigserial, ColumnAttributes{Pkey: true})

	tab := assertTable(t, c, "referrer")
	refersId := assertColumn(t, tab, "id", Bigint, ColumnAttributes{})
	assertConstraints(t, c, refersId, Constraint{
		Table:         tab,
		Name:          "referrer_id_fkey",
		Type:          ConstraintTypeForeignKey,
		RefersTable:   base,
		Refers:        Columns{baseId},
		Constrains:    Columns{refersId},
		DropBehaviour: DropBehaviourRestrict,
	})
}

func TestCompiler_MultiColumnUniqueConstraint(t *testing.T) {
	const multiColumnUniqueConstraint = `
	CREATE TABLE unique_constrained (
		uk1 INT NOT NULL,
		uk2 INT NOT NULL,
		UNIQUE (uk1, uk2)
	);
	`
	c := assertParse(t, multiColumnUniqueConstraint)
	tab := assertTable(t, c, "unique_constrained")
	uk1 := assertColumn(t, tab, "uk1", Integer, ColumnAttributes{NotNull: true})
	uk2 := assertColumn(t, tab, "uk2", Integer, ColumnAttributes{NotNull: true})
	expectedCon := Constraint{
		Table:      tab,
		Name:       "unique_constrained_uk1_uk2_key",
		Type:       ConstraintTypeUnique,
		Constrains: Columns{uk1, uk2},
	}
	assertConstraints(t, c, uk1, expectedCon)
	assertConstraints(t, c, uk2, expectedCon)
}

func TestCompiler_AlterTable_DropConstraintNotNull(t *testing.T) {
	const dropConstraintNotNull = `
	CREATE TABLE test (
	  test int not null
	);
	
	ALTER TABLE test ALTER COLUMN test DROP NOT NULL;
	`
	c := assertParse(t, dropConstraintNotNull)
	tab := assertTable(t, c, "test")
	assertColumn(t, tab, "test", Integer, ColumnAttributes{})
}

func TestCompiler_AlterTable_DropConstraintNotNull_PrimaryKey_Fails(t *testing.T) {
	const sql = `
	CREATE TABLE test (
	  test int primary key
	);
	
	ALTER TABLE test ALTER COLUMN test DROP NOT NULL;
	`
	assertParseError(t, sql, "can't drop not null constraint from primary key column")
}

func TestCompiler_AlterTable_AddConstraint_ForeignKey(t *testing.T) {
	const sql = `
	CREATE TABLE base (
		id bigserial primary key
	);
	
	CREATE TABLE referrer (
		id bigint
	);

	ALTER TABLE referrer ADD FOREIGN KEY (id) REFERENCES base (id);
	`

	c := assertParse(t, sql)
	base := assertTable(t, c, "base")
	baseId := assertColumn(t, base, "id", Bigserial, ColumnAttributes{Pkey: true})

	tab := assertTable(t, c, "referrer")
	refersId := assertColumn(t, tab, "id", Bigint, ColumnAttributes{})
	assertConstraints(t, c, refersId, Constraint{
		Table:         tab,
		Name:          "referrer_id_fkey",
		Type:          ConstraintTypeForeignKey,
		RefersTable:   base,
		Refers:        Columns{baseId},
		Constrains:    Columns{refersId},
		DropBehaviour: DropBehaviourRestrict,
	})
}

func TestCompiler_AlterTable_DropConstraint_ForeignKey(t *testing.T) {
	const sql = `
	CREATE TABLE base (
		id bigserial primary key
	);
	
	CREATE TABLE referrer (
		id bigint REFERENCES base (id)
	);

	ALTER TABLE referrer DROP CONSTRAINT referrer_id_fkey;
	`

	c := assertParse(t, sql)
	assertTable(t, c, "base")
	tab := assertTable(t, c, "referrer")
	refersId := assertColumn(t, tab, "id", Bigint, ColumnAttributes{})
	assertConstraints(t, c, refersId)
}

func TestCompiler_Drop_Table(t *testing.T) {
	const sql = `
	CREATE SCHEMA base;

	CREATE TABLE base.base (
		id bigserial primary key
	);
	
	CREATE TABLE second ();
	
	DROP TABLE base.base, second;
	`

	c := assertParse(t, sql)
	sch1, ok := c.Catalog.Schemas.Get("public")
	require.True(t, ok)
	assert.Len(t, sch1.Tables.List(), 0)
	sch2, ok := c.Catalog.Schemas.Get("base")
	require.True(t, ok)
	assert.Len(t, sch2.Tables.List(), 0)
}

const defaultVariants = `
CREATE TABLE defaulters (
    time1 timestamptz default now(),
    time2 timestamptz default current_timestamp,
    constant text default 'abcd',
    expression int default 10+1,
    nully text default null
)
`

func TestCompiler_DefaultVariants(t *testing.T) {
	assertParse(t, defaultVariants)
}

func TestCompiler_AlterTable_DropDefaultSequence(t *testing.T) {
	const test = `CREATE TABLE seqtest (
    	id BIGSERIAL PRIMARY KEY 
    );

	ALTER TABLE seqtest ALTER COLUMN id DROP DEFAULT;
`

	c := assertParse(t, test)
	tab := assertTable(t, c, "seqtest")
	idCol, ok := tab.Columns.Get("id")
	assert.True(t, ok)
	assert.Equal(t, idCol.Type, Bigint)
	assert.False(t, idCol.Attrs.HasSequence)
	assert.Equal(t, idCol.Attrs.SequenceName, "")
}

func TestCompiler_AlterTable_AlterColumn_ChangeType(t *testing.T) {
	const test = `CREATE TABLE seqtest (
    	id BIGINT PRIMARY KEY 
    );

	ALTER TABLE seqtest ALTER COLUMN id TYPE INT;
`

	c := assertParse(t, test)
	tab := assertTable(t, c, "seqtest")
	idCol, ok := tab.Columns.Get("id")
	assert.True(t, ok)
	assert.Equal(t, idCol.Type, Integer)
}

func TestCompiler_AlterTable_AddDefault(t *testing.T) {
	const test = `CREATE TABLE seqtest (
    	id BIGSERIAL PRIMARY KEY,
    	txt TEXT NOT NULL
    );

	ALTER TABLE seqtest ALTER COLUMN txt SET DEFAULT '';
`

	c := assertParse(t, test)
	_ = c
	// TODO
}

// TODO:
// CREATE TABLE (... PRIMARY KEY(col1, col2) ...)
// CREATE TABLE (... col int null ...)

//func TestCompiler_
