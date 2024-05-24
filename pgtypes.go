package main

type PostgresType struct {
	Name        string
	Aliases     string
	Description string
}

var (
	Bigint           = PostgresType{Name: "bigint", Aliases: "int8", Description: "signed eight-byte integer"}
	Bigserial        = PostgresType{Name: "bigserial", Aliases: "serial8", Description: "autoincrementing eight-byte integer"}
	Bit              = PostgresType{Name: "bit [ (n) ]", Description: "fixed-length bit string"}
	BitVarying       = PostgresType{Name: "bit varying [ (n) ]", Aliases: "varbit [ (n) ]", Description: "variable-length bit string"}
	Boolean          = PostgresType{Name: "boolean", Aliases: "bool", Description: "logical Boolean (true/false)"}
	Box              = PostgresType{Name: "box", Description: "rectangular box on a plane"}
	Bytea            = PostgresType{Name: "bytea", Description: "binary data (“byte array”)"}
	Character        = PostgresType{Name: "character [ (n) ]", Aliases: "char [ (n) ]", Description: "fixed-length character string"}
	CharacterVarying = PostgresType{Name: "character varying [ (n) ]", Aliases: "varchar [ (n) ]", Description: "variable-length character string"}
	CIDR             = PostgresType{Name: "cidr", Description: "IPv4 or IPv6 network address"}
	Circle           = PostgresType{Name: "circle", Description: "circle on a plane"}
	Date             = PostgresType{Name: "date", Description: "calendar date (year, month, day)"}
	Double           = PostgresType{Name: "double precision", Aliases: "float8", Description: "double precision floating-point number (8 bytes)"}
	Inet             = PostgresType{Name: "inet", Description: "IPv4 or IPv6 host address"}
	Integer          = PostgresType{Name: "integer", Aliases: "int, int4", Description: "signed four-byte integer"}
	Interval         = PostgresType{Name: "interval [ fields ] [ (p) ]", Description: "time span"}
	JSON             = PostgresType{Name: "json", Description: "textual JSON data"}
	JSONB            = PostgresType{Name: "jsonb", Description: "binary JSON data, decomposed"}
	Line             = PostgresType{Name: "line", Description: "infinite line on a plane"}
	Lseg             = PostgresType{Name: "lseg", Description: "line segment on a plane"}
	Macaddr          = PostgresType{Name: "macaddr", Description: "MAC (Media Access Control) address"}
	Macaddr8         = PostgresType{Name: "macaddr8", Description: "MAC (Media Access Control) address (EUI-64 format)"}
	Money            = PostgresType{Name: "money", Description: "currency amount"}
	Numeric          = PostgresType{Name: "numeric [ (p, s) ]", Aliases: "decimal [ (p, s) ]", Description: "exact numeric of selectable precision"}
	Path             = PostgresType{Name: "path", Description: "geometric path on a plane"}
	PGLsn            = PostgresType{Name: "pg_lsn", Description: "PostgreSQL Log Sequence Number"}
	PGSnapshot       = PostgresType{Name: "pg_snapshot", Description: "user-level transaction ID snapshot"}
	Point            = PostgresType{Name: "point", Description: "geometric point on a plane"}
	Polygon          = PostgresType{Name: "polygon", Description: "closed geometric path on a plane"}
	Real             = PostgresType{Name: "real", Aliases: "float4", Description: "single precision floating-point number (4 bytes)"}
	Smallint         = PostgresType{Name: "smallint", Aliases: "int2", Description: "signed two-byte integer"}
	Smallserial      = PostgresType{Name: "smallserial", Aliases: "serial2", Description: "autoincrementing two-byte integer"}
	Serial           = PostgresType{Name: "serial", Aliases: "serial4", Description: "autoincrementing four-byte integer"}
	Text             = PostgresType{Name: "text", Description: "variable-length character string"}
	Time             = PostgresType{Name: "time [ (p) ] [ without time zone ]", Description: "time of day (no time zone)"}
	Timetz           = PostgresType{Name: "time [ (p) ] with time zone", Aliases: "timetz", Description: "time of day, including time zone"}
	Timestamp        = PostgresType{Name: "timestamp [ (p) ] [ without time zone ]", Description: "date and time (no time zone)"}
	Timestamptz      = PostgresType{Name: "timestamp [ (p) ] with time zone", Aliases: "timestamptz", Description: "date and time, including time zone"}
	TSQuery          = PostgresType{Name: "tsquery", Description: "text search query"}
	TSVector         = PostgresType{Name: "tsvector", Description: "text search document"}
	TXIDSnapshot     = PostgresType{Name: "txid_snapshot", Description: "user-level transaction ID snapshot (deprecated, see pg_snapshot)"}
	UUID             = PostgresType{Name: "uuid", Description: "universally unique identifier"}
	XML              = PostgresType{Name: "xml", Description: "XML data"}
)
