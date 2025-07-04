package pgmodelparse

import (
	"regexp"
	"strings"

	"github.com/samber/lo"
)

type PostgresType struct {
	Name           string
	Aliases        string
	Description    string
	Schema         string
	IsSerial       bool
	NonSerialType  *PostgresType
	EnumValues     []string
	SimpleMatches  []string
	PatternMatches []*regexp.Regexp
}

var validCasts = map[*PostgresType][]*PostgresType{
	Bigint:      {Bigint, Bigserial, Integer, Serial, Smallint, Smallserial},
	Bigserial:   {Bigint, Bigserial, Integer, Serial, Smallint, Smallserial},
	Integer:     {Bigint, Bigserial, Integer, Serial, Smallint, Smallserial},
	Serial:      {Bigint, Bigserial, Integer, Serial, Smallint, Smallserial},
	Smallint:    {Bigint, Bigserial, Integer, Serial, Smallint, Smallserial},
	Smallserial: {Bigint, Bigserial, Integer, Serial, Smallint, Smallserial},
	Text:        {Bytea},
	Bytea:       {Text},
}

func CanCast(from, to *PostgresType) bool {
	casts, ok := validCasts[from]
	if !ok {
		return false
	}
	for _, cast := range casts {
		if cast == to {
			return true
		}
	}
	return false
}

func optionally(re string) string {
	return "(" + re + ")?"
}

// numInBrackets is a regex portion to flexibly match a numeral in brackets
// " (  1024 )" and "(1024)" both match
const numInBrackets = "\\s*\\(\\s*\\d+\\s*\\)"
const twoNumsInBrackets = "\\s*\\(\\s*\\d+\\s*,\\s*\\d+\\s*\\)"
const withoutTimeZone = "\\s*without\\s*time\\s*zone\\s*"
const withTimeZone = "\\s*with\\s*time\\s*zone\\s*"

var interval = "\\s*\\s*(?:" + intervalsRe + ")\\s*"

var (
	Bigint       = &PostgresType{Name: "bigint", Aliases: "int8", SimpleMatches: []string{"bigint", "int8"}, Description: "signed eight-byte integer"}
	Bigserial    = &PostgresType{Name: "bigserial", Aliases: "serial8", SimpleMatches: []string{"bigserial", "serial8"}, Description: "autoincrementing eight-byte integer", IsSerial: true, NonSerialType: Bigint}
	Boolean      = &PostgresType{Name: "boolean", Aliases: "bool", SimpleMatches: []string{"boolean", "bool"}, Description: "logical Boolean (true/false)"}
	Box          = &PostgresType{Name: "box", SimpleMatches: []string{"box"}, Description: "rectangular box on a plane"}
	Bytea        = &PostgresType{Name: "bytea", SimpleMatches: []string{"bytea"}, Description: "binary data (“byte array”)"}
	CIDR         = &PostgresType{Name: "cidr", SimpleMatches: []string{"cidr"}, Description: "IPv4 or IPv6 network address"}
	Circle       = &PostgresType{Name: "circle", SimpleMatches: []string{"circle"}, Description: "circle on a plane"}
	Date         = &PostgresType{Name: "date", SimpleMatches: []string{"date"}, Description: "calendar date (year, month, day)"}
	Double       = &PostgresType{Name: "double precision", Aliases: "float8", SimpleMatches: []string{"double precision", "float8"}, Description: "double precision floating-point number (8 bytes)"}
	Inet         = &PostgresType{Name: "inet", SimpleMatches: []string{"inet"}, Description: "IPv4 or IPv6 host address"}
	Integer      = &PostgresType{Name: "integer", Aliases: "int, int4", SimpleMatches: []string{"integer", "int", "int4"}, Description: "signed four-byte integer"}
	JSON         = &PostgresType{Name: "json", SimpleMatches: []string{"json"}, Description: "textual JSON data"}
	JSONB        = &PostgresType{Name: "jsonb", SimpleMatches: []string{"jsonb"}, Description: "binary JSON data, decomposed"}
	Line         = &PostgresType{Name: "line", SimpleMatches: []string{"line"}, Description: "infinite line on a plane"}
	Lseg         = &PostgresType{Name: "lseg", SimpleMatches: []string{"lseg"}, Description: "line segment on a plane"}
	Macaddr      = &PostgresType{Name: "macaddr", SimpleMatches: []string{"macaddr"}, Description: "MAC (Media Access Control) address"}
	Macaddr8     = &PostgresType{Name: "macaddr8", SimpleMatches: []string{"macaddr8"}, Description: "MAC (Media Access Control) address (EUI-64 format)"}
	Money        = &PostgresType{Name: "money", SimpleMatches: []string{"money"}, Description: "currency amount"}
	Path         = &PostgresType{Name: "path", SimpleMatches: []string{"path"}, Description: "geometric path on a plane"}
	PGLsn        = &PostgresType{Name: "pg_lsn", SimpleMatches: []string{"pg_lsn"}, Description: "PostgreSQL Log Sequence Number"}
	PGSnapshot   = &PostgresType{Name: "pg_snapshot", SimpleMatches: []string{"pg_snapshot"}, Description: "user-level transaction ID snapshot"}
	Point        = &PostgresType{Name: "point", SimpleMatches: []string{"point"}, Description: "geometric point on a plane"}
	Polygon      = &PostgresType{Name: "polygon", SimpleMatches: []string{"polygon"}, Description: "closed geometric path on a plane"}
	Real         = &PostgresType{Name: "real", Aliases: "float4", SimpleMatches: []string{"real", "float4"}, Description: "single precision floating-point number (4 bytes)"}
	Smallint     = &PostgresType{Name: "smallint", Aliases: "int2", SimpleMatches: []string{"smallint", "int2"}, Description: "signed two-byte integer"}
	Smallserial  = &PostgresType{Name: "smallserial", Aliases: "serial2", SimpleMatches: []string{"smallserial", "serial2"}, Description: "autoincrementing two-byte integer", IsSerial: true, NonSerialType: Smallint}
	Serial       = &PostgresType{Name: "serial", Aliases: "serial4", SimpleMatches: []string{"serial", "serial4"}, Description: "autoincrementing four-byte integer", IsSerial: true, NonSerialType: Integer}
	Text         = &PostgresType{Name: "text", SimpleMatches: []string{"text"}, Description: "variable-length character string"}
	TSQuery      = &PostgresType{Name: "tsquery", SimpleMatches: []string{"tsquery"}, Description: "text search query"}
	TSVector     = &PostgresType{Name: "tsvector", SimpleMatches: []string{"tsvector"}, Description: "text search document"}
	TXIDSnapshot = &PostgresType{Name: "txid_snapshot", SimpleMatches: []string{"txid_snapshot"}, Description: "user-level transaction ID snapshot (deprecated, see pg_snapshot)"}
	UUID         = &PostgresType{Name: "uuid", SimpleMatches: []string{"uuid"}, Description: "universally unique identifier"}
	XML          = &PostgresType{Name: "xml", SimpleMatches: []string{"xml"}, Description: "XML data"}

	Bit        = &PostgresType{Name: "bit", PatternMatches: []*regexp.Regexp{regexp.MustCompile("^bit" + optionally(numInBrackets) + "$")}, Description: "fixed-length bit string"}
	BitVarying = &PostgresType{Name: "bit varying", Aliases: "varbit", PatternMatches: []*regexp.Regexp{
		regexp.MustCompile("^bit varying" + optionally(numInBrackets) + "$"),
		regexp.MustCompile("^varbit" + optionally(numInBrackets) + "$"),
	}, Description: "variable-length bit string"}
	Character = &PostgresType{Name: "character", Aliases: "char", PatternMatches: []*regexp.Regexp{
		regexp.MustCompile("^character" + optionally(numInBrackets) + "$"),
		regexp.MustCompile("^char" + optionally(numInBrackets) + "$"),
	}, Description: "fixed-length character string"}
	CharacterVarying = &PostgresType{Name: "character varying", Aliases: "varchar", PatternMatches: []*regexp.Regexp{
		regexp.MustCompile("^character varying" + optionally(numInBrackets) + "$"),
		regexp.MustCompile("^varchar" + optionally(numInBrackets) + "$"),
	}, Description: "variable-length character string"}
	Interval = &PostgresType{Name: "interval", PatternMatches: []*regexp.Regexp{
		regexp.MustCompile("^interval" + interval + optionally(numInBrackets) + "$"),
	}, Description: "time span"}
	Numeric = &PostgresType{Name: "numeric", Aliases: "decimal", PatternMatches: []*regexp.Regexp{
		regexp.MustCompile("^numeric" + optionally(twoNumsInBrackets) + "$"),
		regexp.MustCompile("^decimal" + optionally(numInBrackets) + "$"),
	}, Description: "exact numeric of selectable precision"}
	Time = &PostgresType{Name: "time", PatternMatches: []*regexp.Regexp{
		regexp.MustCompile("^time" + optionally(numInBrackets) + optionally(withoutTimeZone) + "$"),
	}, Description: "time of day (no time zone)"}
	Timetz = &PostgresType{Name: "timetz", Aliases: "timetz", PatternMatches: []*regexp.Regexp{
		regexp.MustCompile("^time" + optionally(numInBrackets) + withTimeZone + "$"),
		regexp.MustCompile("^timetz" + optionally(numInBrackets) + "$"),
	}, Description: "time of day, including time zone"}
	Timestamp = &PostgresType{Name: "timestamp", PatternMatches: []*regexp.Regexp{
		regexp.MustCompile("^timestamp" + optionally(numInBrackets) + optionally(withoutTimeZone) + "$"),
	},
		SimpleMatches: []string{"timestamp"},
		Description:   "date and time (no time zone)"}
	Timestamptz = &PostgresType{Name: "timestamptz", Aliases: "timestamptz", PatternMatches: []*regexp.Regexp{
		regexp.MustCompile("^timestamp" + optionally(numInBrackets) + withTimeZone + "$"),
		regexp.MustCompile("^timestamptz" + optionally(numInBrackets) + "$"),
	},
		SimpleMatches: []string{"timestamptz"},
		Description:   "date and time, including time zone"}
)

var defaultPGTypes = []*PostgresType{
	Bigint,
	Bigserial,
	Boolean,
	Box,
	Bytea,
	CIDR,
	Circle,
	Date,
	Double,
	Inet,
	Integer,
	JSON,
	JSONB,
	Line,
	Lseg,
	Macaddr,
	Macaddr8,
	Money,
	Path,
	PGLsn,
	PGSnapshot,
	Point,
	Polygon,
	Real,
	Smallint,
	Smallserial,
	Serial,
	Text,
	TSQuery,
	TSVector,
	TXIDSnapshot,
	UUID,
	XML,
	Bit,
	BitVarying,
	Character,
	CharacterVarying,
	Interval,
	Numeric,
	Time,
	Timetz,
	Timestamp,
	Timestamptz,
}

type PostgresInterval string

const (
	PostgresIntervalYear           PostgresInterval = "YEAR"
	PostgresIntervalMonth          PostgresInterval = "MONTH"
	PostgresIntervalDay            PostgresInterval = "DAY"
	PostgresIntervalHour           PostgresInterval = "HOUR"
	PostgresIntervalMinute         PostgresInterval = "MINUTE"
	PostgresIntervalSecond         PostgresInterval = "SECOND"
	PostgresIntervalYearToMonth    PostgresInterval = "YEAR TO MONTH"
	PostgresIntervalDayToHour      PostgresInterval = "DAY TO HOUR"
	PostgresIntervalDayToMinute    PostgresInterval = "DAY TO MINUTE"
	PostgresIntervalDayToSecond    PostgresInterval = "DAY TO SECOND"
	PostgresIntervalHourToMinute   PostgresInterval = "HOUR TO MINUTE"
	PostgresIntervalHourToSecond   PostgresInterval = "HOUR TO SECOND"
	PostgresIntervalMinuteToSecond PostgresInterval = "MINUTE TO SECOND"
)

var intervals = map[PostgresInterval]struct{}{
	PostgresIntervalYear:           {},
	PostgresIntervalMonth:          {},
	PostgresIntervalDay:            {},
	PostgresIntervalHour:           {},
	PostgresIntervalMinute:         {},
	PostgresIntervalSecond:         {},
	PostgresIntervalYearToMonth:    {},
	PostgresIntervalDayToHour:      {},
	PostgresIntervalDayToMinute:    {},
	PostgresIntervalDayToSecond:    {},
	PostgresIntervalHourToMinute:   {},
	PostgresIntervalHourToSecond:   {},
	PostgresIntervalMinuteToSecond: {},
}

var intervalsRe = strings.Join(lo.Map(lo.Keys(intervals), func(item PostgresInterval, index int) string {
	return strings.ToLower(string(item))
}), "|")
