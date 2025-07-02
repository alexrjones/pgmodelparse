package pgmodelparse

import (
	"fmt"
	"regexp"
	"strings"
)

type TypeRegistry struct {
	simpleMatches  map[string]*PostgresType
	patternMatches []patternMatch
	casts          map[*PostgresType][]*PostgresType
}

type patternMatch struct {
	regex *regexp.Regexp
	typ   *PostgresType
}

func NewTypeRegistry() *TypeRegistry {

	ret := &TypeRegistry{
		simpleMatches:  make(map[string]*PostgresType),
		patternMatches: make([]patternMatch, 0, 10),
	}
	registerBasicTypes(ret)
	return ret
}

func registerBasicTypes(t *TypeRegistry) {
	for _, typ := range defaultPGTypes {
		err := t.RegisterType(typ)
		if err != nil {
			panic(err)
		}
	}
}

func (t *TypeRegistry) RegisterType(typ *PostgresType) error {
	for _, sm := range typ.SimpleMatches {
		if oldTyp, ok := t.simpleMatches[sm]; ok {
			return fmt.Errorf("name %s already matches type %s", sm, oldTyp.Name)
		}
		t.simpleMatches[sm] = typ
	}
	for _, pm := range typ.PatternMatches {
		t.patternMatches = append(t.patternMatches, patternMatch{
			regex: pm,
			typ:   typ,
		})
	}
	return nil
}

func (t *TypeRegistry) CanCast(from, to *PostgresType) bool {
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

func (t *TypeRegistry) MatchType(s string) *PostgresType {
	s = strings.ToLower(s)
	if typ, ok := t.simpleMatches[s]; ok {
		return typ
	}
	for _, p := range t.patternMatches {
		if p.regex.MatchString(s) {
			return p.typ
		}
	}
	panic("didn't match")
}
