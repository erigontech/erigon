package app

import (
	"context"
	"encoding"
	"fmt"

	"github.com/erigontech/erigon-lib/app/util"
)

type Incarnation interface {
	Root() uint64
	Version() *SemanticVersion
	String() string

	Equals(o Incarnation) bool
	CompareTo(o Incarnation) int

	asIdent() ident
}

type Id interface {
	KeyedSelector
	encoding.TextMarshaler

	Domain() Domain
	Value() any
	String() string

	Equals(other Id) bool
	Matches(other Id) bool
	CompareTo(other any) int
}

type Identifiable interface {
	Id() Id
}

type IdGenerator[T comparable] interface {
	GenerateId(context context.Context, values ...interface{}) (T, error)
}

type components[T comparable] struct {
	domain Handle[Domain]
	value  Handle[T]
}

func NewId[T comparable](d Domain, t T) (Id, error) {
	return id[T](Make(components[T]{Make(d), Make(t)})), nil
}

type id[T comparable] Handle[components[T]]

var _ Id = id[string]{}

func (i id[T]) Domain() Domain {
	return ((Handle[components[T]])(i)).Value().domain.Value()
}

func (i id[T]) Value() any {
	return ((Handle[components[T]])(i)).Value().value.Value()
}

func (i id[T]) String() string {
	components := ((Handle[components[T]])(i)).Value()
	return fmt.Sprintf("%s:%v", components.domain.Value(), components.value.Value())
}

func (i id[T]) MarshalText() (text []byte, err error) {
	return []byte(i.String()), nil
}

func (i id[T]) Keys() Keys {
	value := ((Handle[components[T]])(i)).Value()
	return KeyArray{value.domain.Value(), value.value.Value()}
}

func (i id[T]) Test(context context.Context, entity interface{}) bool {
	if entity, ok := entity.(id[T]); ok {
		return i == entity
	}
	return false
}

func (i id[T]) Equals(other Id) bool {
	if other, ok := other.(id[T]); ok {
		return i == other
	}
	return false
}

func (i id[T]) Matches(other Id) bool {
	return i.Equals(other) // TODO
}

func (i id[T]) CompareTo(other interface{}) int {
	if other, ok := other.(id[T]); ok {
		otherComponents := ((Handle[components[T]])(other)).Value()
		components := ((Handle[components[T]])(i)).Value()

		if comp := components.domain.Value().CompareTo(otherComponents.domain.Value()); comp != 0 {
			return comp
		}

		return util.Compare(components.value.Value(), otherComponents.value.Value())
	}

	return -1
}
