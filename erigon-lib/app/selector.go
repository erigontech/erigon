package app

import (
	"context"
	"reflect"

	"github.com/erigontech/erigon-lib/app/util"
)

type Keys interface {
	Len() int

	IsArray() bool
	IsMap() bool
}

type KeyArray []interface{}

func (keys KeyArray) Len() int {
	return len(keys)
}

func (keys KeyArray) IsArray() bool {
	return true
}

func (keys KeyArray) IsMap() bool {
	return false
}

type KeyMap map[interface{}]interface{}

func (keys KeyMap) Len() int {
	return len(keys)
}

func (keys KeyMap) IsArray() bool {
	return false
}

func (keys KeyMap) IsMap() bool {
	return true
}

type Selector interface {
	Test(context context.Context, entity interface{}) bool
}

type KeyedSelector interface {
	Keys() Keys
	Test(context context.Context, entity interface{}) bool
}

// SelectorComparator provides a basic comparison on selectors by comparing
// the values of the selectors keys
func SelectorComparator(a, b interface{}) int {
	return util.CompoundCompare(a.(KeyedSelector).Keys(), b.(KeyedSelector).Keys())
}

type RangeSelector interface {
	From() Keys
	To() Keys
	Test(context context.Context, entity interface{}) bool
}

const (
	// Unordered indicates the selection order is unimportant from the selectors
	// perspective.  If the underlying entities have a natural order due to thier
	// storage that may be returned but none is assumed.  T
	Unordered SelectOrder = 0
	// The returned entities should be returned in a forward order as defined by the
	// ordering of the entities selected
	Forward SelectOrder = 1
	// The returned entities should be returned in reverse order as defined by the
	// ordering of the entities selected
	Reverse SelectOrder = -1
)

// VersionType is a constant indicating the underlying type of
//
//	the Version represented by the base Version interface
type SelectOrder int

type Ordered interface {
	Order() SelectOrder
}

type OrderedSelector interface {
	Selector
	Ordered
}

type all struct{}

func (s all) Keys() Keys {
	return KeyArray(nil)
}

func (s all) Test(context context.Context, entity interface{}) bool {
	return true
}

func (s all) CompareTo(entity interface{}) int {
	switch entity.(type) {
	case all:
		return 0
	case none:
		return 1
	}
	return -1
}

type none struct{}

func (s none) Keys() Keys {
	return KeyArray(nil)
}

func (s none) Test(context context.Context, entity interface{}) bool {
	return false
}

func (s none) CompareTo(entity interface{}) int {
	if _, ok := entity.(*none); ok {
		return 0
	}

	return -1
}

var Selectors = struct {
	Any  all
	None none
}{
	Any:  all{},
	None: none{},
}

type TypedSelector interface {
	Selector
}

type TypeSelector struct {
	Type reflect.Type
}

func (selector *TypeSelector) Keys() Keys {
	return KeyArray{selector.Type}
}

func (selector *TypeSelector) Test(context context.Context, entity interface{}) bool {
	if selector.Type != nil {
		return reflect.TypeOf(entity) == selector.Type
	}

	return true
}
