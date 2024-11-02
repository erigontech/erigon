package app

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/erigontech/erigon-lib/app/util/random"
	"golang.org/x/exp/constraints"
)

func SequentialGenerator[T constraints.Integer](initialId T) IdGenerator[T] {
	return &sequentialGenerator[T]{initialId, sync.Mutex{}}
}

type sequentialGenerator[T constraints.Integer] struct {
	nextId T
	mutex  sync.Mutex
}

func (generator *sequentialGenerator[T]) GenerateId(generationContext context.Context, entity ...interface{}) (T, error) {
	generator.mutex.Lock()
	id := generator.nextId
	generator.nextId++
	generator.mutex.Unlock()
	return id, nil
}

func FixedValueGenerator[T comparable](value T) IdGenerator[T] {
	return &fixedValueGenerator[T]{value}
}

type fixedValueGenerator[T comparable] struct {
	value T
}

func (generator *fixedValueGenerator[T]) GenerateId(generationContext context.Context, entity ...interface{}) (T, error) {
	return generator.value, nil
}

func PassThroughGenerator[T comparable]() IdGenerator[T] {
	return &passThroughGenerator[T]{}
}

type passThroughGenerator[T comparable] struct {
}

func (generator *passThroughGenerator[T]) GenerateId(generationContext context.Context, entity ...interface{}) (T, error) {
	return entity[0].(T), nil
}

func RandomGenerator[T comparable](len uint) IdGenerator[T] {
	return &randomGenerator[T]{len}
}

type randomGenerator[T comparable] struct {
	len uint
}

func (generator *randomGenerator[T]) GenerateId(generationContext context.Context, entity ...interface{}) (T, error) {
	var v T
	t := reflect.TypeOf(v)

	switch t.Kind() {
	case reflect.String:
		return ((interface{})(random.RandomString(generator.len))).(T), nil
	case reflect.Slice:
		switch t.Elem().Kind() {
		case reflect.Int8:
			return ((interface{})(random.RandomBytes(generator.len))).(T), nil
		}
	}

	return v, fmt.Errorf("can't generate random values for: %s", t)
}

// NewContextualGenerator returns an id generator which returns a generated value based on the
//
//	contents of the key passed in the received genration contexts.
//
// If the key is a:
//
//	[]byte or string value it is returned as the id
//	cri.IdGenerator it is called to generatan is
func ContextualGenerator[T comparable](key interface{}) IdGenerator[T] {
	return &contextualGenerator[T]{key}
}

type contextualGenerator[T comparable] struct {
	key interface{}
}

func (generator *contextualGenerator[T]) GenerateId(generationContext context.Context, entity ...interface{}) (T, error) {
	value := generationContext.Value(generator.key)

	var zero T
	if value == nil {
		return zero, fmt.Errorf("can't find generation key %v in context", generator.key)
	}

	if ids, ok := value.(*[]T); ok {
		if len(*ids) == 0 {
			return zero, fmt.Errorf("no more ids for generation key %v in context", generator.key)
		}

		id := (*ids)[0]
		*ids = (*ids)[1:]
		value = id
	}

	switch typed := value.(type) {
	/*	case []byte:
			return typed, nil
		case string:
			return []byte(typed), nil
		case fmt.Stringer:
			return []byte(typed.String()), nil
		case cri.IdGenerator:
			return typed.GenerateId(generationContext, entity...)
		default:
			return []byte(fmt.Sprint(typed)), nil
	*/
	case T:
		return typed, nil
	}

	return zero, fmt.Errorf("unhandled type: %T", value)
}
