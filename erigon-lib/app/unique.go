package app

import "sync"

// this is a temporary replacement for 1.23 unique that should be replaced
// once we upgrade our base version to that

// NOTE unique this code does not clean its map - so will intern values permanently

var values = sync.Map{}

type Handle[T comparable] struct {
	value *T
}

func (h Handle[T]) Value() T {
	return *h.value
}

func Make[T comparable](value T) Handle[T] {
	// Keep around any values we allocate for insertion. There
	// are a few different ways we can race with other threads
	// and create values that we might discard. By keeping
	// the first one we make around, we can avoid generating
	// more than one per racing thread.

	ptr, ok := values.Load(value)
	if !ok {
		v := value
		ptr, _ = values.LoadOrStore(v, &v)
	}

	return Handle[T]{ptr.(*T)}
}
