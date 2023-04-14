package beacon_changeset

import (
	"github.com/tidwall/btree"
)

type ListChangeSet[T any] struct {
	list       *btree.Map[int, T]
	listLength int
	replace    bool
	// maps hits for each index
	hitTable map[int]bool
}

// NewListChangeSet creates new list with given length.
func NewListChangeSet[T any](length int, replace bool) *ListChangeSet[T] {
	return &ListChangeSet[T]{
		listLength: length,
		list:       btree.NewMap[int, T](32),
		replace:    replace,
		hitTable:   map[int]bool{},
	}
}

// AddChange appens to a new change to the changeset of the list.
func (l *ListChangeSet[T]) AddChange(index int, elem T) {
	if !l.replace && l.hitTable[index] {
		return
	}
	l.hitTable[index] = true
	l.list.Set(index, elem)
}

// OnAddNewElement handles addition of new element to list set of changes.
func (l *ListChangeSet[T]) OnAddNewElement(elem T) {
	l.AddChange(l.listLength, elem)
	l.listLength++
}

// ApplyChanges Apply changes without any mercy. if it is reverse, you need to call CompactChangesReverse before.
func (l *ListChangeSet[T]) ApplyChanges(input []T) (output []T, changed bool) {
	if l.list.Len() == 0 && l.listLength == len(input) {
		output = input
		return
	}
	changed = true
	// Re-adjust list size.
	output = make([]T, l.listLength)
	copy(output, input)

	l.list.Scan(func(key int, value T) bool {
		if key < len(output) {
			output[key] = value
		}
		return true
	})
	return
}

// ChangesWithHandler uses custom handler to handle changes.
func (l *ListChangeSet[T]) ChangesWithHandler(fn func(value T, index int)) {
	l.list.Scan(func(key int, value T) bool {
		fn(value, key)
		return true
	})
}

func (l *ListChangeSet[T]) CompactChanges() {
	l.hitTable = nil
}

// ListLength return full list length
func (l *ListChangeSet[T]) ListLength() int {
	return l.listLength
}

// Empty return whether current list diff is empty
func (l *ListChangeSet[T]) Empty() bool {
	return l.list.Len() == 0
}

// Clear clears the changesets and re-adjust length.
func (l *ListChangeSet[T]) Clear(newLength int) {
	l.list.Clear()
	l.listLength = newLength
}
