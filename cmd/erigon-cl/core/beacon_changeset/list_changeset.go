package beacon_changeset

import (
	"sort"
)

type ListChangeSet[T any] struct {
	list       []*listElementChangeset[T]
	listLength int
	nextId     int
	compact    bool
}

type listElementChangeset[T any] struct {
	value     T
	listIndex int
	id        int
}

// NewListChangeSet creates new list with given length.
func NewListChangeSet[T any](length int) *ListChangeSet[T] {
	return &ListChangeSet[T]{listLength: length}
}

// AddChange appens to a new change to the changeset of the list.
func (l *ListChangeSet[T]) AddChange(index int, elem T) {
	l.compact = false
	l.list = append(l.list, &listElementChangeset[T]{
		value:     elem,
		listIndex: index,
		id:        l.nextId,
	})
	l.nextId++
}

// CompactChanges removes duplicates from a list using QuickSort and linear scan.
// duplicates may appear if one state parameter is changed more than once.
func (l *ListChangeSet[T]) CompactChanges() {
	if l.compact {
		return
	}
	l.compact = true
	// Check if there are any duplicates to remove.
	if len(l.list) < 2 {
		return
	}

	// Sort the list using QuickSort.
	sort.Slice(l.list, func(i, j int) bool {
		if l.list[i].listIndex == l.list[j].listIndex {
			return l.list[i].id < l.list[j].id
		}
		return l.list[i].listIndex < l.list[j].listIndex
	})

	// Create a new list buffer for the compacted list.
	compactList := []*listElementChangeset[T]{}

	// Do a linear scan through the sorted list and remove duplicates.
	previousIndexElement := l.list[0]
	for _, listElement := range l.list {
		if listElement.listIndex != previousIndexElement.listIndex {
			compactList = append(compactList, previousIndexElement)
		}
		previousIndexElement = listElement
	}
	compactList = append(compactList, previousIndexElement)

	// Update the original list with the compacted list.
	l.list = compactList
}

// CompactChangesReverse removes duplicates from a list using QuickSort and linear scan.
// duplicates may appear if one state parameter is changed more than once.
// Difference with CompactChanges is that the sorting is reversed.
func (l *ListChangeSet[T]) CompactChangesReverse() {
	if l.compact {
		return
	}
	l.compact = true
	// Check if there are any duplicates to remove.
	if len(l.list) < 2 {
		return
	}

	// Sort the list using QuickSort.
	sort.Slice(l.list, func(i, j int) bool {
		if l.list[i].listIndex == l.list[j].listIndex {
			return l.list[i].id > l.list[j].id
		}
		return l.list[i].listIndex < l.list[j].listIndex
	})

	// Create a new list buffer for the compacted list.
	compactList := []*listElementChangeset[T]{}

	// Do a linear scan through the sorted list and remove duplicates.
	previousIndexElement := l.list[0]
	for _, listElement := range l.list {
		if listElement.listIndex != previousIndexElement.listIndex {
			compactList = append(compactList, previousIndexElement)
		}
		previousIndexElement = listElement
	}
	compactList = append(compactList, previousIndexElement)
	// Update the original list with the compacted list.
	l.list = compactList
}

// ApplyChanges Apply changes without any mercy. if it is reverse, you need to call CompactChangesReverse before.
func (l *ListChangeSet[T]) ApplyChanges(input []T) (output []T, changed bool) {
	if len(l.list) == 0 && l.listLength == len(input) {
		output = input
		return
	}
	changed = true
	// Re-adjust list size.
	output = make([]T, l.listLength)
	copy(output, input)
	// Now apply changes to the given list
	for _, elem := range l.list {
		if elem.listIndex >= len(output) {
			continue
		}
		output[elem.listIndex] = elem.value
	}
	return
}

// ChangesWithHandler uses custom handler to handle changes.
func (l *ListChangeSet[T]) ChangesWithHandler(fn func(value T, index int)) {
	// Now apply changes to the given list
	for _, elem := range l.list {
		fn(elem.value, elem.listIndex)
	}
}

// ListLength return full list length
func (l *ListChangeSet[T]) ListLength() int {
	return l.listLength
}

// Empty return whether current list diff is empty
func (l *ListChangeSet[T]) Empty() bool {
	return len(l.list) == 0
}
