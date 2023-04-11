package beacon_changeset

type ListChangeSet[T any] struct {
	list       map[int]T
	listLength int
	replace    bool
}

// NewListChangeSet creates new list with given length.
func NewListChangeSet[T any](length int, replace bool) *ListChangeSet[T] {
	return &ListChangeSet[T]{
		listLength: length,
		replace:    replace,
		list:       map[int]T{},
	}
}

// AddChange appens to a new change to the changeset of the list.
func (l *ListChangeSet[T]) AddChange(index int, elem T) {
	if _, hasAlready := l.list[index]; !l.replace && hasAlready {
		return
	}
	l.list[index] = elem
}

// OnAddNewElement handles addition of new element to list set of changes.
func (l *ListChangeSet[T]) OnAddNewElement(elem T) {
	l.AddChange(l.listLength, elem)
	l.listLength++
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
	for index, elem := range l.list {
		if index >= len(output) {
			continue
		}
		output[index] = elem
	}
	return
}

// ChangesWithHandler uses custom handler to handle changes.
func (l *ListChangeSet[T]) ChangesWithHandler(fn func(value T, index int)) {
	// Now apply changes to the given list
	for index, elem := range l.list {
		fn(elem, index)
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

// Clear clears the changesets and re-adjust length.
func (l *ListChangeSet[T]) Clear(newLength int) {
	l.list = make(map[int]T)
	l.listLength = newLength
}
