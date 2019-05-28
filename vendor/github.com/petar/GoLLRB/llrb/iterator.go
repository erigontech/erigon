package llrb

type ItemIterator func(i Item) bool

//func (t *Tree) Ascend(iterator ItemIterator) {
//	t.AscendGreaterOrEqual(Inf(-1), iterator)
//}

func (t *LLRB) AscendRange(greaterOrEqual, lessThan Item, iterator ItemIterator) {
	t.ascendRange(t.root, greaterOrEqual, lessThan, iterator)
}

func (t *LLRB) ascendRange(h *Node, inf, sup Item, iterator ItemIterator) bool {
	if h == nil {
		return true
	}
	if !less(h.Item, sup) {
		return t.ascendRange(h.Left, inf, sup, iterator)
	}
	if less(h.Item, inf) {
		return t.ascendRange(h.Right, inf, sup, iterator)
	}

	if !t.ascendRange(h.Left, inf, sup, iterator) {
		return false
	}
	if !iterator(h.Item) {
		return false
	}
	return t.ascendRange(h.Right, inf, sup, iterator)
}

// AscendGreaterOrEqual will call iterator once for each element greater or equal to
// pivot in ascending order. It will stop whenever the iterator returns false.
func (t *LLRB) AscendGreaterOrEqual(pivot Item, iterator ItemIterator) {
	t.ascendGreaterOrEqual(t.root, pivot, iterator)
}

// AscendGreaterOrEqual will call iterator once for each element greater or equal to
// pivot in ascending order. It will stop whenever the iterator returns false.
func (t *LLRB) AscendGreaterOrEqual1(pivot Item, iterator ItemIterator) {
	// Estimate the depth of the tree to allocate the stack
	var stack [32]*Node
	var noLeft bool
	var top int
	stack[0] = t.root
	for top >= 0 {
		h := stack[top]
		if h == nil {
			top--
			noLeft = true
			continue
		}
		if less(h.Item, pivot) {
			// Left branch will not be explored, so we replace the top of the stack with the right branch
			noLeft = false
			stack[top] = h.Right
			continue
		}
		if noLeft {
			if !iterator(h.Item) {
				return
			}
			noLeft = false
			stack[top] = h.Right
			continue
		}
		top++
		noLeft = false
		stack[top] = h.Left
	}
}

func (t *LLRB) ascendGreaterOrEqual(h *Node, pivot Item, iterator ItemIterator) bool {
	if h == nil {
		return true
	}
	if !less(h.Item, pivot) {
		if !t.ascendGreaterOrEqual(h.Left, pivot, iterator) {
			return false
		}
		if !iterator(h.Item) {
			return false
		}
	}
	return t.ascendGreaterOrEqual(h.Right, pivot, iterator)
}

func (t *LLRB) AscendLessThan(pivot Item, iterator ItemIterator) {
	t.ascendLessThan(t.root, pivot, iterator)
}

func (t *LLRB) ascendLessThan(h *Node, pivot Item, iterator ItemIterator) bool {
	if h == nil {
		return true
	}
	if !t.ascendLessThan(h.Left, pivot, iterator) {
		return false
	}
	if !iterator(h.Item) {
		return false
	}
	if less(h.Item, pivot) {
		return t.ascendLessThan(h.Right, pivot, iterator)
	}
	return true
}

// DescendLessOrEqual will call iterator once for each element less than the
// pivot in descending order. It will stop whenever the iterator returns false.
func (t *LLRB) DescendLessOrEqual(pivot Item, iterator ItemIterator) {
	t.descendLessOrEqual(t.root, pivot, iterator)
}

func (t *LLRB) descendLessOrEqual(h *Node, pivot Item, iterator ItemIterator) bool {
	if h == nil {
		return true
	}
	if less(h.Item, pivot) || !less(pivot, h.Item) {
		if !t.descendLessOrEqual(h.Right, pivot, iterator) {
			return false
		}
		if !iterator(h.Item) {
			return false
		}
	}
	return t.descendLessOrEqual(h.Left, pivot, iterator)
}

type SeekIterator struct {
	stack [32]*Node
	noLeft bool
	top int
}

func (t *LLRB) NewSeekIterator() *SeekIterator {
	si := &SeekIterator{}
	si.stack[0] = t.root
	return si
}

// Moves the iterator to the specified goal (forward only)
// Returns the found item or the one that is next in the order, or nil
// if there are no more items
func (si *SeekIterator) SeekTo(goal Item) Item {
	top := si.top
	stack := si.stack[:]
	noLeft := si.noLeft
	// First, go down the stack to the item that is not bigger
	for top > 0 && less(stack[top-1].Item, goal) {
		top--
		noLeft = true
	}
	for top >= 0 {
		h := stack[top]
		if h == nil {
			top--
			noLeft = true
			break
		}
		if less(h.Item, goal) {
			// Left branch will not be explored, so we replace the top of the stack with the right branch
			stack[top] = h.Right
			noLeft = false
		} else if less(goal, h.Item) && !noLeft {
			top++
			stack[top] = h.Left
			noLeft = false
		} else {
			break
		}
	}
	var result Item
	if top >= 0 {
		result = stack[top].Item
		// Make extra step
		if stack[top].Right != nil {
			stack[top] = stack[top].Right
			noLeft = false
		} else {
			top--
			noLeft = true
		}
	}
	si.top = top
	si.noLeft = noLeft
	return result
}
