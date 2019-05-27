package collections

type listNode struct {
	Next     *listNode
	Previous *listNode
	Value    interface{}
}

// NewLinkedList returns a new Queue instance.
func NewLinkedList() Queue {
	return &LinkedList{}
}

// LinkedList is an implementation of a fifo buffer using nodes and poitners.
// Remarks; it is not threadsafe. It is constant(ish) time in all ops.
type LinkedList struct {
	head   *listNode
	tail   *listNode
	length int
}

// Len returns the length of the queue in constant time.
func (q *LinkedList) Len() int {
	return q.length
}

// Enqueue adds a new value to the queue.
func (q *LinkedList) Enqueue(value interface{}) {
	node := &listNode{Value: value}

	if q.head == nil { //the queue is empty, that is to say head is nil
		q.head = node
		q.tail = node
	} else { //the queue is not empty, we have a (valid) tail pointer
		q.tail.Previous = node
		node.Next = q.tail
		q.tail = node
	}

	q.length = q.length + 1
}

// Dequeue removes an item from the front of the queue and returns it.
func (q *LinkedList) Dequeue() interface{} {
	if q.head == nil {
		return nil
	}

	headValue := q.head.Value

	if q.length == 1 && q.head == q.tail {
		q.head = nil
		q.tail = nil
	} else {
		q.head = q.head.Previous
		if q.head != nil {
			q.head.Next = nil
		}
	}

	q.length = q.length - 1
	return headValue
}

// Peek returns the first element of the queue but does not remove it.
func (q *LinkedList) Peek() interface{} {
	if q.head == nil {
		return nil
	}
	return q.head.Value
}

// PeekBack returns the last element of the queue.
func (q *LinkedList) PeekBack() interface{} {
	if q.tail == nil {
		return nil
	}
	return q.tail.Value
}

// Clear clears the linked list.
func (q *LinkedList) Clear() {
	q.tail = nil
	q.head = nil
	q.length = 0
}

// Each calls the consumer for each element of the linked list.
func (q *LinkedList) Each(consumer func(value interface{})) {
	if q.head == nil {
		return
	}

	nodePtr := q.head
	for nodePtr != nil {
		consumer(nodePtr.Value)
		nodePtr = nodePtr.Previous
	}
}

// EachUntil calls the consumer for each element of the linked list, but can abort.
func (q *LinkedList) EachUntil(consumer func(value interface{}) bool) {
	if q.head == nil {
		return
	}

	nodePtr := q.head
	for nodePtr != nil {
		if !consumer(nodePtr.Value) {
			return
		}
		nodePtr = nodePtr.Previous
	}
}

// ReverseEachUntil calls the consumer for each element of the linked list, but can abort.
func (q *LinkedList) ReverseEachUntil(consumer func(value interface{}) bool) {
	if q.head == nil {
		return
	}

	nodePtr := q.tail
	for nodePtr != nil {
		if !consumer(nodePtr.Value) {
			return
		}
		nodePtr = nodePtr.Next
	}
}

// AsSlice returns the full contents of the queue as a slice.
func (q *LinkedList) AsSlice() []interface{} {
	if q.head == nil {
		return []interface{}{}
	}

	values := []interface{}{}
	nodePtr := q.head
	for nodePtr != nil {
		values = append(values, nodePtr.Value)
		nodePtr = nodePtr.Previous
	}
	return values
}
