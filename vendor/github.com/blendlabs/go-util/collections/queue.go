package collections

// Queue is an interface for implementations of a FIFO buffer.
type Queue interface {
	Len() int
	Enqueue(value interface{})
	Dequeue() interface{}
	Peek() interface{}
	PeekBack() interface{}
	AsSlice() []interface{}
	Clear()

	Each(consumer func(value interface{}))
	EachUntil(consumer func(value interface{}) bool)
	ReverseEachUntil(consumer func(value interface{}) bool)
}
