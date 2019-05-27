package collections

import "sync"

// NewChannelQueue returns a new ConcurrentQueue instance.
func NewChannelQueue(maxSize int) Queue {
	return &ChannelQueue{MaxSize: maxSize, storage: make(chan interface{}, maxSize), latch: sync.Mutex{}}
}

// ChannelQueue is a threadsafe queue.
type ChannelQueue struct {
	MaxSize int
	storage chan interface{}
	latch   sync.Mutex
}

// Len returns the number of items in the queue.
func (cq *ChannelQueue) Len() int {
	return len(cq.storage)
}

// Enqueue adds an item to the queue.
func (cq *ChannelQueue) Enqueue(item interface{}) {
	cq.storage <- item
}

// Dequeue returns the next element in the queue.
func (cq *ChannelQueue) Dequeue() interface{} {
	if len(cq.storage) != 0 {
		return <-cq.storage
	}
	return nil
}

// Peek returns (but does not remove) the first element of the queue.
func (cq *ChannelQueue) Peek() interface{} {
	if len(cq.storage) == 0 {
		return nil
	}
	elements := cq.AsSlice()
	return elements[0]
}

// PeekBack returns (but does not remove) the last element of the queue.
func (cq *ChannelQueue) PeekBack() interface{} {
	if len(cq.storage) == 0 {
		return nil
	}
	elements := cq.AsSlice()
	return elements[len(elements)-1]
}

// Clear clears the queue.
func (cq *ChannelQueue) Clear() {
	cq.storage = make(chan interface{}, cq.MaxSize)
}

// Each pulls every value out of the channel, calls consumer on it, and puts it back.
func (cq *ChannelQueue) Each(consumer func(value interface{})) {
	if len(cq.storage) == 0 {
		return
	}
	values := []interface{}{}
	for len(cq.storage) != 0 {
		v := <-cq.storage
		consumer(v)
		values = append(values, v)
	}
	for _, v := range values {
		cq.storage <- v
	}
}

// EachUntil pulls every value out of the channel, calls consumer on it, and puts it back and can abort mid process.
func (cq *ChannelQueue) EachUntil(consumer func(value interface{}) bool) {
	panic("Interupted iteration is not supported")
}

// ReverseEachUntil pulls every value out of the channel, calls consumer on it, and puts it back and can abort mid process.
func (cq *ChannelQueue) ReverseEachUntil(consumer func(value interface{}) bool) {
	panic("Reverse iteration is not supported")
}

// AsSlice iterates over the queue and returns an array of its contents.
func (cq *ChannelQueue) AsSlice() []interface{} {
	cq.latch.Lock()
	defer cq.latch.Unlock()

	values := []interface{}{}
	for len(cq.storage) != 0 {
		v := <-cq.storage
		values = append(values, v)
	}
	for _, v := range values {
		cq.storage <- v
	}
	return values
}
