package collections

import (
	"sync"
)

// NewSynchronizedRingBuffer returns a new synchronized ring buffer.
func NewSynchronizedRingBuffer() *SynchronizedRingBuffer {
	return &SynchronizedRingBuffer{
		innerBuffer: NewRingBuffer(),
		syncRoot:    &sync.Mutex{},
	}
}

// NewSynchronizedRingBufferWithCapacity retrns a new synchronized ring buffer with a given initial capacity.
func NewSynchronizedRingBufferWithCapacity(capacity int) *SynchronizedRingBuffer {
	return &SynchronizedRingBuffer{
		innerBuffer: NewRingBufferWithCapacity(capacity),
		syncRoot:    &sync.Mutex{},
	}
}

// SynchronizedRingBuffer is a ring buffer wrapper that adds synchronization.
type SynchronizedRingBuffer struct {
	innerBuffer *RingBuffer
	syncRoot    *sync.Mutex
}

// SyncRoot returns the mutex used to synchronize the collection.
func (srb *SynchronizedRingBuffer) SyncRoot() *sync.Mutex {
	return srb.syncRoot
}

// InnerBuffer returns the inner ringbuffer.
func (srb *SynchronizedRingBuffer) InnerBuffer() *RingBuffer {
	return srb.innerBuffer
}

// Len returns the length of the ring buffer (as it is currently populated).
// Actual memory footprint may be different.
func (srb SynchronizedRingBuffer) Len() (val int) {
	srb.syncRoot.Lock()
	val = srb.innerBuffer.Len()
	srb.syncRoot.Unlock()
	return
}

// TotalLen returns the total size of the ring bufffer, including empty elements.
func (srb *SynchronizedRingBuffer) TotalLen() (val int) {
	srb.syncRoot.Lock()
	val = srb.innerBuffer.TotalLen()
	srb.syncRoot.Unlock()
	return
}

// Enqueue adds an element to the "back" of the RingBuffer.
func (srb *SynchronizedRingBuffer) Enqueue(value interface{}) {
	srb.syncRoot.Lock()
	srb.innerBuffer.Enqueue(value)
	srb.syncRoot.Unlock()
}

// Dequeue removes the first (oldest) element from the RingBuffer.
func (srb *SynchronizedRingBuffer) Dequeue() (val interface{}) {
	srb.syncRoot.Lock()
	val = srb.innerBuffer.Dequeue()
	srb.syncRoot.Unlock()
	return
}

// Peek returns but does not remove the first element.
func (srb *SynchronizedRingBuffer) Peek() (val interface{}) {
	srb.syncRoot.Lock()
	val = srb.innerBuffer.Peek()
	srb.syncRoot.Unlock()
	return
}

// PeekBack returns but does not remove the last element.
func (srb *SynchronizedRingBuffer) PeekBack() (val interface{}) {
	srb.syncRoot.Lock()
	val = srb.innerBuffer.PeekBack()
	srb.syncRoot.Unlock()
	return
}

// TrimExcess resizes the buffer to better fit the contents.
func (srb *SynchronizedRingBuffer) TrimExcess() {
	srb.syncRoot.Lock()
	srb.innerBuffer.TrimExcess()
	srb.syncRoot.Unlock()
}

// AsSlice returns the ring buffer, in order, as a slice.
func (srb *SynchronizedRingBuffer) AsSlice() (val []interface{}) {
	srb.syncRoot.Lock()
	val = srb.innerBuffer.AsSlice()
	srb.syncRoot.Unlock()
	return
}

// Clear removes all objects from the RingBuffer.
func (srb *SynchronizedRingBuffer) Clear() {
	srb.syncRoot.Lock()
	srb.innerBuffer.Clear()
	srb.syncRoot.Unlock()
}

// Each calls the consumer for each element in the buffer.
func (srb *SynchronizedRingBuffer) Each(consumer func(value interface{})) {
	srb.syncRoot.Lock()
	srb.innerBuffer.Each(consumer)
	srb.syncRoot.Unlock()
}

// Drain calls the consumer for each element in the buffer, while also dequeueing that entry.
func (srb *SynchronizedRingBuffer) Drain(consumer func(value interface{})) {
	srb.syncRoot.Lock()
	srb.innerBuffer.Drain(consumer)
	srb.syncRoot.Unlock()
}

// EachUntil calls the consumer for each element in the buffer with a stopping condition in head=>tail order.
func (srb *SynchronizedRingBuffer) EachUntil(consumer func(value interface{}) bool) {
	srb.syncRoot.Lock()
	srb.innerBuffer.EachUntil(consumer)
	srb.syncRoot.Unlock()
}

// ReverseEachUntil calls the consumer for each element in the buffer with a stopping condition in tail=>head order.
func (srb *SynchronizedRingBuffer) ReverseEachUntil(consumer func(value interface{}) bool) {
	srb.syncRoot.Lock()
	srb.innerBuffer.ReverseEachUntil(consumer)
	srb.syncRoot.Unlock()
}
