package collections

import (
	"testing"

	assert "github.com/blendlabs/go-assert"
)

func TestSynchronizedRingBuffer(t *testing.T) {
	assert := assert.New(t)

	buffer := NewSynchronizedRingBuffer()

	buffer.Enqueue(1)
	assert.Equal(1, buffer.Len())
	assert.Equal(1, buffer.Peek())
	assert.Equal(1, buffer.PeekBack())

	buffer.Enqueue(2)
	assert.Equal(2, buffer.Len())
	assert.Equal(1, buffer.Peek())
	assert.Equal(2, buffer.PeekBack())

	buffer.Enqueue(3)
	assert.Equal(3, buffer.Len())
	assert.Equal(1, buffer.Peek())
	assert.Equal(3, buffer.PeekBack())

	buffer.Enqueue(4)
	assert.Equal(4, buffer.Len())
	assert.Equal(1, buffer.Peek())
	assert.Equal(4, buffer.PeekBack())

	buffer.Enqueue(5)
	assert.Equal(5, buffer.Len())
	assert.Equal(1, buffer.Peek())
	assert.Equal(5, buffer.PeekBack())

	buffer.Enqueue(6)
	assert.Equal(6, buffer.Len())
	assert.Equal(1, buffer.Peek())
	assert.Equal(6, buffer.PeekBack())

	buffer.Enqueue(7)
	assert.Equal(7, buffer.Len())
	assert.Equal(1, buffer.Peek())
	assert.Equal(7, buffer.PeekBack())

	buffer.Enqueue(8)
	assert.Equal(8, buffer.Len())
	assert.Equal(1, buffer.Peek())
	assert.Equal(8, buffer.PeekBack())

	value := buffer.Dequeue()
	assert.Equal(1, value)
	assert.Equal(7, buffer.Len())
	assert.Equal(2, buffer.Peek())
	assert.Equal(8, buffer.PeekBack())

	value = buffer.Dequeue()
	assert.Equal(2, value)
	assert.Equal(6, buffer.Len())
	assert.Equal(3, buffer.Peek())
	assert.Equal(8, buffer.PeekBack())

	value = buffer.Dequeue()
	assert.Equal(3, value)
	assert.Equal(5, buffer.Len())
	assert.Equal(4, buffer.Peek())
	assert.Equal(8, buffer.PeekBack())

	value = buffer.Dequeue()
	assert.Equal(4, value)
	assert.Equal(4, buffer.Len())
	assert.Equal(5, buffer.Peek())
	assert.Equal(8, buffer.PeekBack())

	value = buffer.Dequeue()
	assert.Equal(5, value)
	assert.Equal(3, buffer.Len())
	assert.Equal(6, buffer.Peek())
	assert.Equal(8, buffer.PeekBack())

	value = buffer.Dequeue()
	assert.Equal(6, value)
	assert.Equal(2, buffer.Len())
	assert.Equal(7, buffer.Peek())
	assert.Equal(8, buffer.PeekBack())

	value = buffer.Dequeue()
	assert.Equal(7, value)
	assert.Equal(1, buffer.Len())
	assert.Equal(8, buffer.Peek())
	assert.Equal(8, buffer.PeekBack())

	value = buffer.Dequeue()
	assert.Equal(8, value)
	assert.Equal(0, buffer.Len())
	assert.Nil(buffer.Peek())
	assert.Nil(buffer.PeekBack())
}

func TestSynchronizedRingBufferClear(t *testing.T) {
	assert := assert.New(t)

	buffer := NewSynchronizedRingBuffer()
	buffer.Enqueue(1)
	buffer.Enqueue(1)
	buffer.Enqueue(1)
	buffer.Enqueue(1)
	buffer.Enqueue(1)
	buffer.Enqueue(1)
	buffer.Enqueue(1)
	buffer.Enqueue(1)

	assert.Equal(8, buffer.Len())

	buffer.Clear()
	assert.Equal(0, buffer.Len())
	assert.Nil(buffer.Peek())
	assert.Nil(buffer.PeekBack())
}

func TestSynchronizedRingBufferAsSlice(t *testing.T) {
	assert := assert.New(t)

	buffer := NewSynchronizedRingBuffer()
	buffer.Enqueue(1)
	buffer.Enqueue(2)
	buffer.Enqueue(3)
	buffer.Enqueue(4)
	buffer.Enqueue(5)

	contents := buffer.AsSlice()
	assert.Len(contents, 5)
	assert.Equal(1, contents[0])
	assert.Equal(2, contents[1])
	assert.Equal(3, contents[2])
	assert.Equal(4, contents[3])
	assert.Equal(5, contents[4])
}

func TestSynchronizedRingBufferEach(t *testing.T) {
	assert := assert.New(t)

	buffer := NewSynchronizedRingBuffer()

	for x := 1; x < 17; x++ {
		buffer.Enqueue(x)
	}

	var called int
	buffer.Each(func(v interface{}) {
		if typed, isTyped := v.(int); isTyped {
			if typed == (called + 1) {
				called++
			}
		}
	})

	assert.Equal(16, called)
}

func TestSynchronizedRingBufferDrain(t *testing.T) {
	assert := assert.New(t)

	buffer := NewSynchronizedRingBuffer()

	for x := 1; x < 17; x++ {
		buffer.Enqueue(x)
	}

	assert.Equal(16, buffer.Len())

	var called int
	buffer.Drain(func(v interface{}) {
		if _, isTyped := v.(int); isTyped {
			called++
		}
	})

	assert.Equal(16, called)
	assert.Zero(buffer.Len())
}
