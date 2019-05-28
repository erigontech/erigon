package collections

import (
	"testing"

	"github.com/blendlabs/go-assert"
)

func TestConcurrentQueue(t *testing.T) {
	a := assert.New(t)

	q := NewChannelQueue(4)
	a.Empty(q.AsSlice())
	a.Nil(q.Dequeue())
	a.Equal(0, q.Len())

	q.Enqueue("foo")
	a.Equal(1, q.Len())

	q.Enqueue("bar")
	a.Equal(2, q.Len())

	q.Enqueue("baz")
	a.Equal(3, q.Len())

	q.Enqueue("fizz")
	a.Equal(4, q.Len())

	values := q.AsSlice()
	a.Len(values, 4)
	a.Equal("foo", values[0])
	a.Equal("bar", values[1])
	a.Equal("baz", values[2])
	a.Equal("fizz", values[3])

	shouldBeFoo := q.Dequeue()
	a.Equal("foo", shouldBeFoo)
	a.Equal(3, q.Len())

	shouldBeBar := q.Dequeue()
	a.Equal("bar", shouldBeBar)
	a.Equal(2, q.Len())

	shouldBeBaz := q.Dequeue()
	a.Equal("baz", shouldBeBaz)
	a.Equal(1, q.Len())

	shouldBeFizz := q.Dequeue()
	a.Equal("fizz", shouldBeFizz)
	a.Equal(0, q.Len())
}
