package collections

import (
	"testing"

	"github.com/blendlabs/go-assert"
	"github.com/blendlabs/go-util"
)

func TestStringArray(t *testing.T) {
	a := assert.New(t)

	sa := StringArray([]string{"Foo", "bar", "baz"})
	a.True(sa.Contains("Foo"))
	a.False(sa.Contains("FOO"))
	a.False(sa.Contains("will"))

	a.True(sa.ContainsLower("foo"))
	a.False(sa.ContainsLower("will"))

	foo := sa.GetByLower("foo")
	a.Equal("Foo", foo)
	notFoo := sa.GetByLower("will")
	a.Equal(util.StringEmpty, notFoo)
}
