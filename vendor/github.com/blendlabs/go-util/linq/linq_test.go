package linq

import (
	"strings"
	"testing"

	"github.com/blendlabs/go-assert"
)

type KVP struct {
	Key   string
	Value interface{}
}

func TestAny(t *testing.T) {
	assert := assert.New(t)

	objs := []KVP{
		{Key: "Foo", Value: "Bar"},
		{Key: "Foo2", Value: "Baz"},
		{Key: "Foo3", Value: 3},
	}

	hasBar := Any(objs, func(value interface{}) bool {
		if kvpValue, isKvp := value.(KVP); isKvp {
			if stringValue, isString := kvpValue.Value.(string); isString {
				return stringValue == "Bar"
			}
		}
		return false
	})
	assert.True(hasBar)
	hasFizz := Any(objs, func(value interface{}) bool {
		if kvpValue, isKvp := value.(KVP); isKvp {
			if stringValue, isString := kvpValue.Value.(string); isString {
				return stringValue == "Fizz"
			}
		}
		return false
	})
	assert.False(hasFizz)
	assert.True(Any(objs, DeepEqual(KVP{Key: "Foo3", Value: 3})))
}

func TestAnyOfStringIgnoreCase(t *testing.T) {
	assert := assert.New(t)
	items := []string{"foo", "FOO", "bar", "Bar", "BAR", "baZ"}
	assert.True(AnyOfString(items, EqualsOfStringCaseInsenitive("baz")))
	assert.False(AnyOfString(items, EqualsOfStringCaseInsenitive("will")))
}

func TestAll(t *testing.T) {
	assert := assert.New(t)

	objs := []KVP{
		{Key: "Foo", Value: "Bar"},
		{Key: "Foo2", Value: "Baz"},
		{Key: "Foo3", Value: 3},
	}

	hasBar := All(objs, func(value interface{}) bool {
		if kvpValue, isKvp := value.(KVP); isKvp {
			return strings.HasPrefix(kvpValue.Key, "Foo")
		}
		return false
	})
	assert.True(hasBar)
	hasFizz := All(objs, func(value interface{}) bool {
		if kvpValue, isKvp := value.(KVP); isKvp {
			return strings.HasPrefix(kvpValue.Key, "Fizz")
		}
		return false

	})
	assert.False(hasFizz)
}

func TestFirst(t *testing.T) {
	assert := assert.New(t)

	items := []float64{6.1, 3.2, 4.0, 12.4, 912.4, 912.3, 3.14}
	bigNumber := FirstOfFloat(items, func(v float64) bool {
		return v > 900
	})

	assert.NotNil(bigNumber)
	assert.Equal(912.4, *bigNumber)
}

func TestLast(t *testing.T) {
	assert := assert.New(t)

	items := []float64{6.1, 3.2, 4.0, 12.4, 912.4, 912.3, 3.14}
	bigNumber := LastOfFloat(items, func(v float64) bool {
		return v > 900
	})

	assert.NotNil(bigNumber)
	assert.Equal(912.3, *bigNumber)
}
