package util

import (
	"fmt"
	"testing"

	"github.com/blendlabs/go-assert"
)

func TestUnmarshalJSON(t *testing.T) {
	assert := assert.New(t)
	valid := []string{"1", "0", "true", "false", "True", "False", `"true"`, `"false"`}
	notValid := []string{"foo", "123", "-1", "3.14", "", `""`}

	for index, value := range valid {
		var bit Boolean
		jsonErr := bit.UnmarshalJSON([]byte(value))
		assert.Nil(jsonErr)

		if index%2 == 0 {
			assert.True(bit.AsBool(), fmt.Sprintf("%s => %#v\n", value, bit))
		} else {
			assert.False(bit.AsBool())
		}
	}

	for _, value := range notValid {
		var bit Boolean
		jsonErr := bit.UnmarshalJSON([]byte(value))
		assert.NotNil(jsonErr)
	}
}
