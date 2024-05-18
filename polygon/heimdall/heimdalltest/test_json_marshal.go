package heimdalltest

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func AssertJsonMarshalUnmarshal[T any](t *testing.T, value *T) {
	jsonBytes, err := json.Marshal(value)
	require.NoError(t, err)

	decodedValue := new(T)
	err = json.Unmarshal(jsonBytes, decodedValue)
	require.NoError(t, err)

	assert.True(t, reflect.DeepEqual(value, decodedValue))
}
