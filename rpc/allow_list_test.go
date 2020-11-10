package rpc

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAllowListMarshaling(t *testing.T) {

}

func TestAllowListUnmarshaling(t *testing.T) {
	allowListJSON := `[ "one", "two", "three" ]`

	var allowList AllowList
	err := json.Unmarshal([]byte(allowListJSON), &allowList)
	assert.NoError(t, err, "should unmarshal successfully")

	m := map[string]struct{}{"one": {}, "two": {}, "three": {}}
	assert.Equal(t, allowList, AllowList(m))
}
