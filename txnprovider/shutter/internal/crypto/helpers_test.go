package crypto

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/stretchr/testify/assert"
)

func EnsureGobable(t *testing.T, src, dst interface{}) {
	t.Helper()
	buff := bytes.Buffer{}
	err := gob.NewEncoder(&buff).Encode(src)
	assert.NoError(t, err)
	err = gob.NewDecoder(&buff).Decode(dst)
	assert.NoError(t, err)
	assert.Equal(t, src, dst)
}
