package exception

import (
	"testing"

	assert "github.com/blendlabs/go-assert"
)

func TestGetStackTrace(t *testing.T) {
	assert := assert.New(t)

	assert.NotEmpty(GetStackTrace())
}
