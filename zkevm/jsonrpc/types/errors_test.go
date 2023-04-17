package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorConstants(t *testing.T) {
	assert.Equal(t, -32000, DefaultErrorCode)
	assert.Equal(t, -32600, InvalidRequestErrorCode)
	assert.Equal(t, -32601, NotFoundErrorCode)
	assert.Equal(t, -32602, InvalidParamsErrorCode)
	assert.Equal(t, -32700, ParserErrorCode)
}

func TestErrorMethods(t *testing.T) {
	const code, msg = 1, "err"

	var err Error = NewRPCError(code, msg)

	assert.Equal(t, code, err.ErrorCode())
	assert.Equal(t, msg, err.Error())
}
