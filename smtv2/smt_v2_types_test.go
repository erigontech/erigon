package smtv2

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSmtValue8_ToBytes(t *testing.T) {
	value := SmtValue8{1, 2, 3, 4, 5, 6, 7, 8}
	bytes := value.ToBytes()

	fromBytes := value.FromBytes(bytes)

	assert.Equal(t, value, fromBytes)
}
