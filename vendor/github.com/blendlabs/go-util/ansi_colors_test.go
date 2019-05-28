package util

import (
	"testing"

	"github.com/blendlabs/go-assert"
)

func TestAnsiColorApply(t *testing.T) {
	assert := assert.New(t)

	escapedBlack := ColorBlack.escaped()
	assert.Equal("\033["+string(ColorBlack), escapedBlack)

	appliedBlack := ColorBlack.Apply("test")
	assert.Equal(ColorBlack.escaped()+"test"+ColorReset.escaped(), appliedBlack)
}
