package util

import (
	"testing"

	assert "github.com/blendlabs/go-assert"
)

func TestCompress(t *testing.T) {
	assert := assert.New(t)

	contents := []byte(String.RandomString(1024))
	compressed, err := GZip.Compress(contents)
	assert.Nil(err)
	assert.NotEmpty(compressed)
	assert.NotEqual(string(compressed), string(contents))

	assert.True(len(contents) > len(compressed))

	decompressed, err := GZip.Decompress(compressed)
	assert.Nil(err)
	assert.NotEmpty(decompressed)
	assert.Equal(string(contents), string(decompressed))
}
