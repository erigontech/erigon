package util

import (
	"bytes"
	"io/ioutil"
	"testing"

	assert "github.com/blendlabs/go-assert"
)

func TestStreamEncrypterDecrypter(t *testing.T) {
	assert := assert.New(t)
	key, err := Crypto.CreateKey(32)
	assert.Nil(err)
	plaintext := "Eleven is the best person in all of Hawkins Indiana. KSHVdyveduytvadsguvdsjgcv"
	pt := []byte(plaintext)

	src := bytes.NewReader(pt)

	se, err := NewStreamEncrypter(key, src)
	assert.Nil(err)
	assert.NotNil(se)

	encrypted, err := ioutil.ReadAll(se)
	assert.Nil(err)
	assert.NotNil(encrypted)

	sd, err := NewStreamDecrypter(key, se.Meta(), bytes.NewReader(encrypted))
	assert.Nil(err)
	assert.NotNil(sd)

	decrypted, err := ioutil.ReadAll(sd)
	assert.Nil(err)
	assert.Equal(plaintext, string(decrypted))

	assert.Nil(sd.Authenticate())
}
