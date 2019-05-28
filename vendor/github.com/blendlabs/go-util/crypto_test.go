package util

import (
	"testing"

	assert "github.com/blendlabs/go-assert"
)

func TestCryptoEncryptDecrypt(t *testing.T) {
	assert := assert.New(t)
	key, err := Crypto.CreateKey(32)
	assert.Nil(err)
	plaintext := "Mary Jane Hawkins"

	ciphertext, err := Crypto.Encrypt(key, []byte(plaintext))
	assert.Nil(err)

	decryptedPlaintext, err := Crypto.Decrypt(key, ciphertext)
	assert.Nil(err)
	assert.Equal(plaintext, string(decryptedPlaintext))
}

func TestCryptoHash(t *testing.T) {
	assert := assert.New(t)
	key, err := Crypto.CreateKey(128)
	assert.Nil(err)
	plaintext := "123-12-1234"
	assert.Equal(
		Crypto.Hash(key, []byte(plaintext)),
		Crypto.Hash(key, []byte(plaintext)),
	)
}

func TestCryptoGCMEncryptDecrypt(t *testing.T) {
	assert := assert.New(t)
	key, err := Crypto.CreateKey(32)
	assert.Nil(err)
	plaintext := "Mary Jane Hawkins"
	pt := []byte(plaintext)

	dst := make([]byte, 0)
	gcm, err := Crypto.GCMEncrypt(key, pt, dst)
	assert.Nil(err)

	decryptedPlaintext, err := Crypto.GCMDecrypt(key, gcm, dst)
	assert.Nil(err)
	assert.Equal(plaintext, string(decryptedPlaintext))
}

func TestCryptoGCMEncryptDecryptInPlace(t *testing.T) {
	assert := assert.New(t)
	key, err := Crypto.CreateKey(32)
	assert.Nil(err)
	plaintext := "Mary Jane Hawkins"
	pt := []byte(plaintext)

	gcm, err := Crypto.GCMEncryptInPlace(key, pt)
	assert.Nil(err)

	decryptedPlaintext, err := Crypto.GCMDecryptInPlace(key, gcm)
	assert.Nil(err)
	assert.Equal(plaintext, string(decryptedPlaintext))
}
