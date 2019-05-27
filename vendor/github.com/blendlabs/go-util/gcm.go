package util

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"

	exception "github.com/blendlabs/go-exception"
)

// GCM is a namespace for gcm related utility functions
var GCM = gcmUtil{}

type gcmUtil struct{}

// Encrypt provides authenticity, integrity, and confidentiality
// It returns the nonce, ciphertext, and authenticity/integrity tag.
// All three parts are necessary during decryption, but are exposed for
// interoperability with external systems.
func (gu gcmUtil) Encrypt(key, plaintext []byte) ([]byte, []byte, []byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, nil, exception.Wrap(err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, nil, exception.Wrap(err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, nil, exception.Wrap(err)
	}

	// out is the concatination of nonce, ciphertext, and tag
	out := gcm.Seal(nonce, nonce, plaintext, nil)

	tagStart := len(out) - gcm.Overhead()
	tag := out[tagStart:]
	ciphertext := out[gcm.NonceSize():tagStart]

	return nonce, ciphertext, tag, nil
}

// Decrypt checks the authenticity tag and decrypts the ciphertext
func (gu gcmUtil) Decrypt(key, nonce, ciphertext, tag []byte) ([]byte, error) {

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, exception.Wrap(err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, exception.Wrap(err)
	}

	if len(nonce) != gcm.NonceSize() {
		return nil, exception.New("Invalid `nonce`, should be [12]byte")
	}

	message := append(ciphertext, tag...)

	out, err := gcm.Open(nil, nonce, message, nil)
	if err != nil {
		return nil, exception.Wrap(err)
	}

	return out, nil
}
