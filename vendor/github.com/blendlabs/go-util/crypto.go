package util

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha512"
	"fmt"
	"io"

	exception "github.com/blendlabs/go-exception"
)

// Crypto is a namespace for crypto related functions.
var Crypto = cryptoUtil{}

type cryptoUtil struct{}

// GCMEncryptionResult is a struct for a gcm encryption result
type GCMEncryptionResult struct {
	CipherText []byte
	Nonce      []byte
}

func (cu cryptoUtil) CreateKey(keySize int) ([]byte, error) {
	key := make([]byte, keySize)
	_, err := rand.Read(key)
	if err != nil {
		return nil, err
	}
	return key, nil
}

// Encrypt encrypts data with the given key.
func (cu cryptoUtil) Encrypt(key, plainText []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	ciphertext := make([]byte, aes.BlockSize+len(plainText))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}
	cfb := cipher.NewCFBEncrypter(block, iv)
	cfb.XORKeyStream(ciphertext[aes.BlockSize:], plainText)
	return ciphertext, nil
}

// GCMEncryptionInPlace will encrypt and authenticate the plaintext with the given key. GCMEncryptionResult.CipherText will be backed in memory by the inputted plaintext []byte. Use GCMEncryption.CipherText, never plaintext.
func (cu cryptoUtil) GCMEncryptInPlace(key, plainText []byte) (*GCMEncryptionResult, error) {
	return cu.GCMEncrypt(key, plainText, plainText[:0])
}

// GCMEncrypt encrypts and authenticates the plaintext with the given key. dst is the destination slice for the encrypted data
func (cu cryptoUtil) GCMEncrypt(key, plainText, dst []byte) (*GCMEncryptionResult, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, exception.Wrap(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, exception.Wrap(err)
	}
	nonce := make([]byte, aead.NonceSize())
	_, err = rand.Read(nonce)
	if err != nil {
		return nil, exception.Wrap(err)
	}
	dst = aead.Seal(dst, nonce, plainText, nil)
	return &GCMEncryptionResult{CipherText: dst, Nonce: nonce}, nil
}

// GCMDecryptInPlace decrypts the ciphertext in place. GCMEncryptionResult.CipherText will be used as the backing memory, but only use the returned slice
func (cu cryptoUtil) GCMDecryptInPlace(key []byte, gcm *GCMEncryptionResult) ([]byte, error) {
	return cu.GCMDecrypt(key, gcm, gcm.CipherText[:0])
}

// GCMDecrypt decrypts and authenticates the cipherText with the given key
func (cu cryptoUtil) GCMDecrypt(key []byte, gcm *GCMEncryptionResult, dst []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, exception.Wrap(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, exception.Wrap(err)
	}
	dst, err = aead.Open(dst, gcm.Nonce, gcm.CipherText, nil)
	return dst, exception.Wrap(err)
}

// Decrypt decrypts data with the given key.
func (cu cryptoUtil) Decrypt(key, cipherText []byte) ([]byte, error) {
	if len(cipherText) < aes.BlockSize {
		return nil, exception.New(fmt.Sprintf("Cannot decrypt string: `cipherText` is smaller than AES block size (%v)", aes.BlockSize))
	}

	iv := cipherText[:aes.BlockSize]
	cipherText = cipherText[aes.BlockSize:]

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	cfb := cipher.NewCFBDecrypter(block, iv)
	cfb.XORKeyStream(cipherText, cipherText)
	return cipherText, nil
}

// Hash hashes data with the given key.
func (cu cryptoUtil) Hash(key, plainText []byte) []byte {
	mac := hmac.New(sha512.New, key)
	mac.Write([]byte(plainText))
	return mac.Sum(nil)
}
