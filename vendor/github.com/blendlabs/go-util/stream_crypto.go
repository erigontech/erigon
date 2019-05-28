package util

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"hash"
	"io"
	"math/rand"

	exception "github.com/blendlabs/go-exception"
)

// EncryptedStream is metadata about an encrypted stream
type EncryptedStream struct {
	// IV is the initial value for the crypto function
	IV []byte
	// Hash is the sha256 hmac of the stream
	Hash []byte
}

// StreamEncrypter is an encrypter for a stream of data with authentication
type StreamEncrypter struct {
	src    io.Reader
	block  cipher.Block
	stream cipher.Stream
	mac    hash.Hash
	iv     []byte
}

// StreamDecrypter is a decrypter for a stream of data with authentication
type StreamDecrypter struct {
	src    io.Reader
	block  cipher.Block
	stream cipher.Stream
	mac    hash.Hash
	meta   *EncryptedStream
}

// NewStreamEncrypter creates a new stream encrypter
func NewStreamEncrypter(key []byte, plainText io.Reader) (*StreamEncrypter, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, exception.Wrap(err)
	}
	iv := make([]byte, block.BlockSize())
	_, err = rand.Read(iv)
	if err != nil {
		return nil, exception.Wrap(err)
	}
	stream := cipher.NewCTR(block, iv)
	mac := hmac.New(sha256.New, key)
	return &StreamEncrypter{
		src:    plainText,
		block:  block,
		stream: stream,
		mac:    mac,
		iv:     iv,
	}, nil
}

// NewStreamDecrypter creates a new stream decrypter
func NewStreamDecrypter(key []byte, meta *EncryptedStream, cipherText io.Reader) (*StreamDecrypter, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, exception.Wrap(err)
	}
	stream := cipher.NewCTR(block, meta.IV)
	mac := hmac.New(sha256.New, key)
	return &StreamDecrypter{
		src:    cipherText,
		block:  block,
		stream: stream,
		mac:    mac,
		meta:   meta,
	}, nil
}

// Read encrypts the bytes of the inner reader and places them into p
func (s *StreamEncrypter) Read(p []byte) (int, error) {
	n, readErr := s.src.Read(p)
	if n > 0 {
		err := writeHash(s.mac, p[:n])
		if err != nil {
			return n, exception.Wrap(err)
		}
		s.stream.XORKeyStream(p[:n], p[:n])
		return n, readErr
	}
	return 0, io.EOF
}

// Meta returns the encrypted stream metadata for use in decrypting. This should only be called after the stream is finished
func (s *StreamEncrypter) Meta() *EncryptedStream {
	return &EncryptedStream{IV: s.iv, Hash: s.mac.Sum(nil)}
}

// Read reads bytes from the underlying reader and then decrypts them
func (s *StreamDecrypter) Read(p []byte) (int, error) {
	n, readErr := s.src.Read(p)
	if n > 0 {
		s.stream.XORKeyStream(p[:n], p[:n])
		err := writeHash(s.mac, p[:n])
		if err != nil {
			return n, exception.Wrap(err)
		}
		return n, readErr
	}
	return 0, io.EOF
}

// Authenticate verifys that the hash of the stream is correct. This should only be called after processing is finished
func (s *StreamDecrypter) Authenticate() error {
	if !hmac.Equal(s.meta.Hash, s.mac.Sum(nil)) {
		return exception.Newf("Authentication failed")
	}
	return nil
}

func writeHash(mac hash.Hash, p []byte) error {
	m, err := mac.Write(p)
	if err != nil {
		return exception.Wrap(err)
	}
	if m != len(p) {
		return exception.Newf("Could not write all bytes to Hmac")
	}
	return nil
}

func writeToStream(p []byte, dst io.Writer) (int, error) {
	n, err := dst.Write(p)
	if err != nil {
		return n, exception.Wrap(err)
	}
	if n != len(p) {
		return n, exception.Newf("Unable to write all bytes")
	}
	return len(p), nil
}
