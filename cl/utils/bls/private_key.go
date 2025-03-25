package bls

import (
	"crypto/rand"
	"crypto/subtle"
	"fmt"

	blst "github.com/supranational/blst/bindings/go"
)

const privateKeyLength = 32

// PrivateKey defines a BLS private key.
type PrivateKey struct {
	key *blst.SecretKey
}

// GenerateKey creates a new random private key.
func GenerateKey() (*PrivateKey, error) {
	// Generate random bytes.
	bytes := make([]byte, privateKeyLength)
	_, err := rand.Read(bytes)
	if err != nil {
		return nil, err
	}
	// Check that private key is not 0.
	privateKey := &PrivateKey{key: blst.KeyGen(bytes)}
	if privateKey.isZero() {
		return nil, ErrZeroPrivateKey
	}
	return privateKey, nil
}

// PrivateKeyFromBytes creates a BLS private key from bytes.
func NewPrivateKeyFromBytes(privKey []byte) (*PrivateKey, error) {
	if len(privKey) != privateKeyLength {
		return nil, fmt.Errorf("bls(private): invalid key length. should be %d", privateKeyLength)
	}
	key := new(blst.SecretKey).Deserialize(privKey)
	if key == nil {
		return nil, ErrDeserializePrivateKey
	}
	privateKey := &PrivateKey{key: key}
	if privateKey.isZero() {
		return nil, ErrZeroPrivateKey
	}
	return privateKey, nil
}

// PublicKey retrieve public key from the private key.
func (p *PrivateKey) PublicKey() PublicKey {
	return new(blst.P1Affine).From(p.key)
}

// Sign a message with BLS.
func (p *PrivateKey) Sign(msg []byte) *Signature {
	signature := new(blst.P2Affine).Sign(p.key, msg, eth2Curve)
	return &Signature{affine: signature}
}

func (p *PrivateKey) Bytes() []byte {
	return p.key.Serialize()
}

// IsZero checks if the secret key is a zero key.
func (p *PrivateKey) isZero() bool {
	b := byte(0)
	for _, s := range p.Bytes() {
		b |= s
	}
	return subtle.ConstantTimeByteEq(b, 0) == 1
}
