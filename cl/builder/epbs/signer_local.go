package epbs

import (
	"context"
	"fmt"
	"os"

	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
)

// LocalSigner signs builder messages with a local BLS private key.
type LocalSigner struct {
	privateKey *bls.PrivateKey
	publicKey  common.Bytes48
}

var _ Signer = (*LocalSigner)(nil)

// NewLocalSignerFromFile loads a builder BLS private key from path.
func NewLocalSignerFromFile(path string) (*LocalSigner, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("epbs/signer: read key file: %w", err)
	}
	return NewLocalSignerFromBytes(data)
}

// NewLocalSignerFromBytes creates a signer from raw BLS private-key bytes.
func NewLocalSignerFromBytes(secretKey []byte) (*LocalSigner, error) {
	privateKey, err := bls.NewPrivateKeyFromBytes(secretKey)
	if err != nil {
		return nil, fmt.Errorf("epbs/signer: parse private key: %w", err)
	}

	var publicKey common.Bytes48
	copy(publicKey[:], bls.CompressPublicKey(privateKey.PublicKey()))
	return &LocalSigner{privateKey: privateKey, publicKey: publicKey}, nil
}

func (s *LocalSigner) Pubkey() common.Bytes48 {
	return s.publicKey
}

func (s *LocalSigner) SignBid(_ context.Context, signingRoot common.Hash) (common.Bytes96, error) {
	return s.sign(signingRoot), nil
}

func (s *LocalSigner) SignEnvelope(_ context.Context, signingRoot common.Hash) (common.Bytes96, error) {
	return s.sign(signingRoot), nil
}

func (s *LocalSigner) sign(signingRoot common.Hash) common.Bytes96 {
	signature := s.privateKey.Sign(signingRoot[:])
	var out common.Bytes96
	copy(out[:], signature.Bytes())
	return out
}
