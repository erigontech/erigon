package epbs

import (
	"context"
	"fmt"
	"os"

	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
)

// LocalSigner implements Signer using a BLS private key loaded from a local file.
type LocalSigner struct {
	privKey *bls.PrivateKey
	pubkey  common.Bytes48
}

var _ Signer = (*LocalSigner)(nil)

// NewLocalSignerFromFile loads a 32-byte BLS secret key from the given path
// and returns a LocalSigner.
func NewLocalSignerFromFile(path string) (*LocalSigner, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("epbs/signer: read key file: %w", err)
	}
	return NewLocalSignerFromBytes(data)
}

// NewLocalSignerFromBytes creates a LocalSigner from raw 32-byte secret key bytes.
func NewLocalSignerFromBytes(secretKey []byte) (*LocalSigner, error) {
	privKey, err := bls.NewPrivateKeyFromBytes(secretKey)
	if err != nil {
		return nil, fmt.Errorf("epbs/signer: parse private key: %w", err)
	}
	pk := privKey.PublicKey()
	compressed := bls.CompressPublicKey(pk)

	var pubkey common.Bytes48
	copy(pubkey[:], compressed)

	return &LocalSigner{
		privKey: privKey,
		pubkey:  pubkey,
	}, nil
}

func (s *LocalSigner) Pubkey() common.Bytes48 {
	return s.pubkey
}

func (s *LocalSigner) SignBid(_ context.Context, signingRoot common.Hash) (common.Bytes96, error) {
	return s.sign(signingRoot)
}

func (s *LocalSigner) SignEnvelope(_ context.Context, signingRoot common.Hash) (common.Bytes96, error) {
	return s.sign(signingRoot)
}

func (s *LocalSigner) SignDeposit(_ context.Context, signingRoot common.Hash) (common.Bytes96, error) {
	return s.sign(signingRoot)
}

func (s *LocalSigner) sign(signingRoot common.Hash) (common.Bytes96, error) {
	sig := s.privKey.Sign(signingRoot[:])
	var out common.Bytes96
	copy(out[:], sig.Bytes())
	return out, nil
}
