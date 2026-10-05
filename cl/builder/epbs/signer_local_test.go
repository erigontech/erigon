package epbs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestLocalSignerLoadsKeyBytes(t *testing.T) {
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privateKey.Bytes())
	require.NoError(t, err)
	require.Equal(t, common.Bytes48(bls.CompressPublicKey(privateKey.PublicKey())), signer.Pubkey())
}

func TestLocalSignerLoadsKeyFile(t *testing.T) {
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)

	keyPath := filepath.Join(t.TempDir(), "builder.key")
	require.NoError(t, os.WriteFile(keyPath, privateKey.Bytes(), 0o600))

	signer, err := NewLocalSignerFromFile(keyPath)
	require.NoError(t, err)
	require.Equal(t, common.Bytes48(bls.CompressPublicKey(privateKey.PublicKey())), signer.Pubkey())
}

func TestLocalSignerRejectsInvalidKey(t *testing.T) {
	_, err := NewLocalSignerFromBytes([]byte{1, 2, 3})
	require.ErrorContains(t, err, "parse private key")
}

func TestLocalSignerReportsMissingKeyFile(t *testing.T) {
	_, err := NewLocalSignerFromFile(filepath.Join(t.TempDir(), "missing.key"))
	require.ErrorContains(t, err, "read key file")
}

func TestLocalSignerSignsBuilderMessages(t *testing.T) {
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	signer, err := NewLocalSignerFromBytes(privateKey.Bytes())
	require.NoError(t, err)

	message := common.HexToHash("0xdeadbeef")
	for name, sign := range map[string]func(context.Context, common.Hash) (common.Bytes96, error){
		"bid":      signer.SignBid,
		"envelope": signer.SignEnvelope,
	} {
		t.Run(name, func(t *testing.T) {
			signature, err := sign(t.Context(), message)
			require.NoError(t, err)
			publicKey := signer.Pubkey()
			valid, err := bls.Verify(signature[:], message[:], publicKey[:])
			require.NoError(t, err)
			require.True(t, valid)
		})
	}
}
