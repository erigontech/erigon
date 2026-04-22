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

func TestLocalSigner_FromBytes(t *testing.T) {
	// Generate a random key.
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)

	// Pubkey should match the generated key's public key.
	expectedPK := bls.CompressPublicKey(privKey.PublicKey())
	require.Equal(t, common.Bytes48(expectedPK), signer.Pubkey())
}

func TestLocalSigner_FromFile(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	// Write key to a temp file.
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "secret.key")
	require.NoError(t, os.WriteFile(keyPath, privKey.Bytes(), 0600))

	signer, err := NewLocalSignerFromFile(keyPath)
	require.NoError(t, err)

	expectedPK := bls.CompressPublicKey(privKey.PublicKey())
	require.Equal(t, common.Bytes48(expectedPK), signer.Pubkey())
}

func TestLocalSigner_FromFile_NotFound(t *testing.T) {
	_, err := NewLocalSignerFromFile("/nonexistent/path/key.bin")
	require.Error(t, err)
	require.Contains(t, err.Error(), "read key file")
}

func TestLocalSigner_FromBytes_Invalid(t *testing.T) {
	_, err := NewLocalSignerFromBytes([]byte{1, 2, 3}) // too short
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse private key")
}

func TestLocalSigner_SignAndVerify(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)

	ctx := context.Background()
	msg := common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")

	// Test all three signing methods produce valid signatures.
	for name, signFn := range map[string]func(context.Context, common.Hash) (common.Bytes96, error){
		"SignBid":      signer.SignBid,
		"SignEnvelope": signer.SignEnvelope,
		"SignDeposit":  signer.SignDeposit,
	} {
		t.Run(name, func(t *testing.T) {
			sig, err := signFn(ctx, msg)
			require.NoError(t, err)

			pk := signer.Pubkey()
			valid, err := bls.Verify(sig[:], msg[:], pk[:])
			require.NoError(t, err)
			require.True(t, valid, "signature should verify")
		})
	}
}

func TestLocalSigner_DifferentMessages(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)

	ctx := context.Background()
	msg1 := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	msg2 := common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")

	sig1, err := signer.SignBid(ctx, msg1)
	require.NoError(t, err)
	sig2, err := signer.SignBid(ctx, msg2)
	require.NoError(t, err)

	// Different messages produce different signatures.
	require.NotEqual(t, sig1, sig2)

	// Each signature only verifies against its own message.
	pk := signer.Pubkey()
	valid, err := bls.Verify(sig1[:], msg1[:], pk[:])
	require.NoError(t, err)
	require.True(t, valid)

	valid, err = bls.Verify(sig1[:], msg2[:], pk[:])
	require.NoError(t, err)
	require.False(t, valid, "sig1 should not verify against msg2")
}
