// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"testing"
	"time"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/stretchr/testify/require"
)

func TestEOASignerSignAndVerify(t *testing.T) {
	// Generate a test key
	key, err := crypto.GenerateKey()
	require.NoError(t, err)

	addr := crypto.PubkeyToAddress(key.PublicKey)
	did := NewDIDPKH(1, addr)

	signer := NewEOASigner()

	// Sign a payload
	payload := []byte("test UCAN payload data")
	sig, err := signer.Sign(payload, key)
	require.NoError(t, err)
	require.Len(t, sig, 65)

	// Verify succeeds with correct DID
	valid, err := signer.Verify(context.Background(), payload, sig, did.String())
	require.NoError(t, err)
	require.True(t, valid)

	// Verify fails with wrong DID
	wrongDID := NewDIDPKH(1, crypto.PubkeyToAddress(mustGenerateKey(t).PublicKey))
	valid, err = signer.Verify(context.Background(), payload, sig, wrongDID.String())
	require.NoError(t, err)
	require.False(t, valid)

	// Verify fails with tampered payload
	valid, err = signer.Verify(context.Background(), []byte("tampered"), sig, did.String())
	require.NoError(t, err)
	require.False(t, valid)

	// Verify fails with tampered signature
	badSig := make([]byte, 65)
	copy(badSig, sig)
	badSig[10] ^= 0xff
	valid, err = signer.Verify(context.Background(), payload, badSig, did.String())
	// May return error (recovery fails) or false (wrong address)
	if err == nil {
		require.False(t, valid)
	}
}

func TestEOASignerCanVerify(t *testing.T) {
	signer := NewEOASigner()

	require.True(t, signer.CanVerify("did:pkh:eip155:1:0xab5801a7d398351b8be11c439e05c5b3259aec9b"))
	require.False(t, signer.CanVerify("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"))
	require.False(t, signer.CanVerify("not-a-did"))
}

func TestEOASignerInvalidSignatureLength(t *testing.T) {
	signer := NewEOASigner()
	did := "did:pkh:eip155:1:0xab5801a7d398351b8be11c439e05c5b3259aec9b"

	_, err := signer.Verify(context.Background(), []byte("payload"), []byte{0x01, 0x02}, did)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid signature length")
}

// TestFullDelegationChainWithRealSignatures tests a complete Alice→Bob→Carol
// delegation chain with real ECDSA signatures.
func TestFullDelegationChainWithRealSignatures(t *testing.T) {
	// Generate keys for three parties
	aliceKey := mustGenerateKey(t)
	bobKey := mustGenerateKey(t)
	carolKey := mustGenerateKey(t)

	aliceDID := NewDIDPKH(1, crypto.PubkeyToAddress(aliceKey.PublicKey))
	bobDID := NewDIDPKH(1, crypto.PubkeyToAddress(bobKey.PublicKey))
	carolDID := NewDIDPKH(1, crypto.PubkeyToAddress(carolKey.PublicKey))

	signer := NewEOASigner()
	store := NewMemoryStore()
	ctx := context.Background()

	nonce := make([]byte, 16)
	rand.Read(nonce)

	// Alice → Bob: full storage access
	aliceToBob := &Token{
		Issuer:   aliceDID.String(),
		Audience: bobDID.String(),
		Command:  "/storage/*",
		Nonce:    nonce,
		Iat:      uint64(time.Now().Unix()),
		Exp:      uint64(time.Now().Add(1 * time.Hour).Unix()),
	}
	aliceToBobPayload, err := aliceToBob.PayloadBytes()
	require.NoError(t, err)
	aliceToBob.Signature, err = signer.Sign(aliceToBobPayload, aliceKey)
	require.NoError(t, err)

	aliceToBobCID := aliceToBob.ComputeCID()
	require.NoError(t, store.PutToken(ctx, aliceToBobCID, aliceToBob))

	// Bob → Carol: read-only (attenuated)
	nonce2 := make([]byte, 16)
	rand.Read(nonce2)

	bobToCarol := &Token{
		Issuer:   bobDID.String(),
		Audience: carolDID.String(),
		Command:  "/storage/read",
		Nonce:    nonce2,
		Iat:      uint64(time.Now().Unix()),
		Exp:      uint64(time.Now().Add(1 * time.Hour).Unix()),
		Proofs:   []CID{aliceToBobCID},
	}
	bobToCarolPayload, err := bobToCarol.PayloadBytes()
	require.NoError(t, err)
	bobToCarol.Signature, err = signer.Sign(bobToCarolPayload, bobKey)
	require.NoError(t, err)

	// Verify the chain with real signature verification
	verifier := NewVerifier(NewStoreResolver(store), store, signer)

	// Carol's token authorizes /storage/read through the chain
	err = verifier.Verify(ctx, bobToCarol, Capability{Command: "/storage/read"})
	require.NoError(t, err)

	// Carol's token does NOT authorize /storage/write
	err = verifier.Verify(ctx, bobToCarol, Capability{Command: "/storage/write"})
	require.Error(t, err)

	// Tamper with Alice's signature → chain breaks
	aliceToBob.Signature[10] ^= 0xff
	require.NoError(t, store.PutToken(ctx, aliceToBobCID, aliceToBob))
	err = verifier.Verify(ctx, bobToCarol, Capability{Command: "/storage/read"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "signature")
}

func mustGenerateKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	return key
}
