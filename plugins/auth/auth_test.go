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
	"crypto/rand"
	"testing"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

// --- DID Tests ---

func TestParseDIDPKH(t *testing.T) {
	did, err := ParseDID("did:pkh:eip155:1:0xab5801a7d398351b8be11c439e05c5b3259aec9b")
	require.NoError(t, err)
	require.Equal(t, "pkh", did.Method)
	require.Equal(t, uint64(1), did.ChainID())
	require.Equal(t, common.HexToAddress("0xab5801a7d398351b8be11c439e05c5b3259aec9b"), did.Address())
	require.True(t, did.IsPKH())
	require.False(t, did.IsKey())
}

func TestParseDIDKey(t *testing.T) {
	did, err := ParseDID("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK")
	require.NoError(t, err)
	require.Equal(t, "key", did.Method)
	require.True(t, did.IsKey())
	require.False(t, did.IsPKH())
}

func TestParseDIDInvalid(t *testing.T) {
	tests := []string{
		"",
		"not-a-did",
		"did:",
		"did:pkh",
		"did:pkh:eip155",
		"did:pkh:eip155:1",
		"did:pkh:eip155:1:not-an-address",
		"did:pkh:cosmos:1:addr",
		"did:unknown:foo",
	}
	for _, s := range tests {
		_, err := ParseDID(s)
		require.Error(t, err, "expected error for: %q", s)
	}
}

func TestNewDIDPKH(t *testing.T) {
	addr := common.HexToAddress("0xab5801a7d398351b8be11c439e05c5b3259aec9b")
	did := NewDIDPKH(1, addr)
	require.Equal(t, "did:pkh:eip155:1:0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B", did.String())
	require.Equal(t, addr, did.Address())
}

// --- Capability Tests ---

func TestCommandCovers(t *testing.T) {
	tests := []struct {
		granting string
		required string
		covers   bool
	}{
		{"/storage/read", "/storage/read", true},
		{"/storage/*", "/storage/read", true},
		{"/storage/*", "/storage/write", true},
		{"/*", "/anything", true},
		{"/storage/read", "/storage/write", false},
		{"/storage", "/storage/read", false}, // no wildcard = no cover
		{"/id/*", "/storage/read", false},
	}
	for _, tt := range tests {
		result := commandCovers(tt.granting, tt.required)
		require.Equal(t, tt.covers, result, "commandCovers(%q, %q)", tt.granting, tt.required)
	}
}

func TestCapabilityAttenuates(t *testing.T) {
	parent := Capability{Command: "/storage/*"}
	child := Capability{Command: "/storage/read"}
	wider := Capability{Command: "/compute/*"}

	require.True(t, child.Attenuates(parent))
	require.False(t, wider.Attenuates(parent))
	require.False(t, parent.Attenuates(child)) // can't widen
}

// --- Token Tests ---

func TestTokenStructureValidation(t *testing.T) {
	nonce := make([]byte, 16)
	rand.Read(nonce)

	valid := &Token{
		Issuer:    "did:pkh:eip155:1:0xab5801a7d398351b8be11c439e05c5b3259aec9b",
		Audience:  "did:pkh:eip155:1:0x1234567890abcdef1234567890abcdef12345678",
		Command:   "/storage/read",
		Nonce:     nonce,
		Iat:       uint64(time.Now().Unix()),
		Signature: []byte{0x01}, // placeholder
	}
	require.NoError(t, valid.ValidateStructure())

	// Missing issuer
	noIss := *valid
	noIss.Issuer = ""
	require.Error(t, noIss.ValidateStructure())

	// Missing command
	noCmd := *valid
	noCmd.Command = ""
	require.Error(t, noCmd.ValidateStructure())

	// Invalid DID
	badDID := *valid
	badDID.Issuer = "not-a-did"
	require.Error(t, badDID.ValidateStructure())
}

func TestTokenExpiry(t *testing.T) {
	token := &Token{Exp: uint64(time.Now().Add(-1 * time.Hour).Unix())}
	require.True(t, token.IsExpired())
	require.False(t, token.IsActive())

	token.Exp = uint64(time.Now().Add(1 * time.Hour).Unix())
	require.False(t, token.IsExpired())
	require.True(t, token.IsActive())

	token.Exp = 0 // no expiry
	require.False(t, token.IsExpired())
	require.True(t, token.IsActive())
}

func TestTokenCID(t *testing.T) {
	nonce := make([]byte, 16)
	rand.Read(nonce)

	t1 := &Token{
		Issuer:   "did:pkh:eip155:1:0xab5801a7d398351b8be11c439e05c5b3259aec9b",
		Audience: "did:pkh:eip155:1:0x1234567890abcdef1234567890abcdef12345678",
		Command:  "/storage/read",
		Nonce:    nonce,
		Iat:      uint64(time.Now().Unix()),
	}
	t2 := &Token{
		Issuer:   "did:pkh:eip155:1:0xab5801a7d398351b8be11c439e05c5b3259aec9b",
		Audience: "did:pkh:eip155:1:0x1234567890abcdef1234567890abcdef12345678",
		Command:  "/storage/read",
		Nonce:    nonce,
		Iat:      uint64(time.Now().Unix()),
	}

	// Same content → same CID
	require.Equal(t, t1.ComputeCID(), t2.ComputeCID())

	// Different content → different CID
	t2.Command = "/storage/write"
	require.NotEqual(t, t1.ComputeCID(), t2.ComputeCID())
}

// --- Verifier Tests ---

// mockSigner always validates signatures.
type mockSigner struct{}

func (m *mockSigner) CanVerify(did string) bool { return true }
func (m *mockSigner) Verify(_ context.Context, _ []byte, _ []byte, _ string) (bool, error) {
	return true, nil
}

func TestVerifyRootToken(t *testing.T) {
	nonce := make([]byte, 16)
	rand.Read(nonce)

	token := &Token{
		Issuer:    "did:pkh:eip155:1:0xab5801a7d398351b8be11c439e05c5b3259aec9b",
		Audience:  "did:pkh:eip155:1:0x1234567890abcdef1234567890abcdef12345678",
		Command:   "/storage/*",
		Nonce:     nonce,
		Iat:       uint64(time.Now().Unix()),
		Exp:       uint64(time.Now().Add(1 * time.Hour).Unix()),
		Signature: []byte{0x01},
	}

	store := NewMemoryStore()
	verifier := NewVerifier(NewStoreResolver(store), store, &mockSigner{})

	// Root token with wildcard covers /storage/read
	err := verifier.Verify(context.Background(), token, Capability{Command: "/storage/read"})
	require.NoError(t, err)

	// Root token does NOT cover /compute/run
	err = verifier.Verify(context.Background(), token, Capability{Command: "/compute/run"})
	require.Error(t, err)
}

func TestVerifyDelegationChain(t *testing.T) {
	nonce := make([]byte, 16)
	rand.Read(nonce)

	alice := "did:pkh:eip155:1:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bob := "did:pkh:eip155:1:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	carol := "did:pkh:eip155:1:0xcccccccccccccccccccccccccccccccccccccccc"

	// Alice → Bob: full storage access
	aliceToBob := &Token{
		Issuer:    alice,
		Audience:  bob,
		Command:   "/storage/*",
		Nonce:     nonce,
		Iat:       uint64(time.Now().Unix()),
		Exp:       uint64(time.Now().Add(1 * time.Hour).Unix()),
		Signature: []byte{0x01},
	}
	aliceToBobCID := aliceToBob.ComputeCID()

	// Bob → Carol: read-only (attenuated)
	bobToCarol := &Token{
		Issuer:    bob,
		Audience:  carol,
		Command:   "/storage/read",
		Nonce:     nonce,
		Iat:       uint64(time.Now().Unix()),
		Exp:       uint64(time.Now().Add(1 * time.Hour).Unix()),
		Proofs:    []CID{aliceToBobCID},
		Signature: []byte{0x02},
	}

	store := NewMemoryStore()
	store.PutToken(context.Background(), aliceToBobCID, aliceToBob)

	verifier := NewVerifier(NewStoreResolver(store), store, &mockSigner{})

	// Carol's token authorizes /storage/read
	err := verifier.Verify(context.Background(), bobToCarol, Capability{Command: "/storage/read"})
	require.NoError(t, err)

	// Carol's token does NOT authorize /storage/write
	err = verifier.Verify(context.Background(), bobToCarol, Capability{Command: "/storage/write"})
	require.Error(t, err)
}

func TestVerifyBrokenChain(t *testing.T) {
	nonce := make([]byte, 16)
	rand.Read(nonce)

	// Token references a proof that doesn't exist
	token := &Token{
		Issuer:    "did:pkh:eip155:1:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Audience:  "did:pkh:eip155:1:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Command:   "/storage/read",
		Nonce:     nonce,
		Iat:       uint64(time.Now().Unix()),
		Exp:       uint64(time.Now().Add(1 * time.Hour).Unix()),
		Proofs:    []CID{common.Hash{0xff}}, // non-existent
		Signature: []byte{0x01},
	}

	store := NewMemoryStore()
	verifier := NewVerifier(NewStoreResolver(store), store, &mockSigner{})

	err := verifier.Verify(context.Background(), token, Capability{Command: "/storage/read"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "proof not found")
}

func TestVerifyExpiredToken(t *testing.T) {
	nonce := make([]byte, 16)
	rand.Read(nonce)

	token := &Token{
		Issuer:    "did:pkh:eip155:1:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Audience:  "did:pkh:eip155:1:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Command:   "/storage/read",
		Nonce:     nonce,
		Iat:       uint64(time.Now().Unix()),
		Exp:       uint64(time.Now().Add(-1 * time.Hour).Unix()), // expired
		Signature: []byte{0x01},
	}

	store := NewMemoryStore()
	verifier := NewVerifier(NewStoreResolver(store), store, &mockSigner{})

	err := verifier.Verify(context.Background(), token, Capability{Command: "/storage/read"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expired")
}

// --- Store Tests ---

func TestMemoryStoreRevocation(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	cid := common.Hash{0x01}
	issuer := "did:pkh:eip155:1:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	// Not revoked initially
	revoked, err := store.IsRevoked(ctx, cid, issuer, 1000)
	require.NoError(t, err)
	require.False(t, revoked)

	// Revoke specific token
	require.NoError(t, store.Revoke(ctx, cid))
	revoked, err = store.IsRevoked(ctx, cid, issuer, 1000)
	require.NoError(t, err)
	require.True(t, revoked)

	// Bulk revocation
	cid2 := common.Hash{0x02}
	require.NoError(t, store.RevokeAllBefore(ctx, issuer, 2000))
	revoked, err = store.IsRevoked(ctx, cid2, issuer, 1500)
	require.NoError(t, err)
	require.True(t, revoked) // issued before bulk revocation timestamp

	revoked, err = store.IsRevoked(ctx, cid2, issuer, 2500)
	require.NoError(t, err)
	require.False(t, revoked) // issued after bulk revocation timestamp
}
