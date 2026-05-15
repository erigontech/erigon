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

package snapshotsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
)

// TestSignAdvertisement_RoundTrip pins the happy path: sign with a
// key, verify with the matching compressed public key, accept.
func TestSignAdvertisement_RoundTrip(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateKey()
	require.NoError(t, err)
	pubCompressed := crypto.CompressPubkey(&priv.PublicKey)
	pubUncompressed := crypto.MarshalPubkey(&priv.PublicKey)

	data := []byte("'v1.1-000000-001000-headers.seg' = 'aaa'\n'v1.1-000000-001000-bodies.seg' = 'bbb'\n")

	sig, err := SignAdvertisement(data, priv)
	require.NoError(t, err)
	require.Len(t, sig, AdvertisementSignatureSize,
		"signature is 64 bytes (R || S, no recovery byte)")

	require.NoError(t, VerifyAdvertisement(data, sig, pubCompressed),
		"verify with compressed (33 byte) pubkey")

	// The 64-byte uncompressed form from MarshalPubkey is X||Y only
	// (no 0x04 prefix). crypto.VerifySignature accepts 33 or 65 byte
	// inputs, where the 65-byte form has the 0x04 prefix. Construct
	// it explicitly.
	pubFullUncompressed := append([]byte{0x04}, pubUncompressed...)
	require.NoError(t, VerifyAdvertisement(data, sig, pubFullUncompressed),
		"verify with uncompressed (65 byte) pubkey")
}

// TestVerifyAdvertisement_TamperedContent pins the MITM-defence
// invariant: if a single byte of the data has been modified
// post-signing, verification fails. This is the core of why we
// require signatures — a redistributor cannot modify the file
// without invalidating it.
func TestVerifyAdvertisement_TamperedContent(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateKey()
	require.NoError(t, err)
	pub := crypto.CompressPubkey(&priv.PublicKey)

	original := []byte("'v1.1-000000-001000-headers.seg' = 'aaa'\n")
	sig, err := SignAdvertisement(original, priv)
	require.NoError(t, err)

	// Tamper: change one character in the hash field.
	tampered := []byte("'v1.1-000000-001000-headers.seg' = 'aab'\n")
	require.ErrorIs(t, VerifyAdvertisement(tampered, sig, pub),
		ErrAdvertisementSignatureInvalid,
		"single-byte modification breaks signature — wholesale reject")
}

// TestVerifyAdvertisement_WrongKey pins that signing with key A and
// verifying with key B's public key fails. This prevents a peer
// from re-signing another peer's manifest under their own ENR.
func TestVerifyAdvertisement_WrongKey(t *testing.T) {
	t.Parallel()

	privA, err := crypto.GenerateKey()
	require.NoError(t, err)
	privB, err := crypto.GenerateKey()
	require.NoError(t, err)
	pubB := crypto.CompressPubkey(&privB.PublicKey)

	data := []byte("'v1.1-000000-001000-headers.seg' = 'aaa'\n")
	sig, err := SignAdvertisement(data, privA)
	require.NoError(t, err)

	require.ErrorIs(t, VerifyAdvertisement(data, sig, pubB),
		ErrAdvertisementSignatureInvalid,
		"signature from key A does not verify against key B's public key")
}

// TestVerifyAdvertisement_WrongSize pins defensive length checks:
// truncated or padded signatures / pubkeys must be rejected without
// reaching the crypto library (which would otherwise return false
// without explanation, masking the real cause).
func TestVerifyAdvertisement_WrongSize(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateKey()
	require.NoError(t, err)
	pub := crypto.CompressPubkey(&priv.PublicKey)

	data := []byte("'v1.1-000000-001000-headers.seg' = 'aaa'\n")
	sig, err := SignAdvertisement(data, priv)
	require.NoError(t, err)

	require.ErrorIs(t, VerifyAdvertisement(data, sig[:30], pub),
		ErrAdvertisementSignatureInvalid,
		"short signature rejected")
	require.ErrorIs(t, VerifyAdvertisement(data, append(sig, 0x00), pub),
		ErrAdvertisementSignatureInvalid,
		"long signature rejected (would otherwise pass a recovery-byte form)")
	require.ErrorIs(t, VerifyAdvertisement(data, sig, pub[:20]),
		ErrAdvertisementSignatureInvalid,
		"truncated pubkey rejected")
	require.ErrorIs(t, VerifyAdvertisement(data, sig, append(pub, 0x00, 0x00)),
		ErrAdvertisementSignatureInvalid,
		"oversized pubkey rejected")
}

// TestSignAdvertisement_NilKey pins error handling for nil private key.
func TestSignAdvertisement_NilKey(t *testing.T) {
	t.Parallel()

	_, err := SignAdvertisement([]byte("data"), nil)
	require.Error(t, err, "nil private key returns error, not panic")
}

// TestVerifyAdvertisement_EmptyData pins that empty data is signable
// and verifiable. The empty manifest is a valid edge case (e.g., a
// fresh node with no inventory yet) and must not error.
func TestVerifyAdvertisement_EmptyData(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateKey()
	require.NoError(t, err)
	pub := crypto.CompressPubkey(&priv.PublicKey)

	sig, err := SignAdvertisement([]byte{}, priv)
	require.NoError(t, err)

	require.NoError(t, VerifyAdvertisement([]byte{}, sig, pub),
		"empty data sign+verify must work — fresh nodes with empty inventory are valid")
}

// TestSignAdvertisement_IdempotentVerify pins that a signature can be
// verified arbitrarily many times — verification is pure / no state
// mutation. This is implicit in any sensible verifier but worth
// pinning since the consumer caches signatures and may re-check
// them on access.
func TestSignAdvertisement_IdempotentVerify(t *testing.T) {
	t.Parallel()

	priv, err := crypto.GenerateKey()
	require.NoError(t, err)
	pub := crypto.CompressPubkey(&priv.PublicKey)

	data := []byte("payload")
	sig, err := SignAdvertisement(data, priv)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		require.NoError(t, VerifyAdvertisement(data, sig, pub),
			"verify %d/5 — repeated verification of the same signature must always succeed", i+1)
	}
}
