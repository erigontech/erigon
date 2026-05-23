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

package snapshotauth

import (
	"crypto/ecdsa"
	"encoding/hex"
	"testing"

	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

func compressedPubkeyHex(t *testing.T) (string, []byte) {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	pub := crypto.CompressPubkey(&key.PublicKey)
	return hex.EncodeToString(pub), pub
}

func didKeyFor(pub []byte) string {
	return "did:key:z" + base58.Encode(append([]byte{0xe7, 0x01}, pub...))
}

func enrStringFor(t *testing.T, key *ecdsa.PrivateKey) string {
	t.Helper()
	var r enr.Record
	require.NoError(t, enode.SignV4(&r, key))
	n, err := enode.New(enode.ValidSchemes, &r)
	require.NoError(t, err)
	return n.String()
}

func TestParseTrustRoots_HexPubkey(t *testing.T) {
	hexPub, pub := compressedPubkeyHex(t)

	for _, spec := range []string{hexPub, "0x" + hexPub} {
		roots, err := ParseTrustRoots(spec)
		require.NoError(t, err)
		require.Len(t, roots, 1)
		require.Equal(t, RootENR, roots[0].Kind)
		require.Equal(t, pub, roots[0].Pubkey)
	}
}

func TestParseTrustRoots_DIDKey(t *testing.T) {
	_, pub := compressedPubkeyHex(t)
	did := didKeyFor(pub)

	roots, err := ParseTrustRoots(did)
	require.NoError(t, err)
	require.Len(t, roots, 1)
	require.Equal(t, RootDID, roots[0].Kind)
	require.Equal(t, pub, roots[0].Pubkey)
	require.Equal(t, did, roots[0].DID)
}

func TestParseTrustRoots_ENR(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	roots, err := ParseTrustRoots(enrStringFor(t, key))
	require.NoError(t, err)
	require.Len(t, roots, 1)
	require.Equal(t, RootENR, roots[0].Kind)
	require.Equal(t, crypto.CompressPubkey(&key.PublicKey), roots[0].Pubkey)
}

func TestParseTrustRoots_MixedAndWhitespace(t *testing.T) {
	hexPub, pub := compressedPubkeyHex(t)
	_, pub2 := compressedPubkeyHex(t)

	roots, err := ParseTrustRoots("  " + hexPub + " , ," + didKeyFor(pub2) + "  ")
	require.NoError(t, err)
	require.Len(t, roots, 2)
	require.Equal(t, pub, roots[0].Pubkey)
	require.Equal(t, RootDID, roots[1].Kind)
	require.Equal(t, pub2, roots[1].Pubkey)
}

func TestParseTrustRoots_Empty(t *testing.T) {
	for _, spec := range []string{"", "   ", " , , "} {
		roots, err := ParseTrustRoots(spec)
		require.NoError(t, err)
		require.Nil(t, roots)
	}
}

func TestParseTrustRoots_Errors(t *testing.T) {
	hexPub, _ := compressedPubkeyHex(t)

	for name, spec := range map[string]string{
		"bad hex":         "zzzz",
		"short hex":       "0xdeadbeef",
		"did:web":         "did:web:erigon.network",
		"unsupported did": "did:example:123",
		"garbage element": hexPub + ",not-a-root",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseTrustRoots(spec)
			require.Error(t, err)
		})
	}
}

// TestParseTrustRoots_DIDKeyMalformed pins each rejection branch inside
// parseDIDKeyRoot. The existing TestParseTrustRoots_Errors covers the
// outer dispatcher (did:web, did:example) but not the inner fences:
//   - wrong multibase prefix (q, k, ...) inside did:key
//   - valid prefix but malformed base58 payload
//   - valid base58 but the decoded bytes don't carry the secp256k1
//     multicodec prefix
//   - valid prefix + secp256k1 multicodec but trailing pubkey wrong length
func TestParseTrustRoots_DIDKeyMalformed(t *testing.T) {
	cases := map[string]struct {
		spec    string
		errFrag string
	}{
		"wrong multibase":                 {"did:key:qABCDEF", "base58btc multibase"},
		"bad base58 after z":              {"did:key:z!!!notbase58!!!", "base58 decode"},
		"valid base58, no secp prefix":    {"did:key:zABC", "not a secp256k1 key"},
		"secp prefix, short pubkey":       {didKeyWithCustomPubBytes(t, []byte{0x01, 0x02, 0x03}), "33 bytes"},
		"secp prefix, wrong-length 32 b":  {didKeyWithCustomPubBytes(t, bytesN(32)), "33 bytes"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseTrustRoots(tc.spec)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errFrag)
		})
	}
}

// didKeyWithCustomPubBytes builds a did:key string with the secp256k1
// multicodec prefix followed by the caller's pubkey bytes. Used to
// drive the post-prefix validateCompressedPubkey rejection branches.
func didKeyWithCustomPubBytes(t *testing.T, pub []byte) string {
	t.Helper()
	payload := append([]byte{}, didKeySecp256k1Prefix...)
	payload = append(payload, pub...)
	return "did:key:z" + base58Encode(payload)
}

func bytesN(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i + 1)
	}
	return b
}

// base58Encode is a thin wrapper so the test reads naturally. Uses the
// same btcsuite base58 the production parser uses, so a round-trip
// through the same library is guaranteed.
func base58Encode(b []byte) string { return base58.Encode(b) }
