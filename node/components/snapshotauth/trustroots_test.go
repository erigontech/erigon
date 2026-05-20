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
