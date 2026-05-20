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
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMintContentUCAN(t *testing.T) {
	t.Parallel()
	operatorKey := newKey(t)

	// An Authority UCAN for the operator to parent the Content UCAN to.
	authority, err := New(
		&operatorKey.PublicKey, &operatorKey.PublicKey,
		[]string{string(CapAdvertise), string(CapServe), string(CapDelegate)},
		time.Time{}, time.Time{}, BootstrapDepthCap, nil,
	)
	require.NoError(t, err)
	require.NoError(t, authority.Sign(operatorKey))
	authorityEncoded, err := authority.Encode()
	require.NoError(t, err)

	tomlBytes := []byte("version = 2\n[blocks]\n")
	now := time.Unix(1_700_000_000, 0)

	encoded, err := MintContentUCAN(tomlBytes, operatorKey, authorityEncoded, now)
	require.NoError(t, err)

	d, err := Decode(encoded)
	require.NoError(t, err)
	require.NoError(t, d.VerifySignature(), "Content UCAN must verify against its issuer")

	// Self-issued: issuer == audience == operator pubkey.
	require.Equal(t, d.Issuer, d.Audience, "Content UCAN is self-audience")

	// The single capability binds the sha256 of the manifest bytes.
	sum := sha256.Sum256(tomlBytes)
	require.Equal(t,
		[]string{ContentHashCapability(hex.EncodeToString(sum[:]))},
		d.Capabilities)

	// ParentHash references the Authority UCAN by hash.
	require.Equal(t, HashOf(authorityEncoded), d.ParentHash)

	// Leaf (not re-delegable), time-bounded to ContentUCANValidity.
	require.Equal(t, uint16(0), d.DepthCap)
	require.Equal(t, now.Unix(), d.NotBefore)
	require.Equal(t, now.Add(ContentUCANValidity).Unix(), d.Expires)
}

// TestMintContentUCAN_HashBindsToContent confirms the capability
// changes when the manifest bytes change — the attestation is bound to
// exact content, so a tampered manifest no longer matches.
func TestMintContentUCAN_HashBindsToContent(t *testing.T) {
	t.Parallel()
	operatorKey := newKey(t)
	authority, err := New(
		&operatorKey.PublicKey, &operatorKey.PublicKey,
		[]string{string(CapAdvertise)},
		time.Time{}, time.Time{}, BootstrapDepthCap, nil,
	)
	require.NoError(t, err)
	require.NoError(t, authority.Sign(operatorKey))
	authorityEncoded, err := authority.Encode()
	require.NoError(t, err)

	now := time.Unix(1_700_000_000, 0)
	a, err := MintContentUCAN([]byte("manifest-A"), operatorKey, authorityEncoded, now)
	require.NoError(t, err)
	b, err := MintContentUCAN([]byte("manifest-B"), operatorKey, authorityEncoded, now)
	require.NoError(t, err)

	da, err := Decode(a)
	require.NoError(t, err)
	db, err := Decode(b)
	require.NoError(t, err)
	require.NotEqual(t, da.Capabilities, db.Capabilities,
		"distinct manifest bytes must produce distinct chain.v2:hash capabilities")
}

func TestMintContentUCAN_NilKey(t *testing.T) {
	t.Parallel()
	_, err := MintContentUCAN([]byte("x"), nil, nil, time.Now())
	require.Error(t, err)
}
