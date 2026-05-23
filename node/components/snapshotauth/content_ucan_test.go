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

// TestContentUCAN_CapabilityRejectsManifestSubstitution is the load-
// bearing replay-prevention test: a Content UCAN minted for manifest-A
// must not validate against manifest-B's bytes. The capability hex IS
// the binding; a consumer that verifies a peer's V2 manifest must
// confirm the Content UCAN's capability hex equals sha256(manifest).
// This pins that contract so it can't quietly drift.
func TestContentUCAN_CapabilityRejectsManifestSubstitution(t *testing.T) {
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

	manifestA := []byte("version = 2\n[blocks]\nA\n")
	manifestB := []byte("version = 2\n[blocks]\nB\n")
	encoded, err := MintContentUCAN(manifestA, operatorKey, authorityEncoded, time.Unix(1_700_000_000, 0))
	require.NoError(t, err)

	d, err := Decode(encoded)
	require.NoError(t, err)
	require.NoError(t, d.VerifySignature())

	// The decoded Content UCAN's single capability binds sha256(manifestA).
	require.Len(t, d.Capabilities, 1)
	hexHash, ok := ParseContentHashCapability(d.Capabilities[0])
	require.True(t, ok, "capability must be a chain.v2:hash:<hex> form")

	// Consumer-side check: the hex on the capability MUST equal
	// sha256(received-manifest). Presenting manifest-B fails the check.
	sumA := sha256.Sum256(manifestA)
	sumB := sha256.Sum256(manifestB)
	require.Equal(t, hex.EncodeToString(sumA[:]), hexHash,
		"capability hex matches the manifest the UCAN was minted for")
	require.NotEqual(t, hex.EncodeToString(sumB[:]), hexHash,
		"capability hex MUST NOT match a different manifest — replay defense")
}

// TestContentUCAN_ParentHashBindsAuthority pins the parent-link
// contract: the Content UCAN's ParentHash MUST equal sha256 of the
// Authority UCAN's canonical CBOR. A consumer's parent-resolver lookup
// uses ParentHash as the key; if the binding drifts, the consumer
// either cannot resolve (rejected) or could be steered to a different
// Authority UCAN by a substitution attack.
func TestContentUCAN_ParentHashBindsAuthority(t *testing.T) {
	t.Parallel()
	operatorKey := newKey(t)

	// Two different Authority UCANs for the same operator (different
	// timestamps → different bytes → different hashes).
	authority1, err := New(
		&operatorKey.PublicKey, &operatorKey.PublicKey,
		[]string{string(CapAdvertise)},
		time.Unix(1_700_000_000, 0), time.Unix(1_800_000_000, 0), BootstrapDepthCap, nil,
	)
	require.NoError(t, err)
	require.NoError(t, authority1.Sign(operatorKey))
	authority1Encoded, err := authority1.Encode()
	require.NoError(t, err)

	authority2, err := New(
		&operatorKey.PublicKey, &operatorKey.PublicKey,
		[]string{string(CapAdvertise)},
		time.Unix(1_700_000_001, 0), time.Unix(1_800_000_001, 0), BootstrapDepthCap, nil,
	)
	require.NoError(t, err)
	require.NoError(t, authority2.Sign(operatorKey))
	authority2Encoded, err := authority2.Encode()
	require.NoError(t, err)

	require.NotEqual(t, HashOf(authority1Encoded), HashOf(authority2Encoded),
		"distinct Authority UCAN bytes must yield distinct hashes")

	contentEnc, err := MintContentUCAN([]byte("manifest"), operatorKey,
		authority1Encoded, time.Unix(1_700_000_500, 0))
	require.NoError(t, err)
	d, err := Decode(contentEnc)
	require.NoError(t, err)

	require.Equal(t, HashOf(authority1Encoded), d.ParentHash,
		"Content UCAN's ParentHash MUST equal sha256 of the Authority CBOR it was minted against")
	require.NotEqual(t, HashOf(authority2Encoded), d.ParentHash,
		"a different Authority UCAN MUST NOT match — defense against parent substitution")
}
