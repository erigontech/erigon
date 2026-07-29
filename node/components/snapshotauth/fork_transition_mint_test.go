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
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMintForkTransitionUCAN_HappyPath: minted UCAN round-trips via
// Verifier when passed the same trust root that signed it. The check
// pins that Mint produces a self-issued, root-signed leaf carrying
// exactly one fork:transition:<name> capability — the same shape
// verifyForkTransition requires at the RPC edge.
func TestMintForkTransitionUCAN_HappyPath(t *testing.T) {
	root := newKey(t)
	now := time.Now()
	enc, err := MintForkTransitionUCAN(root, "hoodi-fork-42", now, now.Add(time.Hour))
	require.NoError(t, err)

	leaf, err := Decode(enc)
	require.NoError(t, err)

	require.True(t, bytes.Equal(leaf.Issuer, leaf.Audience), "must be self-issued")
	require.Empty(t, leaf.ParentHash, "must be root-signed")
	require.Equal(t, []string{"fork:transition:hoodi-fork-42"}, leaf.Capabilities)
	require.Equal(t, uint16(0), leaf.DepthCap, "no re-delegation permitted")

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, root)}})
	_, err = v.Verify(enc, leaf.Audience,
		[]string{"fork:transition:hoodi-fork-42"}, now, nil)
	require.NoError(t, err)
}

// TestMintForkTransitionUCAN_RejectsInvalidInputs — nil key +
// bad chain name surface immediately. The chain-name check
// composes with ForkTransitionCapability's own validation, so
// whitespace-only names fail here too.
func TestMintForkTransitionUCAN_RejectsInvalidInputs(t *testing.T) {
	root := newKey(t)
	now := time.Now()

	_, err := MintForkTransitionUCAN(nil, "hoodi-fork-42", now, now.Add(time.Hour))
	require.ErrorContains(t, err, "nil trust root key")

	_, err = MintForkTransitionUCAN(root, "", now, now.Add(time.Hour))
	require.ErrorContains(t, err, "empty fork chain name")

	_, err = MintForkTransitionUCAN(root, "   ", now, now.Add(time.Hour))
	require.ErrorContains(t, err, "empty fork chain name")

	_, err = MintForkTransitionUCAN(root, "hoodi fork 42", now, now.Add(time.Hour))
	require.ErrorContains(t, err, "whitespace")
}

// TestMintForkTransitionUCAN_DifferentChainNamesProduceDifferentCaps —
// the whole point of binding transition caps to a fork chain name
// is that a UCAN for one fork can't authorise another. This test
// pins that Mint honours that by producing distinct outputs whose
// capabilities differ.
func TestMintForkTransitionUCAN_DifferentChainNamesProduceDifferentCaps(t *testing.T) {
	root := newKey(t)
	now := time.Now()
	encA, err := MintForkTransitionUCAN(root, "hoodi-fork-A", now, now.Add(time.Hour))
	require.NoError(t, err)
	encB, err := MintForkTransitionUCAN(root, "hoodi-fork-B", now, now.Add(time.Hour))
	require.NoError(t, err)

	leafA, err := Decode(encA)
	require.NoError(t, err)
	leafB, err := Decode(encB)
	require.NoError(t, err)

	require.Equal(t, []string{"fork:transition:hoodi-fork-A"}, leafA.Capabilities)
	require.Equal(t, []string{"fork:transition:hoodi-fork-B"}, leafB.Capabilities)
}
