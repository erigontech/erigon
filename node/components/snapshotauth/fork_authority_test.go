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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestForkedFromCapability_BuildsCanonicalString(t *testing.T) {
	// A 33-byte compressed-pubkey-like input produces the expected
	// fork:from:<66-hex-chars> form.
	pk := make([]byte, PubKeyLen)
	for i := range pk {
		pk[i] = byte(i + 1)
	}
	cap, err := ForkedFromCapability(pk)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(cap, CapForkedFromPrefix), "must carry the fork:from: prefix")
	require.Equal(t, len(CapForkedFromPrefix)+2*PubKeyLen, len(cap),
		"prefix + 66 hex chars = total")
}

func TestForkedFromCapability_RejectsWrongLengthPubkey(t *testing.T) {
	_, err := ForkedFromCapability(make([]byte, 16))
	require.Error(t, err)
	require.Contains(t, err.Error(), "pubkey length 16")

	_, err = ForkedFromCapability(nil)
	require.Error(t, err)
}

func TestParseForkedFromCapability_RoundTrip(t *testing.T) {
	pk := make([]byte, PubKeyLen)
	for i := range pk {
		pk[i] = byte(0xa0 + i)
	}
	cap, err := ForkedFromCapability(pk)
	require.NoError(t, err)

	decoded, ok := ParseForkedFromCapability(cap)
	require.True(t, ok)
	require.Equal(t, pk, decoded)
}

func TestParseForkedFromCapability_RejectsNonMatchingCapabilities(t *testing.T) {
	cases := []string{
		string(CapAdvertise),
		string(CapServe),
		"chain.v2:hash:deadbeef",
		"random:garbage",
		"",
	}
	for _, c := range cases {
		_, ok := ParseForkedFromCapability(c)
		require.False(t, ok, "%q should not parse as forked-from", c)
	}
}

func TestParseForkedFromCapability_RejectsMalformedHex(t *testing.T) {
	_, ok := ParseForkedFromCapability(CapForkedFromPrefix + "not-hex!!")
	require.False(t, ok)

	// Wrong length (32 hex chars = 16 bytes, not the 33-byte target)
	_, ok = ParseForkedFromCapability(CapForkedFromPrefix + "0102030405060708090a0b0c0d0e0f10")
	require.False(t, ok)
}

func TestMintForkAuthorityUCAN_EmbedsForkedFromCapability(t *testing.T) {
	trustRoot := newKey(t)
	operator := newKey(t)
	parentTrustRootPub := compressed(t, newKey(t))

	now := time.Now()
	encoded, err := MintForkAuthorityUCAN(
		trustRoot, &operator.PublicKey, parentTrustRootPub,
		now, now.Add(ForkAuthorityUCANValidity),
		nil, // self-rooted (shadow-fork operator's own trust root)
	)
	require.NoError(t, err)

	d, err := Decode(encoded)
	require.NoError(t, err)

	// Standard publish capabilities are present.
	hasCap := func(want string) bool {
		for _, c := range d.Capabilities {
			if c == want {
				return true
			}
		}
		return false
	}
	require.True(t, hasCap(string(CapAdvertise)))
	require.True(t, hasCap(string(CapServe)))
	require.True(t, hasCap(string(CapDelegate)))

	// fork:from:<parent pubkey> is embedded.
	extracted, ok := ExtractForkedFromCapability(d)
	require.True(t, ok, "minted fork authority UCAN must carry a forked-from capability")
	require.Equal(t, parentTrustRootPub, extracted)

	// Signature verifies + Issuer is the trust root.
	require.NoError(t, d.VerifySignature())
	require.Equal(t, compressed(t, trustRoot), d.Issuer)
	require.Equal(t, compressed(t, operator), d.Audience)

	// Sub-delegations are allowed (per-generation Content UCANs are
	// signed by the operator audience under this authority).
	require.Greater(t, int(d.DepthCap), 0,
		"fork authority must allow sub-delegations")
}

func TestMintForkAuthorityUCAN_RejectsBadInputs(t *testing.T) {
	trustRoot := newKey(t)
	operator := newKey(t)
	parentPub := compressed(t, newKey(t))
	now := time.Now()

	_, err := MintForkAuthorityUCAN(nil, &operator.PublicKey, parentPub, now, now.Add(time.Hour), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil trust root key")

	_, err = MintForkAuthorityUCAN(trustRoot, nil, parentPub, now, now.Add(time.Hour), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil operator pubkey")

	_, err = MintForkAuthorityUCAN(trustRoot, &operator.PublicKey, []byte{1, 2, 3}, now, now.Add(time.Hour), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pubkey length 3")
}

func TestExtractForkedFromCapability_NoCapabilityReturnsOkFalse(t *testing.T) {
	// A plain authority UCAN (no fork:from: capability) returns ok=false.
	root := newKey(t)
	operator := newKey(t)
	d, err := New(&root.PublicKey, &operator.PublicKey,
		[]string{string(CapAdvertise), string(CapServe)},
		time.Time{}, time.Time{}, 16, nil)
	require.NoError(t, err)

	_, ok := ExtractForkedFromCapability(d)
	require.False(t, ok, "non-fork authority must not carry forked-from")
}

func TestExtractForkedFromCapability_NilDelegationIsSafe(t *testing.T) {
	_, ok := ExtractForkedFromCapability(nil)
	require.False(t, ok)
}

func TestAcceptSet_EmptySetRejectsEverything(t *testing.T) {
	pk := make([]byte, PubKeyLen)
	require.False(t, AcceptSet(pk, nil))
	require.False(t, AcceptSet(pk, [][]byte{}))
}

func TestAcceptSet_EmptyPubkeyAlwaysRejected(t *testing.T) {
	// Defensive: zero/empty input never passes, even if the accept-set
	// has nil/empty entries.
	require.False(t, AcceptSet(nil, [][]byte{nil}))
	require.False(t, AcceptSet([]byte{}, [][]byte{{}}))
}

func TestAcceptSet_MatchesByByteEquality(t *testing.T) {
	pk1 := make([]byte, PubKeyLen)
	for i := range pk1 {
		pk1[i] = 0x11
	}
	pk2 := make([]byte, PubKeyLen)
	for i := range pk2 {
		pk2[i] = 0x22
	}
	other := make([]byte, PubKeyLen)
	for i := range other {
		other[i] = 0xff
	}

	require.True(t, AcceptSet(pk1, [][]byte{pk1, pk2}))
	require.True(t, AcceptSet(pk2, [][]byte{pk1, pk2}))
	require.False(t, AcceptSet(other, [][]byte{pk1, pk2}))
}

func TestForkAuthority_EndToEnd_LiteVerification(t *testing.T) {
	// End-to-end shape of lite-mode verification:
	//   1. Operator mints a fork authority UCAN against trust-root key,
	//      embedding parent-trust-root pubkey as fork:from:<id>.
	//   2. Fork-follower decodes the UCAN, extracts the forked-from
	//      capability, checks it against an accept-set.
	//   3. Pass when the embedded pubkey is in the accept-set; fail
	//      when it isn't.
	trustRoot := newKey(t)
	operator := newKey(t)
	parent1Pub := compressed(t, newKey(t))
	parent2Pub := compressed(t, newKey(t))
	otherPub := compressed(t, newKey(t))

	now := time.Now()
	encoded, err := MintForkAuthorityUCAN(
		trustRoot, &operator.PublicKey, parent1Pub,
		now, now.Add(ForkAuthorityUCANValidity),
		nil,
	)
	require.NoError(t, err)

	d, err := Decode(encoded)
	require.NoError(t, err)
	extracted, ok := ExtractForkedFromCapability(d)
	require.True(t, ok)

	// Accept-set containing parent1 accepts the fork.
	require.True(t, AcceptSet(extracted, [][]byte{parent1Pub, parent2Pub}))

	// Accept-set without parent1 rejects the fork.
	require.False(t, AcceptSet(extracted, [][]byte{otherPub, parent2Pub}))
}
