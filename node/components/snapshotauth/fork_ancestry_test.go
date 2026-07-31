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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mintRootChainUCAN produces a root authority UCAN — no forked-from,
// self-rooted issuer.
func mintRootChainUCAN(t *testing.T, trustRoot *ecdsa.PrivateKey) []byte {
	t.Helper()
	operator := newKey(t)
	now := time.Now()
	d, err := New(&trustRoot.PublicKey, &operator.PublicKey,
		[]string{string(CapAdvertise), string(CapServe), string(CapDelegate)},
		now, now.Add(24*time.Hour), 16, nil)
	require.NoError(t, err)
	require.NoError(t, d.Sign(trustRoot))
	encoded, err := d.Encode()
	require.NoError(t, err)
	return encoded
}

// mintForkChainUCAN produces a fork authority UCAN — carries a
// forked-from:<parentTrustRootPub> capability.
func mintForkChainUCAN(t *testing.T, trustRoot *ecdsa.PrivateKey, parentTrustRootPub []byte) []byte {
	t.Helper()
	operator := newKey(t)
	now := time.Now()
	encoded, err := MintForkAuthorityUCAN(trustRoot, &operator.PublicKey, parentTrustRootPub, now, now.Add(24*time.Hour), nil)
	require.NoError(t, err)
	return encoded
}

// mapResolver is a static test resolver backed by a pubkey→UCAN map.
type mapResolver map[string][]byte

func (m mapResolver) AuthorityUCANForTrustRoot(pubkey []byte) ([]byte, error) {
	return m[string(pubkey)], nil
}

func TestWalkForkAncestry_RootChainReturnsSingleElementChain(t *testing.T) {
	root := newKey(t)
	rootPub := compressed(t, root)
	ucan := mintRootChainUCAN(t, root)

	chain, err := WalkForkAncestry(ucan, mapResolver{}, 0)
	require.NoError(t, err)
	require.Len(t, chain, 1)
	require.Equal(t, rootPub, chain[0])
}

func TestWalkForkAncestry_Depth1ReturnsForkThenParent(t *testing.T) {
	mainnet := newKey(t)
	mainnetPub := compressed(t, mainnet)
	mainnetUCAN := mintRootChainUCAN(t, mainnet)

	fork := newKey(t)
	forkPub := compressed(t, fork)
	forkUCAN := mintForkChainUCAN(t, fork, mainnetPub)

	resolver := mapResolver{
		string(mainnetPub): mainnetUCAN,
	}
	chain, err := WalkForkAncestry(forkUCAN, resolver, 0)
	require.NoError(t, err)
	require.Len(t, chain, 2)
	require.Equal(t, forkPub, chain[0], "leaf first")
	require.Equal(t, mainnetPub, chain[1], "root last")
	require.Equal(t, mainnetPub, AcceptSetFromAncestry(chain))
}

func TestWalkForkAncestry_Depth2ReturnsAllThreeLevels(t *testing.T) {
	mainnet := newKey(t)
	mainnetPub := compressed(t, mainnet)
	mainnetUCAN := mintRootChainUCAN(t, mainnet)

	child := newKey(t)
	childPub := compressed(t, child)
	childUCAN := mintForkChainUCAN(t, child, mainnetPub)

	grandchild := newKey(t)
	grandchildPub := compressed(t, grandchild)
	grandchildUCAN := mintForkChainUCAN(t, grandchild, childPub)

	resolver := mapResolver{
		string(mainnetPub): mainnetUCAN,
		string(childPub):   childUCAN,
	}
	chain, err := WalkForkAncestry(grandchildUCAN, resolver, 0)
	require.NoError(t, err)
	require.Len(t, chain, 3)
	require.Equal(t, grandchildPub, chain[0])
	require.Equal(t, childPub, chain[1])
	require.Equal(t, mainnetPub, chain[2])
}

func TestWalkForkAncestry_MissingParentInResolverErrors(t *testing.T) {
	mainnet := newKey(t)
	mainnetPub := compressed(t, mainnet)

	fork := newKey(t)
	forkUCAN := mintForkChainUCAN(t, fork, mainnetPub)

	_, err := WalkForkAncestry(forkUCAN, mapResolver{}, 0)
	require.ErrorIs(t, err, ErrForkAncestryResolverNotFound)
}

func TestWalkForkAncestry_IssuerMismatchErrors(t *testing.T) {
	claimed := newKey(t)
	claimedPub := compressed(t, claimed)

	imposter := newKey(t)
	imposterUCAN := mintRootChainUCAN(t, imposter)

	fork := newKey(t)
	forkUCAN := mintForkChainUCAN(t, fork, claimedPub)

	resolver := mapResolver{
		string(claimedPub): imposterUCAN,
	}
	_, err := WalkForkAncestry(forkUCAN, resolver, 0)
	require.ErrorIs(t, err, ErrForkAncestryIssuerMismatch)
}

func TestWalkForkAncestry_CycleErrors(t *testing.T) {
	forkA := newKey(t)
	forkAPub := compressed(t, forkA)
	forkB := newKey(t)
	forkBPub := compressed(t, forkB)

	ucanA := mintForkChainUCAN(t, forkA, forkBPub) // A's forked-from = B
	ucanB := mintForkChainUCAN(t, forkB, forkAPub) // B's forked-from = A

	resolver := mapResolver{
		string(forkAPub): ucanA,
		string(forkBPub): ucanB,
	}
	_, err := WalkForkAncestry(ucanA, resolver, 0)
	require.ErrorIs(t, err, ErrForkAncestryCycle)
}

func TestWalkForkAncestry_DepthCapEnforced(t *testing.T) {
	const totalLevels = 6
	keys := make([]*ecdsa.PrivateKey, totalLevels)
	pubs := make([][]byte, totalLevels)
	ucans := make([][]byte, totalLevels)
	for i := range totalLevels {
		keys[i] = newKey(t)
		pubs[i] = compressed(t, keys[i])
	}
	ucans[totalLevels-1] = mintRootChainUCAN(t, keys[totalLevels-1])
	for i := totalLevels - 2; i >= 0; i-- {
		ucans[i] = mintForkChainUCAN(t, keys[i], pubs[i+1])
	}
	resolver := mapResolver{}
	for i := 1; i < totalLevels; i++ {
		resolver[string(pubs[i])] = ucans[i]
	}

	_, err := WalkForkAncestry(ucans[0], resolver, 3)
	require.ErrorIs(t, err, ErrForkAncestryDepthExceeded)
}

func TestWalkForkAncestry_ParentWithParentHashRejected(t *testing.T) {
	root := newKey(t)
	rootUCAN := mintRootChainUCAN(t, root)

	child := newKey(t)
	childOperator := newKey(t)
	now := time.Now()
	nested, err := New(&child.PublicKey, &childOperator.PublicKey,
		[]string{string(CapAdvertise)}, now, now.Add(time.Hour), 8, rootUCAN)
	require.NoError(t, err)
	require.NoError(t, nested.Sign(child))
	nestedEncoded, err := nested.Encode()
	require.NoError(t, err)

	fork := newKey(t)
	childPub := compressed(t, child)
	forkUCAN := mintForkChainUCAN(t, fork, childPub)
	resolver := mapResolver{
		string(childPub): nestedEncoded,
	}
	_, err = WalkForkAncestry(forkUCAN, resolver, 0)
	require.ErrorIs(t, err, ErrForkAncestryParentNotRoot)
}

func TestWalkForkAncestry_NilResolverErrors(t *testing.T) {
	root := newKey(t)
	ucan := mintRootChainUCAN(t, root)
	_, err := WalkForkAncestry(ucan, nil, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil resolver")
}

func TestWalkForkAncestry_MalformedLeafReturnsDecodeError(t *testing.T) {
	_, err := WalkForkAncestry([]byte{0xff, 0xfe}, mapResolver{}, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode leaf")
}
