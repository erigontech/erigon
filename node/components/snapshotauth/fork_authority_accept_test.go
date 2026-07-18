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

// forkAcceptFixture wires a fork-follower's full-pipeline setup.
type forkAcceptFixture struct {
	forkTrustRoot   *ecdsa.PrivateKey
	operator        *ecdsa.PrivateKey
	parentTrustRoot *ecdsa.PrivateKey
	forkAuthorityUCAN []byte
	parentUCAN      []byte
	acceptor        *ForkAuthorityAccept
	now             time.Time
}

// buildForkAcceptFixture produces a depth-1 fork configured for a
// consumer that accepts the parent's trust root and knows the fork's
// direct trust root. The composed pipeline should pass on the returned
// forkAuthorityUCAN.
func buildForkAcceptFixture(t *testing.T) *forkAcceptFixture {
	t.Helper()
	forkTrustRoot := newKey(t)
	operator := newKey(t)
	parentTrustRoot := newKey(t)
	parentTrustRootPub := compressed(t, parentTrustRoot)

	now := time.Now()
	forkUCAN, err := MintForkAuthorityUCAN(forkTrustRoot, &operator.PublicKey, parentTrustRootPub, now, now.Add(24*time.Hour), nil)
	require.NoError(t, err)
	parentUCAN := mintRootChainUCAN(t, parentTrustRoot)

	resolver := mapResolver{
		string(parentTrustRootPub): parentUCAN,
	}
	verifier := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, forkTrustRoot)}})

	return &forkAcceptFixture{
		forkTrustRoot:     forkTrustRoot,
		operator:          operator,
		parentTrustRoot:   parentTrustRoot,
		forkAuthorityUCAN: forkUCAN,
		parentUCAN:        parentUCAN,
		acceptor: &ForkAuthorityAccept{
			ForkVerifier:   verifier,
			ParentResolver: resolver,
			RootAcceptSet:  [][]byte{parentTrustRootPub},
			RequiredCaps:   []string{string(CapAdvertise), string(CapServe)},
			Audience:       compressed(t, operator),
		},
		now: now,
	}
}

func TestForkAuthorityAccept_HappyPath(t *testing.T) {
	f := buildForkAcceptFixture(t)
	require.NoError(t, f.acceptor.Accept(f.forkAuthorityUCAN, f.now))
}

func TestForkAuthorityAccept_WrongForkTrustRootRejects(t *testing.T) {
	// Verifier's root set doesn't include the ACTUAL signer of the
	// leaf. Verify layer rejects; no cascade walk runs.
	f := buildForkAcceptFixture(t)
	imposter := newKey(t)
	f.acceptor.ForkVerifier = NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, imposter)}})

	err := f.acceptor.Accept(f.forkAuthorityUCAN, f.now)
	require.Error(t, err)
	require.Contains(t, err.Error(), "verify leaf",
		"failure must be at the verifier layer, not the cascade")
}

func TestForkAuthorityAccept_ExpiredLeafRejects(t *testing.T) {
	f := buildForkAcceptFixture(t)
	err := f.acceptor.Accept(f.forkAuthorityUCAN, f.now.Add(48*time.Hour))
	require.Error(t, err)
	require.Contains(t, err.Error(), "verify leaf")
}

func TestForkAuthorityAccept_ParentNotInAcceptSetRejects(t *testing.T) {
	// Cascade walks fine (resolver knows the parent) but the walked
	// terminal isn't in the operator's accept-set. This is the
	// primary E.3 rejection surface — a "wrong-root" fork by
	// parent-chain identity.
	f := buildForkAcceptFixture(t)
	other := compressed(t, newKey(t))
	f.acceptor.RootAcceptSet = [][]byte{other}

	err := f.acceptor.Accept(f.forkAuthorityUCAN, f.now)
	require.ErrorIs(t, err, ErrForkAuthorityTerminalNotInAcceptSet)
}

func TestForkAuthorityAccept_EmptyAcceptSetRejects(t *testing.T) {
	f := buildForkAcceptFixture(t)
	f.acceptor.RootAcceptSet = nil

	err := f.acceptor.Accept(f.forkAuthorityUCAN, f.now)
	require.ErrorIs(t, err, ErrForkAuthorityTerminalNotInAcceptSet)
}

func TestForkAuthorityAccept_MissingParentInResolverRejects(t *testing.T) {
	// Resolver doesn't know the parent → cascade fails at walk step.
	f := buildForkAcceptFixture(t)
	f.acceptor.ParentResolver = mapResolver{}

	err := f.acceptor.Accept(f.forkAuthorityUCAN, f.now)
	require.Error(t, err)
	require.Contains(t, err.Error(), "walk ancestry")
	require.ErrorIs(t, err, ErrForkAncestryResolverNotFound)
}

func TestForkAuthorityAccept_SelfRootedForkPassesWithEmptyAcceptSet(t *testing.T) {
	// A self-rooted UCAN has no forked-from — the ancestry walk
	// terminates at length 1. Since Verifier already confirmed the
	// leaf issuer is in the ForkVerifier's root set, no accept-set
	// check runs. Callers who want to reject self-rooted forks must
	// enforce that separately.
	root := newKey(t)
	operator := newKey(t)
	now := time.Now()
	d, err := New(&root.PublicKey, &operator.PublicKey,
		[]string{string(CapAdvertise), string(CapServe), string(CapDelegate)},
		now, now.Add(24*time.Hour), 16, nil)
	require.NoError(t, err)
	require.NoError(t, d.Sign(root))
	rootUCAN, err := d.Encode()
	require.NoError(t, err)

	acceptor := &ForkAuthorityAccept{
		ForkVerifier:  NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, root)}}),
		RootAcceptSet: nil,
		RequiredCaps:  []string{string(CapAdvertise), string(CapServe)},
		Audience:      compressed(t, operator),
	}
	require.NoError(t, acceptor.Accept(rootUCAN, now))
}

func TestForkAuthorityAccept_Depth2ChainWithRootInAcceptSet(t *testing.T) {
	// Grandchild fork whose ancestry walks two hops back to a
	// root-chain trust root that IS in the accept-set. Pipeline
	// accepts.
	mainnet := newKey(t)
	mainnetPub := compressed(t, mainnet)
	mainnetUCAN := mintRootChainUCAN(t, mainnet)

	child := newKey(t)
	childPub := compressed(t, child)
	childUCAN := mintForkChainUCAN(t, child, mainnetPub)

	grandchild := newKey(t)
	operator := newKey(t)
	now := time.Now()
	grandchildUCAN, err := MintForkAuthorityUCAN(grandchild, &operator.PublicKey, childPub, now, now.Add(24*time.Hour), nil)
	require.NoError(t, err)

	resolver := mapResolver{
		string(mainnetPub): mainnetUCAN,
		string(childPub):   childUCAN,
	}
	acceptor := &ForkAuthorityAccept{
		ForkVerifier:   NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, grandchild)}}),
		ParentResolver: resolver,
		RootAcceptSet:  [][]byte{mainnetPub},
		RequiredCaps:   []string{string(CapAdvertise), string(CapServe)},
		Audience:       compressed(t, operator),
	}
	require.NoError(t, acceptor.Accept(grandchildUCAN, now))
}
