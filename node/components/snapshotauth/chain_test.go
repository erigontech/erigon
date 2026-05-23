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
	"crypto/elliptic"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// chainKeys is a small helper bundle for tests that build root → mid → leaf chains.
type chainKeys struct {
	root, mid, leaf *ecdsa.PrivateKey
}

func newChainKeys(t *testing.T) chainKeys {
	t.Helper()
	return chainKeys{
		root: newKey(t),
		mid:  newKey(t),
		leaf: newKey(t),
	}
}

func compressed(t *testing.T, k *ecdsa.PrivateKey) []byte {
	t.Helper()
	return elliptic.MarshalCompressed(k.PublicKey.Curve, k.PublicKey.X, k.PublicKey.Y)
}

func TestVerify_RootDelegationSucceeds(t *testing.T) {
	root := newKey(t)
	leaf := newKey(t)

	d, err := New(&root.PublicKey, &leaf.PublicKey,
		[]string{string(CapAdvertise), string(CapServe)},
		time.Time{}, time.Time{}, 0, nil)
	require.NoError(t, err)
	require.NoError(t, d.Sign(root))
	enc, err := d.Encode()
	require.NoError(t, err)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, root)}})

	res, err := v.Verify(enc, compressed(t, leaf),
		[]string{string(CapAdvertise)}, time.Now(), nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, RootENR, res.MatchedRoot.Kind)
	require.Len(t, res.Chain, 1)
}

// parentResolver builds an in-memory ParentResolver over the given
// encoded parent delegations, keyed by their canonical-CBOR hash.
func parentResolver(parents ...[]byte) ParentResolver {
	byHash := make(map[string][]byte, len(parents))
	for _, p := range parents {
		byHash[string(HashOf(p))] = p
	}
	return func(h []byte) ([]byte, error) {
		p, ok := byHash[string(h)]
		if !ok {
			return nil, fmt.Errorf("parent hash %x not registered", h)
		}
		return p, nil
	}
}

func TestVerify_TwoHopChainSucceeds(t *testing.T) {
	keys := newChainKeys(t)
	now := time.Now()

	rootDel := mustSignedDelegation(t, keys.root, keys.mid,
		[]string{string(CapAdvertise), string(CapServe), string(CapDelegate)},
		2, nil)
	rootEnc := mustEncoded(t, rootDel)
	midDel := mustSignedDelegation(t, keys.mid, keys.leaf,
		[]string{string(CapAdvertise), string(CapServe)},
		1, rootEnc)
	leafCBOR := mustEncoded(t, midDel)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, keys.root)}})
	res, err := v.Verify(leafCBOR, compressed(t, keys.leaf),
		[]string{string(CapAdvertise)}, now, parentResolver(rootEnc))
	require.NoError(t, err)
	require.Len(t, res.Chain, 2, "root + mid")
}

func TestVerify_DIDRootBeatsENRRoot(t *testing.T) {
	root := newKey(t)
	leaf := newKey(t)

	d := mustSignedDelegation(t, root, leaf,
		[]string{string(CapAdvertise)}, 0, nil)
	enc := mustEncoded(t, d)

	// Both ENR and DID roots are configured with the same pubkey.
	// Verifier must report DID as the matched root since DID has
	// higher priority.
	pub := compressed(t, root)
	v := NewVerifier([]TrustRoot{
		{Kind: RootENR, Pubkey: pub},
		{Kind: RootDID, Pubkey: pub, DID: "did:key:zABC"},
		{Kind: RootBootnode, Pubkey: pub},
	})

	res, err := v.Verify(enc, compressed(t, leaf),
		[]string{string(CapAdvertise)}, time.Now(), nil)
	require.NoError(t, err)
	require.Equal(t, RootDID, res.MatchedRoot.Kind,
		"DID priority must win over ENR/Bootnode when multiple roots match")
	require.Equal(t, "did:key:zABC", res.MatchedRoot.DID)
}

func TestVerify_NoMatchingRoot(t *testing.T) {
	root := newKey(t)
	leaf := newKey(t)
	other := newKey(t)

	d := mustSignedDelegation(t, root, leaf,
		[]string{string(CapAdvertise)}, 0, nil)
	enc := mustEncoded(t, d)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, other)}})
	_, err := v.Verify(enc, compressed(t, leaf),
		[]string{string(CapAdvertise)}, time.Now(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match any configured trust root")
}

func TestVerify_AudienceMismatch(t *testing.T) {
	root := newKey(t)
	leaf := newKey(t)
	other := newKey(t)

	d := mustSignedDelegation(t, root, leaf,
		[]string{string(CapAdvertise)}, 0, nil)
	enc := mustEncoded(t, d)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, root)}})
	_, err := v.Verify(enc, compressed(t, other),
		[]string{string(CapAdvertise)}, time.Now(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "audience does not match")
}

func TestVerify_RequiredCapabilityMissing(t *testing.T) {
	root := newKey(t)
	leaf := newKey(t)

	// Leaf grants only :advertise; we require :serve.
	d := mustSignedDelegation(t, root, leaf,
		[]string{string(CapAdvertise)}, 0, nil)
	enc := mustEncoded(t, d)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, root)}})
	_, err := v.Verify(enc, compressed(t, leaf),
		[]string{string(CapServe)}, time.Now(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing required capability")
}

func TestVerify_TimeWindowEnforced(t *testing.T) {
	root := newKey(t)
	leaf := newKey(t)

	notBefore := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	expires := time.Date(2031, 1, 1, 0, 0, 0, 0, time.UTC)

	d, err := New(&root.PublicKey, &leaf.PublicKey,
		[]string{string(CapAdvertise)},
		notBefore, expires, 0, nil)
	require.NoError(t, err)
	require.NoError(t, d.Sign(root))
	enc, err := d.Encode()
	require.NoError(t, err)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, root)}})
	audience := compressed(t, leaf)
	caps := []string{string(CapAdvertise)}

	// Before NotBefore — must fail.
	_, err = v.Verify(enc, audience, caps,
		notBefore.Add(-time.Hour), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not currently valid")

	// Inside the window — must succeed.
	_, err = v.Verify(enc, audience, caps, notBefore.Add(24*time.Hour), nil)
	require.NoError(t, err)

	// After Expires — must fail.
	_, err = v.Verify(enc, audience, caps, expires.Add(time.Hour), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not currently valid")
}

func TestVerify_IndefiniteExpiry(t *testing.T) {
	root := newKey(t)
	leaf := newKey(t)

	d := mustSignedDelegation(t, root, leaf,
		[]string{string(CapServe)}, 0, nil)
	enc := mustEncoded(t, d)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, root)}})
	// Far-future timestamp must still verify when Expires == 0.
	_, err := v.Verify(enc, compressed(t, leaf),
		[]string{string(CapServe)},
		time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC), nil)
	require.NoError(t, err, "indefinite-expiry delegation should still verify in 2099")
}

func TestVerify_CapabilityAttenuationViolation(t *testing.T) {
	keys := newChainKeys(t)

	// Root grants :advertise, :delegate; mid (incorrectly) grants :serve too.
	rootDel := mustSignedDelegation(t, keys.root, keys.mid,
		[]string{string(CapAdvertise), string(CapDelegate)}, 2, nil)
	rootEnc := mustEncoded(t, rootDel)
	midDel := mustSignedDelegation(t, keys.mid, keys.leaf,
		[]string{string(CapAdvertise), string(CapServe)}, 1, rootEnc)
	enc := mustEncoded(t, midDel)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, keys.root)}})
	_, err := v.Verify(enc, compressed(t, keys.leaf),
		[]string{string(CapAdvertise)}, time.Now(), parentResolver(rootEnc))
	require.Error(t, err)
	require.Contains(t, err.Error(), "capability attenuation violated")
}

func TestVerify_DepthAttenuationViolation(t *testing.T) {
	keys := newChainKeys(t)

	// Root has DepthCap 1 — mid can only have DepthCap 0.
	// We construct mid with DepthCap 2 to trigger violation.
	rootDel := mustSignedDelegation(t, keys.root, keys.mid,
		[]string{string(CapAdvertise), string(CapDelegate)}, 1, nil)
	rootEnc := mustEncoded(t, rootDel)
	midDel := mustSignedDelegation(t, keys.mid, keys.leaf,
		[]string{string(CapAdvertise)}, 2, rootEnc)
	enc := mustEncoded(t, midDel)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, keys.root)}})
	_, err := v.Verify(enc, compressed(t, keys.leaf),
		[]string{string(CapAdvertise)}, time.Now(), parentResolver(rootEnc))
	require.Error(t, err)
	require.Contains(t, err.Error(), "depth attenuation violated")
}

func TestVerify_LeafParentLacksDelegateCap(t *testing.T) {
	keys := newChainKeys(t)

	// Root grants :advertise but NOT :delegate. Mid trying to chain
	// from it should fail — parent didn't authorise re-delegation.
	rootDel := mustSignedDelegation(t, keys.root, keys.mid,
		[]string{string(CapAdvertise)}, 1, nil)
	rootEnc := mustEncoded(t, rootDel)
	midDel := mustSignedDelegation(t, keys.mid, keys.leaf,
		[]string{string(CapAdvertise)}, 0, rootEnc)
	enc := mustEncoded(t, midDel)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, keys.root)}})
	_, err := v.Verify(enc, compressed(t, keys.leaf),
		[]string{string(CapAdvertise)}, time.Now(), parentResolver(rootEnc))
	require.Error(t, err)
	require.Contains(t, err.Error(), "lacks snapshot:delegate")
}

func TestVerify_TamperedSignature(t *testing.T) {
	root := newKey(t)
	leaf := newKey(t)

	d := mustSignedDelegation(t, root, leaf,
		[]string{string(CapAdvertise)}, 0, nil)
	// Flip a byte in the signature.
	d.Signature[0] ^= 0xFF
	enc, err := d.Encode()
	require.NoError(t, err)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, root)}})
	_, err = v.Verify(enc, compressed(t, leaf),
		[]string{string(CapAdvertise)}, time.Now(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "signature failure")
}

func TestVerify_ChainIntegrityBreak(t *testing.T) {
	keys := newChainKeys(t)
	other := newKey(t)

	// Mid is signed by `mid`, but the parent's audience is `other`
	// (we forge a parent that doesn't authorise mid).
	otherAsAudience, err := New(&keys.root.PublicKey, &other.PublicKey,
		[]string{string(CapAdvertise), string(CapDelegate)},
		time.Time{}, time.Time{}, 2, nil)
	require.NoError(t, err)
	require.NoError(t, otherAsAudience.Sign(keys.root))
	parentEnc, err := otherAsAudience.Encode()
	require.NoError(t, err)

	midDel := mustSignedDelegation(t, keys.mid, keys.leaf,
		[]string{string(CapAdvertise)}, 1, parentEnc)
	enc := mustEncoded(t, midDel)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, keys.root)}})
	_, err = v.Verify(enc, compressed(t, keys.leaf),
		[]string{string(CapAdvertise)}, time.Now(), parentResolver(parentEnc))
	require.Error(t, err)
	require.Contains(t, err.Error(), "chain integrity break")
}

func TestVerify_NoRootsConfigured(t *testing.T) {
	root := newKey(t)
	leaf := newKey(t)
	d := mustSignedDelegation(t, root, leaf,
		[]string{string(CapAdvertise)}, 0, nil)
	enc := mustEncoded(t, d)

	v := NewVerifier(nil)
	_, err := v.Verify(enc, compressed(t, leaf),
		[]string{string(CapAdvertise)}, time.Now(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no trust roots configured")
}

// TestVerify_ParentHashNotResolvable pins the rejection path when a
// non-root leaf carries a ParentHash that the supplied ParentResolver
// can't resolve — e.g. a peer references a parent UCAN the consumer
// has not fetched. Verify must fail at parent resolution, not silently
// proceed.
func TestVerify_ParentHashNotResolvable(t *testing.T) {
	keys := newChainKeys(t)
	now := time.Now()

	// Mid delegation references a root we never register with the resolver.
	rootDel := mustSignedDelegation(t, keys.root, keys.mid,
		[]string{string(CapAdvertise), string(CapDelegate)}, 2, nil)
	rootEnc := mustEncoded(t, rootDel)
	midDel := mustSignedDelegation(t, keys.mid, keys.leaf,
		[]string{string(CapAdvertise)}, 1, rootEnc)
	leafCBOR := mustEncoded(t, midDel)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, keys.root)}})
	_, err := v.Verify(leafCBOR, compressed(t, keys.leaf),
		[]string{string(CapAdvertise)}, now,
		// Empty resolver — knows about no parents.
		parentResolver())
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolve parent")
}

// TestVerify_ResolvedParentHashMismatch pins the rejection path when a
// resolver returns CBOR that does not hash to the child's ParentHash —
// the defense against a resolver substituting a different parent. The
// Verify code re-hashes the returned bytes and rejects on mismatch.
func TestVerify_ResolvedParentHashMismatch(t *testing.T) {
	keys := newChainKeys(t)
	now := time.Now()

	// Real root + mid chain.
	rootDel := mustSignedDelegation(t, keys.root, keys.mid,
		[]string{string(CapAdvertise), string(CapDelegate)}, 2, nil)
	rootEnc := mustEncoded(t, rootDel)
	midDel := mustSignedDelegation(t, keys.mid, keys.leaf,
		[]string{string(CapAdvertise)}, 1, rootEnc)
	leafCBOR := mustEncoded(t, midDel)

	// Build a substitute parent — a totally different delegation that
	// happens to be well-formed. The resolver returns it for the hash
	// the child actually references.
	otherKey := newKey(t)
	otherDel := mustSignedDelegation(t, otherKey, keys.mid,
		[]string{string(CapAdvertise), string(CapDelegate)}, 2, nil)
	otherEnc := mustEncoded(t, otherDel)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, keys.root)}})
	_, err := v.Verify(leafCBOR, compressed(t, keys.leaf),
		[]string{string(CapAdvertise)}, now,
		// Resolver returns otherEnc bytes regardless of the requested hash —
		// a tampered/substituting resolver.
		func(_ []byte) ([]byte, error) { return otherEnc, nil })
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not hash to")
}

// TestVerify_NonRootWithNilResolver pins the rejection path when a
// non-root leaf is presented but no ParentResolver is supplied. Verify
// cannot follow the chain without one — silently treating the leaf as
// root would let a non-root leaf escape root-issuer matching.
func TestVerify_NonRootWithNilResolver(t *testing.T) {
	keys := newChainKeys(t)
	now := time.Now()

	rootDel := mustSignedDelegation(t, keys.root, keys.mid,
		[]string{string(CapAdvertise), string(CapDelegate)}, 2, nil)
	rootEnc := mustEncoded(t, rootDel)
	midDel := mustSignedDelegation(t, keys.mid, keys.leaf,
		[]string{string(CapAdvertise)}, 1, rootEnc)
	leafCBOR := mustEncoded(t, midDel)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, keys.root)}})
	_, err := v.Verify(leafCBOR, compressed(t, keys.leaf),
		[]string{string(CapAdvertise)}, now, nil) // nil resolver
	require.Error(t, err)
	require.Contains(t, err.Error(), "no ParentResolver")
}

// TestVerify_ChainDepthExceeded pins the rejection path when a chain
// presents more than maxChainDepth links. Defense against unbounded or
// cyclic parent references in malicious artefacts.
func TestVerify_ChainDepthExceeded(t *testing.T) {
	now := time.Now()

	// Build a chain of (maxChainDepth + 1) links so the walker's
	// len(chainLeafFirst) >= maxChainDepth check trips before reaching
	// the root. numKeys = numLinks + 1; each link sits between two
	// adjacent keys.
	numLinks := maxChainDepth + 1
	keysPath := make([]*ecdsa.PrivateKey, numLinks+1)
	for i := range keysPath {
		keysPath[i] = newKey(t)
	}

	var parentEnc []byte
	parents := []([]byte){}
	// Root's DepthCap covers the whole chain; each link decrements by 1.
	rootCap := uint16(numLinks + 2)
	for i := 0; i < numLinks; i++ {
		dc := rootCap - uint16(i)
		d := mustSignedDelegation(t, keysPath[i], keysPath[i+1],
			[]string{string(CapAdvertise), string(CapDelegate)},
			dc, parentEnc)
		enc := mustEncoded(t, d)
		parents = append(parents, enc)
		parentEnc = enc
	}
	leafCBOR := parentEnc

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, keysPath[0])}})
	_, err := v.Verify(leafCBOR, compressed(t, keysPath[numLinks]),
		[]string{string(CapAdvertise)}, now, parentResolver(parents...))
	require.Error(t, err)
	require.Contains(t, err.Error(), "max depth")
}

// TestVerify_ParentDepthCapZero pins the leaf-cannot-re-delegate fence
// (chain.go's `parent.DepthCap == 0` check). A parent with DepthCap=0
// is a leaf authority — any child claiming to chain from it must be
// rejected even if the parent has CapDelegate and the child's own
// DepthCap is also 0 (which would otherwise pass the depth-attenuation
// check via uint16 underflow).
func TestVerify_ParentDepthCapZero(t *testing.T) {
	keys := newChainKeys(t)

	// Root has CapDelegate (so the lacks-delegate check passes) but
	// DepthCap=0 — it cannot re-delegate.
	rootDel := mustSignedDelegation(t, keys.root, keys.mid,
		[]string{string(CapAdvertise), string(CapDelegate)}, 0, nil)
	rootEnc := mustEncoded(t, rootDel)
	midDel := mustSignedDelegation(t, keys.mid, keys.leaf,
		[]string{string(CapAdvertise)}, 0, rootEnc)
	enc := mustEncoded(t, midDel)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, keys.root)}})
	_, err := v.Verify(enc, compressed(t, keys.leaf),
		[]string{string(CapAdvertise)}, time.Now(), parentResolver(rootEnc))
	require.Error(t, err)
	require.Contains(t, err.Error(), "is a leaf")
}

// TestVerify_LeafNotSelfIssued documents the responsibility boundary:
// snapshotauth's Verify checks signature + chain-integrity
// (parent.Audience == child.Issuer) but does NOT enforce that a leaf is
// self-issued (issuer == audience). Content UCANs in the V2 manifest
// flow are minted self-issued by MintContentUCAN, but that invariant
// is established at mint time, not policed by the chain verifier — any
// consumer that interprets a leaf's audience as the publisher identity
// must verify self-issue itself.
//
// Pins current behavior. If a future change adds self-issue enforcement
// inside snapshotauth Verify, update this test to assert rejection.
func TestVerify_LeafNotSelfIssued(t *testing.T) {
	rootKey := newKey(t)
	operatorA := newKey(t)
	operatorB := newKey(t)

	// Authority: root → operatorA, with CapDelegate so operatorA can
	// re-issue. CapAdvertise present so the leaf can chain it (subset).
	authority := mustSignedDelegation(t, rootKey, operatorA,
		[]string{string(CapAdvertise), string(CapDelegate)}, 2, nil)
	authorityEnc := mustEncoded(t, authority)

	// Leaf: issued by operatorA, audience = operatorB (NOT self-issued).
	// Only CapAdvertise so capability attenuation passes.
	leafDel := mustSignedDelegation(t, operatorA, operatorB,
		[]string{string(CapAdvertise)}, 0, authorityEnc)
	leafEnc := mustEncoded(t, leafDel)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, rootKey)}})
	res, err := v.Verify(leafEnc, compressed(t, operatorB),
		[]string{string(CapAdvertise)}, time.Now(), parentResolver(authorityEnc))
	require.NoError(t, err, "snapshotauth does NOT enforce leaf self-issue; this is a caller invariant")
	require.NotEqual(t, res.Leaf.Issuer, res.Leaf.Audience,
		"leaf is not self-issued — caller must check this if it relies on it")
}

func mustSignedDelegation(t *testing.T, issuerKey, audienceKey *ecdsa.PrivateKey, caps []string, depth uint16, parent []byte) *Delegation {
	t.Helper()
	d, err := New(&issuerKey.PublicKey, &audienceKey.PublicKey,
		caps, time.Time{}, time.Time{}, depth, parent)
	require.NoError(t, err)
	require.NoError(t, d.Sign(issuerKey))
	return d
}

func mustEncoded(t *testing.T, d *Delegation) []byte {
	t.Helper()
	enc, err := d.Encode()
	require.NoError(t, err)
	return enc
}
