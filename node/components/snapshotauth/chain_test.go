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
		[]string{string(CapAdvertise)}, time.Now())
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, RootENR, res.MatchedRoot.Kind)
	require.Len(t, res.Chain, 1)
}

func TestVerify_TwoHopChainSucceeds(t *testing.T) {
	keys := newChainKeys(t)
	now := time.Now()

	rootDel := mustSignedDelegation(t, keys.root, keys.mid,
		[]string{string(CapAdvertise), string(CapServe), string(CapDelegate)},
		2, nil)
	midDel := mustSignedDelegation(t, keys.mid, keys.leaf,
		[]string{string(CapAdvertise), string(CapServe)},
		1, mustEncoded(t, rootDel))
	leafCBOR := mustEncoded(t, midDel)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, keys.root)}})
	res, err := v.Verify(leafCBOR, compressed(t, keys.leaf),
		[]string{string(CapAdvertise)}, now)
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
		[]string{string(CapAdvertise)}, time.Now())
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
		[]string{string(CapAdvertise)}, time.Now())
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
		[]string{string(CapAdvertise)}, time.Now())
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
		[]string{string(CapServe)}, time.Now())
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
		notBefore.Add(-time.Hour))
	require.Error(t, err)
	require.Contains(t, err.Error(), "not currently valid")

	// Inside the window — must succeed.
	_, err = v.Verify(enc, audience, caps, notBefore.Add(24*time.Hour))
	require.NoError(t, err)

	// After Expires — must fail.
	_, err = v.Verify(enc, audience, caps, expires.Add(time.Hour))
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
		time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err, "indefinite-expiry delegation should still verify in 2099")
}

func TestVerify_CapabilityAttenuationViolation(t *testing.T) {
	keys := newChainKeys(t)

	// Root grants :advertise, :delegate; mid (incorrectly) grants :serve too.
	rootDel := mustSignedDelegation(t, keys.root, keys.mid,
		[]string{string(CapAdvertise), string(CapDelegate)}, 2, nil)
	midDel := mustSignedDelegation(t, keys.mid, keys.leaf,
		[]string{string(CapAdvertise), string(CapServe)}, 1,
		mustEncoded(t, rootDel))
	enc := mustEncoded(t, midDel)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, keys.root)}})
	_, err := v.Verify(enc, compressed(t, keys.leaf),
		[]string{string(CapAdvertise)}, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "capability attenuation violated")
}

func TestVerify_DepthAttenuationViolation(t *testing.T) {
	keys := newChainKeys(t)

	// Root has DepthCap 1 — mid can only have DepthCap 0.
	// We construct mid with DepthCap 2 to trigger violation.
	rootDel := mustSignedDelegation(t, keys.root, keys.mid,
		[]string{string(CapAdvertise), string(CapDelegate)}, 1, nil)
	midDel := mustSignedDelegation(t, keys.mid, keys.leaf,
		[]string{string(CapAdvertise)}, 2, mustEncoded(t, rootDel))
	enc := mustEncoded(t, midDel)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, keys.root)}})
	_, err := v.Verify(enc, compressed(t, keys.leaf),
		[]string{string(CapAdvertise)}, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "depth attenuation violated")
}

func TestVerify_LeafParentLacksDelegateCap(t *testing.T) {
	keys := newChainKeys(t)

	// Root grants :advertise but NOT :delegate. Mid trying to chain
	// from it should fail — parent didn't authorise re-delegation.
	rootDel := mustSignedDelegation(t, keys.root, keys.mid,
		[]string{string(CapAdvertise)}, 1, nil)
	midDel := mustSignedDelegation(t, keys.mid, keys.leaf,
		[]string{string(CapAdvertise)}, 0, mustEncoded(t, rootDel))
	enc := mustEncoded(t, midDel)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, keys.root)}})
	_, err := v.Verify(enc, compressed(t, keys.leaf),
		[]string{string(CapAdvertise)}, time.Now())
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
		[]string{string(CapAdvertise)}, time.Now())
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
		[]string{string(CapAdvertise)}, time.Now())
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
		[]string{string(CapAdvertise)}, time.Now())
	require.Error(t, err)
	require.Contains(t, err.Error(), "no trust roots configured")
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
