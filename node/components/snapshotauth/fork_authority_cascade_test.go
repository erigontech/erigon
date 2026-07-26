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
	"crypto/ecdsa"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Cascade -ve tests for the fork authority UCAN trust tree.
//
// The fork trust model (see memory/fork-trust-root-model-2026-05-24)
// has multiple verification layers. Each tampering point must reject
// at the EARLIEST possible point in the cascade:
//
//   1. Decode + leaf audience match          ← Verifier.Verify
//   2. Signature integrity                   ← Verifier.Verify
//   3. Time-window validity (nbf / expires)  ← Verifier.Verify
//   4. Trust-root match (issuer ∈ roots)     ← Verifier.Verify
//   5. Required capabilities present         ← Verifier.Verify
//   6. forked-from capability extraction     ← ExtractForkedFromCapability
//   7. Parent trust root ∈ accept-set        ← AcceptSet
//
// Layers 1-5 are enforced by Verifier.Verify; layers 6-7 are the
// fork-specific extension that runs after Verifier.Verify succeeds.
// Each cascade test asserts that the named tampering rejects at the
// matching layer's error.

// forkFixture is the standard "honest fork operator" setup the
// cascade tests tamper around. It captures the keys + bytes the
// negative-path tests vary one element of.
type forkFixture struct {
	trustRoot    *ecdsa.PrivateKey
	operator     *ecdsa.PrivateKey
	parentPub    []byte // 33-byte compressed pubkey of the parent trust root
	encoded      []byte // freshly minted, untampered fork authority UCAN
	notBefore    time.Time
	expires      time.Time
	verifier     *Verifier
	audience     []byte // compressed pubkey of the operator (Verify's audience input)
	acceptSet    [][]byte
	now          time.Time
	requiredCaps []string
}

func mintForkFixture(t *testing.T) forkFixture {
	t.Helper()
	trustRoot := newKey(t)
	operator := newKey(t)
	parentPub := compressed(t, newKey(t))
	now := time.Now()
	expires := now.Add(ForkAuthorityUCANValidity)
	encoded, err := MintForkAuthorityUCAN(trustRoot, &operator.PublicKey, parentPub, now, expires, nil)
	require.NoError(t, err)
	return forkFixture{
		trustRoot:    trustRoot,
		operator:     operator,
		parentPub:    parentPub,
		encoded:      encoded,
		notBefore:    now,
		expires:      expires,
		verifier:     NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, trustRoot)}}),
		audience:     compressed(t, operator),
		acceptSet:    [][]byte{parentPub},
		now:          now,
		requiredCaps: []string{string(CapAdvertise), string(CapServe)},
	}
}

// runFullCascade is the +ve-path verification a fork-follower would
// perform: Verifier.Verify the fork authority UCAN, then check
// forked-from is in the accept-set. Tests use it to assert the
// untampered fixture passes end-to-end.
func runFullCascade(t *testing.T, f forkFixture, leafCBOR []byte) (parentTrustRoot []byte, err error) {
	t.Helper()
	res, err := f.verifier.Verify(leafCBOR, f.audience, f.requiredCaps, f.now, nil)
	if err != nil {
		return nil, err
	}
	extracted, ok := ExtractForkedFromCapability(res.Leaf)
	if !ok {
		return nil, errForkedFromMissing
	}
	if !AcceptSet(extracted, f.acceptSet) {
		return nil, errParentNotInAcceptSet
	}
	return extracted, nil
}

// Sentinel errors so the cascade tests can assert which layer rejected.
// Defined in tests rather than the production code because no consumer
// has wired the full cascade as a single function yet (Phase 2c-CL).
var (
	errForkedFromMissing    = stringErr("forked-from capability missing")
	errParentNotInAcceptSet = stringErr("parent trust root not in accept-set")
)

type stringErr string

func (e stringErr) Error() string { return string(e) }

// TestForkAuthorityCascade_HappyPath_AcceptsHonestFixture pins the
// baseline: the untampered fixture passes all cascade layers and
// returns the embedded parent trust root.
func TestForkAuthorityCascade_HappyPath_AcceptsHonestFixture(t *testing.T) {
	f := mintForkFixture(t)
	parent, err := runFullCascade(t, f, f.encoded)
	require.NoError(t, err)
	require.Equal(t, f.parentPub, parent)
}

// === Layer 1-5: Verifier.Verify rejections ===

// TestForkAuthorityCascade_TamperedSignatureRejects covers the case
// in the cascade memo: "Fork authority UCAN signature broken → Fork
// manifest UCAN verify (layer 1)". Mutating the encoded delegation
// (after the signature is bound) must reject at the signature step.
func TestForkAuthorityCascade_TamperedSignatureRejects(t *testing.T) {
	f := mintForkFixture(t)
	// Decode, flip one bit in the signature, re-encode. The
	// (Issuer, digest, Signature) triple no longer verifies.
	d, err := Decode(f.encoded)
	require.NoError(t, err)
	d.Signature = append([]byte{}, d.Signature...)
	d.Signature[0] ^= 0xFF
	tampered, err := d.Encode()
	require.NoError(t, err)

	_, err = runFullCascade(t, f, tampered)
	require.Error(t, err)
	require.Contains(t, err.Error(), "signature failure",
		"signature tampering must reject at Verifier.Verify's signature step")
}

// TestForkAuthorityCascade_ExpiredRejects covers "Fork operator
// delegation expired → layer 1". A fork authority UCAN whose expires
// is in the past at verification time must reject at the time-window
// check.
func TestForkAuthorityCascade_ExpiredRejects(t *testing.T) {
	f := mintForkFixture(t)
	// Re-mint with an expires in the past.
	pastExpires := f.now.Add(-time.Hour)
	pastNotBefore := pastExpires.Add(-time.Hour)
	encoded, err := MintForkAuthorityUCAN(f.trustRoot, &f.operator.PublicKey, f.parentPub, pastNotBefore, pastExpires, nil)
	require.NoError(t, err)

	_, err = runFullCascade(t, f, encoded)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not currently valid",
		"expired UCAN must reject at Verifier.Verify's time-window step")
}

// TestForkAuthorityCascade_NotYetValidRejects mirrors the expired case
// from the opposite side: a UCAN whose notBefore is in the future at
// verification time must reject at the time-window check.
func TestForkAuthorityCascade_NotYetValidRejects(t *testing.T) {
	f := mintForkFixture(t)
	futureNotBefore := f.now.Add(24 * time.Hour)
	futureExpires := futureNotBefore.Add(24 * time.Hour)
	encoded, err := MintForkAuthorityUCAN(f.trustRoot, &f.operator.PublicKey, f.parentPub, futureNotBefore, futureExpires, nil)
	require.NoError(t, err)

	_, err = runFullCascade(t, f, encoded)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not currently valid",
		"not-yet-valid UCAN must reject at Verifier.Verify's time-window step")
}

// TestForkAuthorityCascade_IssuerNotInTrustRootsRejects covers a UCAN
// signed by a key the verifier doesn't recognise. The signature
// itself is valid (some key signed it), but no configured trust root
// matches.
func TestForkAuthorityCascade_IssuerNotInTrustRootsRejects(t *testing.T) {
	f := mintForkFixture(t)
	// Mint a UCAN signed by an unrelated key.
	imposter := newKey(t)
	encoded, err := MintForkAuthorityUCAN(imposter, &f.operator.PublicKey, f.parentPub, f.notBefore, f.expires, nil)
	require.NoError(t, err)

	_, err = runFullCascade(t, f, encoded)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match any configured trust root",
		"unknown issuer must reject at the trust-root match step")
}

// TestForkAuthorityCascade_LeafAudienceMismatchRejects covers a UCAN
// where the audience presented to Verify doesn't match the leaf's
// Audience field. Tampering with the audience (or simply passing the
// wrong one) must reject at the leaf-audience step, before any
// signature work.
func TestForkAuthorityCascade_LeafAudienceMismatchRejects(t *testing.T) {
	f := mintForkFixture(t)
	other := newKey(t)
	// Use the honest fixture's encoded bytes but pass the wrong audience.
	res, err := f.verifier.Verify(f.encoded, compressed(t, other), f.requiredCaps, f.now, nil)
	require.Nil(t, res)
	require.Error(t, err)
	require.Contains(t, err.Error(), "audience does not match",
		"audience mismatch must reject before any signature work")
}

// TestForkAuthorityCascade_RequiredCapabilityMissingRejects covers
// a fork authority UCAN that doesn't grant a capability the
// fork-follower requires. MintForkAuthorityUCAN grants advertise +
// serve + delegate + fork:from:<id>; we require an unrelated cap.
func TestForkAuthorityCascade_RequiredCapabilityMissingRejects(t *testing.T) {
	f := mintForkFixture(t)
	// Require a capability the fork authority UCAN doesn't grant.
	res, err := f.verifier.Verify(f.encoded, f.audience, []string{"snapshot:nonexistent"}, f.now, nil)
	require.Nil(t, res)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing required capability",
		"missing cap must reject at the required-capability step")
}

// === Layer 6: forked-from capability extraction ===

// TestForkAuthorityCascade_ForkedFromCapMissingRejects covers a
// delegation that VERIFIES correctly but lacks the fork:from:<id>
// capability — i.e. a regular authority UCAN, not a fork authority.
// Verifier.Verify passes; ExtractForkedFromCapability returns
// ok=false; the cascade rejects at layer 6.
func TestForkAuthorityCascade_ForkedFromCapMissingRejects(t *testing.T) {
	f := mintForkFixture(t)
	// Build a non-fork authority UCAN: same trust root + operator,
	// standard caps, but no fork:from:<id>.
	d, err := New(&f.trustRoot.PublicKey, &f.operator.PublicKey,
		[]string{string(CapAdvertise), string(CapServe), string(CapDelegate)},
		f.notBefore, f.expires, 16, nil)
	require.NoError(t, err)
	require.NoError(t, d.Sign(f.trustRoot))
	encoded, err := d.Encode()
	require.NoError(t, err)

	_, err = runFullCascade(t, f, encoded)
	require.Error(t, err)
	require.ErrorIs(t, err, errForkedFromMissing,
		"a non-fork authority UCAN must reject at the forked-from extraction step")
}

// TestForkAuthorityCascade_MalformedForkedFromRejects covers layer-6
// (E.5): a UCAN carries a `fork:from:` prefix but the payload is
// malformed (non-hex or wrong-length pubkey). ExtractForkedFromCapability
// silently skips such entries; if no other well-formed forked-from is
// present the cascade rejects at the extraction step.
func TestForkAuthorityCascade_MalformedForkedFromRejects(t *testing.T) {
	f := mintForkFixture(t)

	malformed := []string{
		CapForkedFromPrefix + "not-hex!!",
		CapForkedFromPrefix + "0102030405060708090a0b0c0d0e0f10", // 16 bytes, wrong length
		CapForkedFromPrefix,
	}
	for _, cap := range malformed {
		d, err := New(&f.trustRoot.PublicKey, &f.operator.PublicKey,
			[]string{string(CapAdvertise), string(CapServe), string(CapDelegate), cap},
			f.notBefore, f.expires, 16, nil)
		require.NoError(t, err)
		require.NoError(t, d.Sign(f.trustRoot))
		encoded, err := d.Encode()
		require.NoError(t, err)

		_, err = runFullCascade(t, f, encoded)
		require.ErrorIs(t, err, errForkedFromMissing,
			"malformed fork:from cap %q must be skipped and cascade rejects at extraction", cap)
	}
}

// === Layer 7: accept-set check ===

// TestForkAuthorityCascade_ParentNotInAcceptSetRejects covers a
// fork authority UCAN that verifies correctly AND carries a
// well-formed forked-from capability, but the embedded parent trust
// root pubkey is NOT one the operator's accept-set vetted. Must
// reject at layer 7.
func TestForkAuthorityCascade_ParentNotInAcceptSetRejects(t *testing.T) {
	f := mintForkFixture(t)
	// Configure the fixture's accept-set to a different parent.
	otherParent := compressed(t, newKey(t))
	f.acceptSet = [][]byte{otherParent}

	_, err := runFullCascade(t, f, f.encoded)
	require.Error(t, err)
	require.ErrorIs(t, err, errParentNotInAcceptSet,
		"forked-from not in accept-set must reject at the accept-set step")
}

// TestForkAuthorityCascade_ParentRotationDoesNotWidenAcceptSet pins
// the immutability rule (D-5): a fork's ValidParentTrustRoots is
// captured at fork-from time and does NOT drift as the parent chain
// rotates its own trust roots. A UCAN minted under a rotated parent
// root (rotatedParent) that isn't in the fork's original accept-set
// must reject at the accept-set step — the fork's cascade uses only
// the LOCAL config's captured set, never a set derived from the peer
// manifest or from the parent's current roots.
func TestForkAuthorityCascade_ParentRotationDoesNotWidenAcceptSet(t *testing.T) {
	f := mintForkFixture(t)

	// Fork was created with accept-set = [f.parentPub] (the original
	// parent trust root). Mint a UCAN under a DIFFERENT parent root
	// simulating the parent chain rotating its root post-fork-creation.
	rotatedParentPub := compressed(t, newKey(t))
	encoded, err := MintForkAuthorityUCAN(f.trustRoot, &f.operator.PublicKey, rotatedParentPub, f.notBefore, f.expires, nil)
	require.NoError(t, err)

	_, err = runFullCascade(t, f, encoded)
	require.ErrorIs(t, err, errParentNotInAcceptSet,
		"rotated-parent UCAN must reject; accept-set is immutable")

	// The original-parent UCAN still verifies under the same fixture —
	// existing fork lineage stays valid after parent rotation.
	_, err = runFullCascade(t, f, f.encoded)
	require.NoError(t, err, "original-parent UCAN continues to verify")
}

// TestForkAuthorityCascade_EmptyAcceptSetRejectsEverything is the
// degenerate case: an operator who hasn't configured any accept-set
// can never accept any fork. Defends against a misconfigured launch
// that ships with no parent-trust-roots flag value.
func TestForkAuthorityCascade_EmptyAcceptSetRejectsEverything(t *testing.T) {
	f := mintForkFixture(t)
	f.acceptSet = nil

	_, err := runFullCascade(t, f, f.encoded)
	require.Error(t, err)
	require.ErrorIs(t, err, errParentNotInAcceptSet,
		"empty accept-set must reject every fork at the accept-set step")
}

// === Earliest-rejection invariant ===

// TestForkAuthorityCascade_EarliestRejection_SignatureBeforeTime
// asserts the cascade ordering: when MULTIPLE elements are tampered
// simultaneously, the EARLIER layer's rejection wins. A UCAN that is
// both signature-tampered AND time-expired must reject at the
// signature step, not the time step — because signature is the
// earlier layer.
//
// This invariant matters operationally: a fork-follower's verifier
// burns minimal work on bogus inputs. Tampering can't be used to
// probe later layers' behavior by short-circuiting earlier ones.
func TestForkAuthorityCascade_EarliestRejection_SignatureBeforeTime(t *testing.T) {
	f := mintForkFixture(t)
	// Mint with expires in the past AND tamper signature.
	pastExpires := f.now.Add(-time.Hour)
	pastNotBefore := pastExpires.Add(-time.Hour)
	encoded, err := MintForkAuthorityUCAN(f.trustRoot, &f.operator.PublicKey, f.parentPub, pastNotBefore, pastExpires, nil)
	require.NoError(t, err)
	// Now also tamper signature by flipping a bit.
	d, err := Decode(encoded)
	require.NoError(t, err)
	d.Signature = append([]byte{}, d.Signature...)
	d.Signature[0] ^= 0xFF
	tampered, err := d.Encode()
	require.NoError(t, err)

	_, err = runFullCascade(t, f, tampered)
	require.Error(t, err)
	// Verifier.Verify orders signature BEFORE time-window per chain.go.
	require.Contains(t, err.Error(), "signature failure",
		"signature is earlier than time-window in the cascade; signature must reject first")
}

// TestForkAuthorityCascade_EarliestRejection_TrustRootBeforeSignature
// pins the ordering between root-issuer match (chain.go:177) and
// signature verify (chain.go:183). A UCAN signed by an unknown key
// AND with a mangled signature must reject at the trust-root step —
// there's no point burning EC work on a chain that can't reach a root.
func TestForkAuthorityCascade_EarliestRejection_TrustRootBeforeSignature(t *testing.T) {
	f := mintForkFixture(t)
	imposter := newKey(t)
	encoded, err := MintForkAuthorityUCAN(imposter, &f.operator.PublicKey, f.parentPub, f.notBefore, f.expires, nil)
	require.NoError(t, err)
	// Also flip the signature.
	d, err := Decode(encoded)
	require.NoError(t, err)
	d.Signature = append([]byte{}, d.Signature...)
	d.Signature[0] ^= 0xFF
	tampered, err := d.Encode()
	require.NoError(t, err)

	_, err = runFullCascade(t, f, tampered)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match any configured trust root",
		"trust-root match runs before signature; must reject there first")
}

// TestForkAuthorityCascade_EarliestRejection_AudienceBeforeTrustRoot
// pins ordering: leaf audience mismatch (chain.go:139) runs before the
// chain is walked at all, so it must reject before any trust-root work
// even if the trust root also mismatches.
func TestForkAuthorityCascade_EarliestRejection_AudienceBeforeTrustRoot(t *testing.T) {
	f := mintForkFixture(t)
	imposter := newKey(t)
	// Mint by imposter (unknown root) — but query with wrong audience.
	encoded, err := MintForkAuthorityUCAN(imposter, &f.operator.PublicKey, f.parentPub, f.notBefore, f.expires, nil)
	require.NoError(t, err)
	other := newKey(t)
	res, err := f.verifier.Verify(encoded, compressed(t, other), f.requiredCaps, f.now, nil)
	require.Nil(t, res)
	require.Error(t, err)
	require.Contains(t, err.Error(), "audience does not match",
		"leaf audience mismatch runs before any chain walking or trust-root work")
}

// === Depth-2 chain: Fork Authority → sub-delegation to a serving node ===
//
// A depth-2 fork chain re-delegates from the fork authority operator
// to another node (e.g. an untrusted publisher granted serve-only
// authority). res.Chain is [ForkAuthority, SubDelegation] with the
// sub-delegation as res.Leaf.
//
// Content UCANs are verified out-of-band (manifest_exchange/provider.go
// line 715+), not through Verifier.Verify, so they're not tested here.

// mintForkSubDelegation issues a leaf sub-delegation off the honest
// fork-authority fixture. leaf caps are a strict subset of the fork
// authority's caps so attenuation passes.
func mintForkSubDelegation(t *testing.T, f forkFixture, subAudience *ecdsa.PrivateKey, caps []string) []byte {
	t.Helper()
	leaf, err := New(&f.operator.PublicKey, &subAudience.PublicKey,
		caps, f.notBefore, f.expires, 0, f.encoded)
	require.NoError(t, err)
	require.NoError(t, leaf.Sign(f.operator))
	encoded, err := leaf.Encode()
	require.NoError(t, err)
	return encoded
}

// runFullChainCascade verifies a depth-2 fork chain: sub-delegation
// leaf; ExtractForkedFromCapability walks the chain (fork:from: sits
// on chain[0], the fork authority, not the leaf).
func runFullChainCascade(t *testing.T, f forkFixture, subAudience []byte, leafCBOR []byte, requiredCaps []string) error {
	t.Helper()
	res, err := f.verifier.Verify(leafCBOR, subAudience, requiredCaps, f.now, parentResolver(f.encoded))
	if err != nil {
		return err
	}
	var found []byte
	for _, link := range res.Chain {
		if pub, ok := ExtractForkedFromCapability(link); ok {
			found = pub
			break
		}
	}
	if found == nil {
		return errForkedFromMissing
	}
	if !AcceptSet(found, f.acceptSet) {
		return errParentNotInAcceptSet
	}
	return nil
}

// TestForkChainCascade_HappyPath_SubDelegation pins the depth-2
// baseline: honest fork authority + honest sub-delegation → passes
// end-to-end, fork:from: found by walking the chain.
func TestForkChainCascade_HappyPath_SubDelegation(t *testing.T) {
	f := mintForkFixture(t)
	sub := newKey(t)
	leaf := mintForkSubDelegation(t, f, sub, []string{string(CapAdvertise), string(CapServe)})
	require.NoError(t, runFullChainCascade(t, f, compressed(t, sub), leaf,
		[]string{string(CapAdvertise), string(CapServe)}))
}

// TestForkChainCascade_TamperedLeafSignatureRejects mutates the
// sub-delegation bytes after signing. Verifier's per-link signature
// check catches it.
func TestForkChainCascade_TamperedLeafSignatureRejects(t *testing.T) {
	f := mintForkFixture(t)
	sub := newKey(t)
	leaf := mintForkSubDelegation(t, f, sub, []string{string(CapAdvertise)})
	d, err := Decode(leaf)
	require.NoError(t, err)
	d.Signature = append([]byte{}, d.Signature...)
	d.Signature[0] ^= 0xFF
	tampered, err := d.Encode()
	require.NoError(t, err)

	err = runFullChainCascade(t, f, compressed(t, sub), tampered, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "signature failure",
		"leaf signature tampering must reject at the per-link signature step")
}

// TestForkChainCascade_ParentSubstitutionRejects covers the parent-hash
// binding: the leaf pins the exact Fork Authority bytes via
// ParentHash. A resolver that returns different bytes for that hash
// must fail the hash-equality check inside Verify. Force this by
// making the resolver return an unrelated blob when asked for the
// pinned parent hash.
func TestForkChainCascade_ParentSubstitutionRejects(t *testing.T) {
	f := mintForkFixture(t)
	sub := newKey(t)
	leaf := mintForkSubDelegation(t, f, sub, []string{string(CapAdvertise)})

	// Build an alternate fork authority (different bytes) and a
	// resolver that answers the honest fork authority's hash with the
	// alternate's bytes.
	other := mintForkFixture(t)
	honestHash := HashOf(f.encoded)
	swapResolver := func(h []byte) ([]byte, error) {
		if bytes.Equal(h, honestHash) {
			return other.encoded, nil
		}
		return nil, nil
	}

	res, err := f.verifier.Verify(leaf, compressed(t, sub), nil, f.now, swapResolver)
	require.Nil(t, res)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not hash to the child's ParentHash",
		"a substituted parent whose bytes don't hash to the pinned ParentHash must reject at the hash-equality check")
}

// TestForkChainCascade_CapabilityAttenuationBreak covers a
// sub-delegation whose caps are NOT a subset of the fork authority's.
// The fork authority does not grant `chain.v2:hash:<hex>` (that's a
// Content-UCAN-only cap), so a sub-delegation attempting to carry one
// via manually-constructed bytes must reject at subset check.
func TestForkChainCascade_CapabilityAttenuationBreak(t *testing.T) {
	f := mintForkFixture(t)
	sub := newKey(t)
	// leaf.Issuer must equal parent.Audience (= f.operator) so chain
	// integrity check passes and attenuation is the layer that trips.
	// Leaf carries a content-hash cap the fork authority does not.
	leaf := &Delegation{
		Version:      CurrentVersion,
		Issuer:       compressed(t, f.operator),
		Audience:     compressed(t, sub),
		ParentHash:   HashOf(f.encoded),
		Capabilities: []string{ContentHashCapability(strings.Repeat("cd", sha256.Size))},
		NotBefore:    f.notBefore.Unix(),
		Expires:      f.expires.Unix(),
		DepthCap:     0,
	}
	require.NoError(t, leaf.Sign(f.operator))
	leafEnc, err := leaf.Encode()
	require.NoError(t, err)

	err = runFullChainCascade(t, f, compressed(t, sub), leafEnc, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "capability attenuation violated",
		"child cap not present in parent must reject at subset check")
}

// TestForkChainCascade_ChainIntegrityBreak_IssuerMismatch covers
// parent.Audience != child.Issuer. Craft a leaf whose Issuer is not
// the fork operator (i.e. not the audience the fork authority
// endorsed). Per-link signature still verifies (some key signed
// something) but chain integrity check rejects.
func TestForkChainCascade_ChainIntegrityBreak_IssuerMismatch(t *testing.T) {
	f := mintForkFixture(t)
	imposter := newKey(t)
	sub := newKey(t)
	forgedLeaf, err := New(&imposter.PublicKey, &sub.PublicKey,
		[]string{string(CapAdvertise)}, f.notBefore, f.expires, 0, f.encoded)
	require.NoError(t, err)
	require.NoError(t, forgedLeaf.Sign(imposter))
	leafEnc, err := forgedLeaf.Encode()
	require.NoError(t, err)

	res, err := f.verifier.Verify(leafEnc, compressed(t, sub), nil, f.now, parentResolver(f.encoded))
	require.Nil(t, res)
	require.Error(t, err)
	require.Contains(t, err.Error(), "chain integrity break",
		"child.Issuer != parent.Audience must reject at chain-integrity check")
}

// TestForkChainCascade_ParentLacksDelegate covers a fork authority
// UCAN that verifies but does NOT grant CapDelegate — so it cannot
// have children. This shouldn't happen with MintForkAuthorityUCAN,
// which always grants delegate, but a hand-constructed fork authority
// might omit it. Any sub-delegation must reject at the parent-lacks-
// delegate check (chain.go:204).
func TestForkChainCascade_ParentLacksDelegate(t *testing.T) {
	trustRoot := newKey(t)
	operator := newKey(t)
	sub := newKey(t)
	parentPub := compressed(t, newKey(t))
	forkCap, err := ForkedFromCapability(parentPub)
	require.NoError(t, err)
	// Manually-minted fork authority WITHOUT CapDelegate.
	forkAuthWithoutDelegate, err := New(&trustRoot.PublicKey, &operator.PublicKey,
		[]string{string(CapAdvertise), string(CapServe), forkCap},
		time.Time{}, time.Time{}, 16, nil)
	require.NoError(t, err)
	require.NoError(t, forkAuthWithoutDelegate.Sign(trustRoot))
	faEnc, err := forkAuthWithoutDelegate.Encode()
	require.NoError(t, err)

	leaf, err := New(&operator.PublicKey, &sub.PublicKey,
		[]string{string(CapAdvertise)}, time.Time{}, time.Time{}, 0, faEnc)
	require.NoError(t, err)
	require.NoError(t, leaf.Sign(operator))
	leafEnc, err := leaf.Encode()
	require.NoError(t, err)

	v := NewVerifier([]TrustRoot{{Kind: RootENR, Pubkey: compressed(t, trustRoot)}})
	res, err := v.Verify(leafEnc, compressed(t, sub), nil, time.Now(), parentResolver(faEnc))
	require.Nil(t, res)
	require.Error(t, err)
	require.Contains(t, err.Error(), "lacks snapshot:delegate",
		"a parent without CapDelegate cannot re-delegate")
}
