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
