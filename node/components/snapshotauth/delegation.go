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

// Package snapshotauth defines the UCAN-shaped delegation envelope used
// by the snapshot subsystem to gate which peers a node trusts as data
// sources. A Delegation carries a signed authorisation from an issuer
// public key to an audience public key, scoped to a set of capability
// strings, with optional expiry and a depth cap on further delegation.
//
// Storage shape — canonical CBOR. Each on-disk artefact is a single
// CBOR-encoded Delegation; chains are nested via the Parent field which
// itself holds the CBOR-encoded parent. JSON output is provided as a
// read-only inspection utility — never written as the canonical form.
//
// Verifier rules summarised:
//   - Signature is over canonicalBytes(d) where canonicalBytes excludes
//     the Signature field itself; this is deterministic CBOR with
//     fixed field order.
//   - Expires == 0 means indefinite; otherwise NotBefore <= now <= Expires.
//   - DepthCap on a non-root delegation must not exceed Parent.DepthCap - 1.
//     Re-delegation can narrow but never widen the cap.
//   - Capabilities on a non-root delegation must be a subset of the parent's
//     (case-sensitive set comparison after sort/dedup).
//   - Audience of the parent must equal Issuer of the child.
//   - Root delegation: Parent == nil; Issuer is the trust anchor the
//     verifier matches against its configured root set.
package snapshotauth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/fxamacker/cbor/v2"

	"github.com/erigontech/erigon/common/crypto"
)

// CurrentVersion is the schema version stamped into every Delegation
// emitted by this package. Verifiers reject unknown versions outright;
// future schema evolutions bump this and the verifier learns the new
// shape.
//
// v2: the parent link is stored by hash (ParentHash) rather than inline
// (the former Parent []byte). v1 is rejected outright — nothing is in
// production, so no migration path is kept.
const CurrentVersion uint8 = 2

// PubKeyLen is the length of a compressed secp256k1 public key in
// bytes. Issuer and Audience are always compressed.
const PubKeyLen = 33

// SignatureLen is the length of a secp256k1 ECDSA signature in the
// recoverable [R || S || V] form used elsewhere in the codebase
// (common/crypto.Sign).
const SignatureLen = 65

// Delegation is the on-disk + on-wire shape of a snapshot trust
// attestation. Field order is fixed by `cbor:",toarray"` so the
// canonical encoding is deterministic without relying on map-key
// ordering.
type Delegation struct {
	_ struct{} `cbor:",toarray"`

	// Version of the schema; reject unknown values at parse time.
	Version uint8

	// Issuer is the compressed secp256k1 pubkey of the signer of this
	// delegation. For a root delegation, this is the trust-anchor
	// pubkey the verifier matches against its configured root set.
	Issuer []byte

	// Audience is the compressed secp256k1 pubkey this delegation
	// authorises. For a root delegation issued to a target node,
	// Audience is the target node's pubkey.
	Audience []byte

	// Capabilities are the snapshot capability strings this
	// delegation grants. Sorted lexicographically + de-duplicated for
	// canonical encoding.
	Capabilities []string

	// NotBefore is the unix-second timestamp before which this
	// delegation is invalid. 0 = effective immediately.
	NotBefore int64

	// Expires is the unix-second timestamp after which this
	// delegation is invalid. 0 = indefinite (no expiry).
	Expires int64

	// DepthCap is the maximum further delegation depth. 0 means this
	// is a leaf (cannot be re-delegated). Each re-delegation must use
	// at most DepthCap-1 in the child.
	DepthCap uint16

	// ParentHash is sha256 of the parent Delegation's canonical CBOR
	// encoding, or nil for a root. The parent itself is fetched
	// out-of-band by the verifier's ParentResolver and bound back to
	// this hash. By-hash (rather than the former inline parent) keeps
	// the slow-changing Authority UCAN out of every fast-changing
	// Content UCAN generation — see docs/plans/20260520-chaintoml-ucan-flow-spec.md.
	ParentHash []byte

	// Signature is the secp256k1 signature over the canonical CBOR
	// encoding of this Delegation with Signature itself zeroed.
	Signature []byte
}

// New constructs an unsigned Delegation. Sign() must be called before
// the value is encoded for distribution.
//
// parentEncoded is the canonical CBOR of the parent Delegation, or nil
// for a root. New stores sha256(parentEncoded) as ParentHash; the
// caller is responsible for making the parent itself resolvable by
// that hash (the verifier's ParentResolver fetches it).
//
// The capabilities slice is sorted and de-duplicated in place; callers
// must not assume their input ordering is preserved.
func New(issuerPub, audiencePub *ecdsa.PublicKey, capabilities []string, notBefore, expires time.Time, depthCap uint16, parentEncoded []byte) (*Delegation, error) {
	issuerCompressed, err := compressedPubKey(issuerPub)
	if err != nil {
		return nil, fmt.Errorf("issuer pubkey: %w", err)
	}
	audienceCompressed, err := compressedPubKey(audiencePub)
	if err != nil {
		return nil, fmt.Errorf("audience pubkey: %w", err)
	}
	caps := canonicalCapabilities(capabilities)
	if len(caps) == 0 {
		return nil, errors.New("delegation must grant at least one capability")
	}
	d := &Delegation{
		Version:      CurrentVersion,
		Issuer:       issuerCompressed,
		Audience:     audienceCompressed,
		Capabilities: caps,
		NotBefore:    unixOrZero(notBefore),
		Expires:      unixOrZero(expires),
		DepthCap:     depthCap,
	}
	if len(parentEncoded) > 0 {
		sum := sha256.Sum256(parentEncoded)
		d.ParentHash = sum[:]
	}
	return d, nil
}

// HashOf returns sha256 of a delegation's canonical CBOR encoding —
// the value a child stores in ParentHash and the verifier's
// ParentResolver result is bound back to.
func HashOf(encoded []byte) []byte {
	sum := sha256.Sum256(encoded)
	return sum[:]
}

// Sign computes the signature over the canonical encoding of d (with
// Signature zeroed) and stores it in d.Signature. The signing key must
// match d.Issuer; mismatch returns an error.
func (d *Delegation) Sign(privKey *ecdsa.PrivateKey) error {
	expected, err := compressedPubKey(&privKey.PublicKey)
	if err != nil {
		return fmt.Errorf("derive signer pubkey: %w", err)
	}
	if !equalBytes(expected, d.Issuer) {
		return errors.New("signer key does not match Issuer")
	}
	digest, err := d.signingDigest()
	if err != nil {
		return err
	}
	sig, err := crypto.Sign(digest, privKey)
	if err != nil {
		return fmt.Errorf("sign delegation: %w", err)
	}
	if len(sig) != SignatureLen {
		return fmt.Errorf("unexpected signature length %d (want %d)", len(sig), SignatureLen)
	}
	d.Signature = sig
	return nil
}

// VerifySignature checks that d.Signature is a valid secp256k1
// signature over the canonical encoding by d.Issuer. Does NOT verify
// the chain to a trust root; chain verification is the verifier's
// responsibility (see Verify).
func (d *Delegation) VerifySignature() error {
	if len(d.Issuer) != PubKeyLen {
		return fmt.Errorf("issuer pubkey length %d (want %d)", len(d.Issuer), PubKeyLen)
	}
	if len(d.Signature) != SignatureLen {
		return fmt.Errorf("signature length %d (want %d)", len(d.Signature), SignatureLen)
	}
	digest, err := d.signingDigest()
	if err != nil {
		return err
	}
	// VerifySignature wants the [R || S] form (64 bytes); strip the recovery byte.
	if !crypto.VerifySignature(d.Issuer, digest, d.Signature[:64]) {
		return errors.New("signature does not verify against issuer pubkey")
	}
	return nil
}

// Encode returns the canonical CBOR encoding of d. The result is
// deterministic for a given Delegation value.
func (d *Delegation) Encode() ([]byte, error) {
	enc, err := canonicalEncoder()
	if err != nil {
		return nil, err
	}
	return enc.Marshal(d)
}

// Decode parses a canonical CBOR encoding of a Delegation. Signature
// verification is NOT performed here — callers run VerifySignature
// (or full Verify) explicitly.
func Decode(data []byte) (*Delegation, error) {
	var d Delegation
	if err := cbor.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("decode delegation: %w", err)
	}
	if d.Version != CurrentVersion {
		return nil, fmt.Errorf("unsupported delegation version %d (want %d)", d.Version, CurrentVersion)
	}
	if len(d.Issuer) != PubKeyLen {
		return nil, fmt.Errorf("issuer pubkey length %d (want %d)", len(d.Issuer), PubKeyLen)
	}
	if len(d.Audience) != PubKeyLen {
		return nil, fmt.Errorf("audience pubkey length %d (want %d)", len(d.Audience), PubKeyLen)
	}
	return &d, nil
}

// signingDigest computes sha256 of the canonical encoding with
// Signature zeroed. This is what Sign signs and VerifySignature
// verifies.
func (d *Delegation) signingDigest() ([]byte, error) {
	cp := *d
	cp.Signature = nil
	enc, err := canonicalEncoder()
	if err != nil {
		return nil, err
	}
	encoded, err := enc.Marshal(&cp)
	if err != nil {
		return nil, fmt.Errorf("encode for signing: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return sum[:], nil
}

// canonicalEncoder returns a CBOR encoder configured for deterministic
// output — sorted map keys and shortest-form integers. Field order in
// `toarray`-tagged structs is already deterministic via the struct
// layout, but we still want any map-typed sub-fields to encode
// canonically.
func canonicalEncoder() (cbor.EncMode, error) {
	opts := cbor.CanonicalEncOptions()
	return opts.EncMode()
}

// compressedPubKey returns the 33-byte compressed encoding of an
// ecdsa.PublicKey on the secp256k1 curve.
func compressedPubKey(pub *ecdsa.PublicKey) ([]byte, error) {
	if pub == nil || pub.X == nil || pub.Y == nil {
		return nil, errors.New("nil public key")
	}
	return elliptic.MarshalCompressed(pub.Curve, pub.X, pub.Y), nil
}

// canonicalCapabilities returns a sorted, de-duplicated copy of caps.
// Empty strings are dropped.
func canonicalCapabilities(caps []string) []string {
	seen := make(map[string]struct{}, len(caps))
	out := make([]string, 0, len(caps))
	for _, c := range caps {
		if c == "" {
			continue
		}
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
