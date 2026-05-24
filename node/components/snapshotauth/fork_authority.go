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
	"fmt"
	"time"
)

// ForkAuthorityUCANValidity is the default lifetime of a fork
// authority UCAN. Long-lived (months) — the same cadence as a root
// chain's authority UCAN, since both root the operator's publish
// authority and rotate on operator-key changes, not on per-generation
// cadence. Callers can override via notBefore/expires in
// MintForkAuthorityUCAN.
const ForkAuthorityUCANValidity = 90 * 24 * time.Hour

// MintForkAuthorityUCAN issues the fork's long-lived authority UCAN —
// the Delegation that roots the fork operator's publishing key under
// the fork's trust root AND embeds which parent trust root vetted
// ParentManifestHash at fork creation.
//
// The minted UCAN's capability set is the standard publish set
// (snapshot:advertise, snapshot:serve, snapshot:delegate) PLUS one
// fork:from:<parent-trust-root-pubkey-hex> capability. A fork-follower
// reads that single capability to identify which parent trust root the
// fork claims it forked from; if the operator's accept-set contains
// the pubkey, the fork's parental provenance is accepted.
//
// Args:
//   - trustRootKey: the fork's trust-root secp256k1 private key
//     (signer of this UCAN). The Issuer is its pubkey.
//   - operatorPubkey: the fork operator's secp256k1 pubkey (Audience).
//   - parentTrustRootPubkey: 33-byte compressed secp256k1 pubkey of
//     the parent trust root the fork operator used at fork-from time
//     to validate ParentManifestHash. Embedded as
//     fork:from:<hex(pubkey)>.
//   - notBefore / expires: validity window. Callers typically pass
//     time.Now() and time.Now().Add(ForkAuthorityUCANValidity).
//   - parentEncoded: the canonical CBOR of the root delegation
//     this fork authority chains to (or nil for a root). For a fork
//     whose trust root is the operator's own, parentEncoded is nil
//     (self-rooted fork — common in shadow-fork testnets).
//
// Returns the canonical CBOR bytes ready for distribution as the
// chain.ucan.authority.<enr-fp>.<rev>.bin sidecar.
func MintForkAuthorityUCAN(
	trustRootKey *ecdsa.PrivateKey,
	operatorPubkey *ecdsa.PublicKey,
	parentTrustRootPubkey []byte,
	notBefore, expires time.Time,
	parentEncoded []byte,
) ([]byte, error) {
	if trustRootKey == nil {
		return nil, fmt.Errorf("MintForkAuthorityUCAN: nil trust root key")
	}
	if operatorPubkey == nil {
		return nil, fmt.Errorf("MintForkAuthorityUCAN: nil operator pubkey")
	}
	forkCap, err := ForkedFromCapability(parentTrustRootPubkey)
	if err != nil {
		return nil, fmt.Errorf("MintForkAuthorityUCAN: %w", err)
	}
	capabilities := []string{
		string(CapAdvertise),
		string(CapServe),
		string(CapDelegate),
		forkCap,
	}
	d, err := New(
		&trustRootKey.PublicKey, operatorPubkey,
		capabilities,
		notBefore, expires,
		// DepthCap > 0 so the operator can issue sub-delegations (e.g.
		// per-generation Content UCANs against this authority).
		16,
		parentEncoded,
	)
	if err != nil {
		return nil, fmt.Errorf("construct fork authority UCAN: %w", err)
	}
	if err := d.Sign(trustRootKey); err != nil {
		return nil, fmt.Errorf("sign fork authority UCAN: %w", err)
	}
	return d.Encode()
}

// ExtractForkedFromCapability reads a Delegation's capability set and
// returns the parent-trust-root pubkey embedded in its
// fork:from:<hex> capability, if any. ok=false when the delegation
// carries no forked-from capability (i.e. not a fork authority — a
// root chain's authority UCAN never embeds this).
//
// A delegation should have at most one fork:from: capability; if more
// than one is present, the first is returned with ok=true (multiple
// is malformed but defensive — the verifier's accept-set check
// against the returned pubkey is what enforces correctness).
func ExtractForkedFromCapability(d *Delegation) (parentTrustRootPubkey []byte, ok bool) {
	if d == nil {
		return nil, false
	}
	for _, c := range d.Capabilities {
		if pub, ok := ParseForkedFromCapability(c); ok {
			return pub, true
		}
	}
	return nil, false
}

// AcceptSet returns true when parentTrustRootPubkey is present in the
// operator-configured accept-set. Used after ExtractForkedFromCapability
// to decide whether a fork's claimed parental provenance is acceptable.
//
// Empty accept-set returns false for any non-zero input — an operator
// who hasn't configured any accept-set rejects every fork. Empty
// accept-set + zero/empty input returns false too (defensive: an
// uninitialised value can't accidentally pass).
//
// Comparison is byte-equal on the 33-byte compressed secp256k1 pubkey;
// matches the standard TrustRoot.Pubkey equality used by the verifier.
func AcceptSet(parentTrustRootPubkey []byte, accept [][]byte) bool {
	if len(parentTrustRootPubkey) == 0 {
		return false
	}
	for _, p := range accept {
		if bytes.Equal(p, parentTrustRootPubkey) {
			return true
		}
	}
	return false
}
