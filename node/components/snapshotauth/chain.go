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
	"errors"
	"fmt"
	"time"
)

// TrustRootKind tags a trust root by where its authority comes from.
// The verifier walks roots in priority order DID → ENR → Bootnode, so
// the most specific external authority wins when multiple kinds could
// satisfy the same chain.
type TrustRootKind int

const (
	// RootDID is an externally-issued DID-bound public key. Most
	// specific authority — operator opted into a third-party root.
	RootDID TrustRootKind = iota

	// RootENR is an explicit pinned-ENR allowlist. The pubkey is the
	// secp256k1 from the trusted ENR.
	RootENR

	// RootBootnode is a pubkey derived from the operator's configured
	// bootnode set. Catch-all "trust the fleet" floor when no
	// explicit ENR/DID list is supplied.
	RootBootnode
)

func (k TrustRootKind) String() string {
	switch k {
	case RootDID:
		return "did"
	case RootENR:
		return "enr"
	case RootBootnode:
		return "bootnode"
	}
	return "unknown"
}

// TrustRoot is a single configured authority anchor. Pubkey is the
// 33-byte compressed secp256k1 public key the verifier compares
// against a chain's root Issuer.
//
// DID is informational for the DID kind (e.g. "did:key:z6M..."); the
// authority itself is the bound Pubkey. Empty for non-DID kinds.
type TrustRoot struct {
	Kind   TrustRootKind
	Pubkey []byte
	DID    string
}

// Verifier evaluates a delegation chain against a configured set of
// trust roots. Construct once with NewVerifier; Verify is safe to call
// concurrently — Verifier holds no mutable state.
type Verifier struct {
	roots []TrustRoot
}

// NewVerifier returns a Verifier configured with the given roots. The
// roots slice is copied; the verifier won't observe later mutations.
//
// Roots may be supplied in any order; Verify walks them in priority
// order DID → ENR → Bootnode regardless of the input ordering.
func NewVerifier(roots []TrustRoot) *Verifier {
	cp := make([]TrustRoot, len(roots))
	copy(cp, roots)
	return &Verifier{roots: cp}
}

// VerifyResult is the validated outcome of a successful chain check.
// MatchedRoot tells the caller WHICH configured root the chain rooted
// at — useful for diagnostics (which DID / ENR / bootnode endorsed
// this peer).
type VerifyResult struct {
	Leaf        *Delegation
	Chain       []*Delegation // root..leaf order
	MatchedRoot TrustRoot
}

// Verify checks a delegation chain against the configured trust roots.
// On success returns the decoded leaf and the matched root. Returns an
// error describing the first failure (signature, time, capability
// attenuation, depth, audience, or no-matching-root).
//
// Required-side parameters:
//
//   - leafCBOR: canonical CBOR of the leaf delegation (the deepest
//     link the peer presented; parent links nest inside).
//   - audience: 33-byte compressed pubkey the leaf must address — the
//     peer whose authority we are checking.
//   - requireCaps: capabilities the leaf must grant; usually
//     [snapshot:advertise, snapshot:serve] for source selection.
//   - now: evaluation time (caller passes time.Now() in production;
//     tests pass a fixed instant).
func (v *Verifier) Verify(leafCBOR []byte, audience []byte, requireCaps []string, now time.Time) (*VerifyResult, error) {
	if len(v.roots) == 0 {
		return nil, errors.New("verifier has no trust roots configured")
	}

	leaf, err := Decode(leafCBOR)
	if err != nil {
		return nil, fmt.Errorf("decode leaf: %w", err)
	}
	if !equalBytes(leaf.Audience, audience) {
		return nil, fmt.Errorf("leaf audience does not match expected pubkey")
	}

	// Walk back to the root, building the chain leaf..root, then
	// reverse so each step verifies against its parent above it.
	chainLeafFirst := []*Delegation{leaf}
	for cur := leaf; len(cur.Parent) > 0; {
		parent, err := Decode(cur.Parent)
		if err != nil {
			return nil, fmt.Errorf("decode parent at depth %d: %w", len(chainLeafFirst), err)
		}
		chainLeafFirst = append(chainLeafFirst, parent)
		cur = parent
	}
	// Reverse to root..leaf so loop indices read naturally.
	chain := make([]*Delegation, len(chainLeafFirst))
	for i, d := range chainLeafFirst {
		chain[len(chainLeafFirst)-1-i] = d
	}
	root := chain[0]

	matched, ok := v.matchRoot(root.Issuer)
	if !ok {
		return nil, fmt.Errorf("chain root issuer does not match any configured trust root")
	}

	for i, link := range chain {
		if err := link.VerifySignature(); err != nil {
			return nil, fmt.Errorf("signature failure at depth %d: %w", i, err)
		}
		if !timeWithin(link, now) {
			return nil, fmt.Errorf("delegation at depth %d not currently valid (NotBefore/Expires bounds)", i)
		}
		if i > 0 {
			parent := chain[i-1]
			if !equalBytes(parent.Audience, link.Issuer) {
				return nil, fmt.Errorf("chain integrity break at depth %d: parent.audience != child.issuer", i)
			}
			if !capabilitiesAreSubset(link.Capabilities, parent.Capabilities) {
				return nil, fmt.Errorf("capability attenuation violated at depth %d: child grants caps the parent did not", i)
			}
			if parent.DepthCap == 0 {
				return nil, fmt.Errorf("parent at depth %d is a leaf (DepthCap=0); cannot re-delegate", i-1)
			}
			if link.DepthCap > parent.DepthCap-1 {
				return nil, fmt.Errorf("depth attenuation violated at depth %d: child cap %d > parent cap %d - 1",
					i, link.DepthCap, parent.DepthCap)
			}
			if !hasCapability(parent.Capabilities, string(CapDelegate)) {
				return nil, fmt.Errorf("parent at depth %d lacks snapshot:delegate; cannot re-delegate", i-1)
			}
		}
	}

	for _, want := range requireCaps {
		if !hasCapability(leaf.Capabilities, want) {
			return nil, fmt.Errorf("leaf missing required capability %q", want)
		}
	}

	return &VerifyResult{
		Leaf:        leaf,
		Chain:       chain,
		MatchedRoot: matched,
	}, nil
}

// matchRoot walks the configured roots in priority order (DID first,
// then ENR, then Bootnode) and returns the first whose Pubkey matches
// the supplied issuer. Pubkey comparison is byte-exact; both forms
// must be 33-byte compressed.
func (v *Verifier) matchRoot(issuer []byte) (TrustRoot, bool) {
	priority := []TrustRootKind{RootDID, RootENR, RootBootnode}
	for _, kind := range priority {
		for _, r := range v.roots {
			if r.Kind != kind {
				continue
			}
			if equalBytes(r.Pubkey, issuer) {
				return r, true
			}
		}
	}
	return TrustRoot{}, false
}

// timeWithin reports whether now is in [NotBefore, Expires]. Zero
// NotBefore means effective-immediately; zero Expires means indefinite.
func timeWithin(d *Delegation, now time.Time) bool {
	unix := now.Unix()
	if d.NotBefore != 0 && unix < d.NotBefore {
		return false
	}
	if d.Expires != 0 && unix > d.Expires {
		return false
	}
	return true
}

// capabilitiesAreSubset reports whether every capability in child is
// also in parent. Both slices are assumed already sorted/de-duplicated
// (the canonical form New() produces).
func capabilitiesAreSubset(child, parent []string) bool {
	parentSet := make(map[string]struct{}, len(parent))
	for _, c := range parent {
		parentSet[c] = struct{}{}
	}
	for _, c := range child {
		if _, ok := parentSet[c]; !ok {
			return false
		}
	}
	return true
}

func hasCapability(caps []string, want string) bool {
	for _, c := range caps {
		if c == want {
			return true
		}
	}
	return false
}
