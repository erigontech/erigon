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

// ForkAuthorityAccept composes the layers a fork-follower runs on a
// received fork-authority UCAN into a single pass:
//
//  1. Verifier.Verify — signature integrity, time window, issuer in
//     the fork trust-root set, required capabilities present.
//  2. WalkForkAncestry — the arbitrary-depth cascade C.4 uses to walk
//     from the leaf up to a root-chain UCAN.
//  3. AcceptSet — the walked terminal trust root must be in the
//     operator's accept-set for parent chains.
//
// Rejections carry the earliest-layer error to keep the failure mode
// actionable (which layer said no).
type ForkAuthorityAccept struct {
	// ForkVerifier is the Verifier configured with the FORK'S own
	// trust root(s) — the direct signer of the leaf authority UCAN.
	// Must be non-nil.
	ForkVerifier *Verifier

	// ParentResolver fetches an ancestor's authority UCAN by trust
	// root pubkey. See WalkForkAncestry docs. Nil accepts only
	// self-rooted UCANs (no forked-from).
	ParentResolver ForkAncestryResolver

	// RootAcceptSet is the operator's list of acceptable ROOT-chain
	// trust-root pubkeys (each 33-byte compressed secp256k1). A
	// walked cascade's terminal must be a byte-equal member. Empty
	// rejects every fork whose ancestry actually walks (a root-only
	// UCAN never consults the accept-set — it's already the root).
	RootAcceptSet [][]byte

	// RequiredCaps is passed to Verifier.Verify — the fork authority
	// must carry all of these.
	RequiredCaps []string

	// Audience is the compressed pubkey of the operator the leaf
	// delegates to. Matched by Verifier.Verify.
	Audience []byte

	// DepthCap bounds the ancestry walk. Zero uses
	// DefaultForkAncestryDepthCap.
	DepthCap int
}

// ErrForkAuthorityTerminalNotInAcceptSet is returned when the cascade
// walks successfully but the terminal (root-chain) trust root isn't
// in ForkAuthorityAccept.RootAcceptSet.
var ErrForkAuthorityTerminalNotInAcceptSet = errors.New("walked cascade terminal trust root is not in accept-set")

// Accept runs the composed pipeline. Returns nil when every layer
// passes; wraps the layer-specific error otherwise.
func (a *ForkAuthorityAccept) Accept(leafUCAN []byte, now time.Time) error {
	if a == nil || a.ForkVerifier == nil {
		return fmt.Errorf("ForkAuthorityAccept.Accept: nil verifier")
	}
	if _, err := a.ForkVerifier.Verify(leafUCAN, a.Audience, a.RequiredCaps, now, nil); err != nil {
		return fmt.Errorf("verify leaf: %w", err)
	}
	resolver := a.ParentResolver
	if resolver == nil {
		resolver = ForkAncestryResolverFunc(func(_ []byte) ([]byte, error) { return nil, nil })
	}
	chain, err := WalkForkAncestry(leafUCAN, resolver, a.DepthCap)
	if err != nil {
		return fmt.Errorf("walk ancestry: %w", err)
	}
	terminal := AcceptSetFromAncestry(chain)
	if len(chain) == 1 {
		// Self-rooted authority (no forked-from) — nothing to check
		// against the parent accept-set. The Verifier already
		// confirmed the terminal (== leaf) issuer is in ForkVerifier's
		// root set. A caller who wants stricter behavior for
		// self-rooted forks can post-check chain[0] itself.
		return nil
	}
	if !AcceptSet(terminal, a.RootAcceptSet) {
		return fmt.Errorf("%w: terminal=%x", ErrForkAuthorityTerminalNotInAcceptSet, terminal)
	}
	return nil
}
