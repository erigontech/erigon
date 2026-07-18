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
	"errors"
	"fmt"
)

// DefaultForkAncestryDepthCap bounds the maximum number of forked-from
// hops WalkForkAncestry will take before erroring. Chosen well above
// any realistic use case so a genuine deep fork chain still walks, but
// low enough that a pathological (or adversarial) resolver can't spin
// forever.
const DefaultForkAncestryDepthCap = 32

// Sentinel errors returned by WalkForkAncestry.
var (
	ErrForkAncestryCycle              = errors.New("fork ancestry contains a cycle")
	ErrForkAncestryDepthExceeded      = errors.New("fork ancestry exceeds depth cap")
	ErrForkAncestryIssuerMismatch     = errors.New("parent UCAN issuer does not match child's forked-from pubkey")
	ErrForkAncestryResolverNotFound   = errors.New("parent authority UCAN not resolvable")
	ErrForkAncestryParentDecodeFailed = errors.New("parent authority UCAN decode failed")
	ErrForkAncestryParentNotRoot      = errors.New("intermediate parent authority UCAN is not a root (has ParentHash)")
)

// ForkAncestryResolver looks up the authority UCAN for a given trust
// root pubkey (compressed secp256k1, 33 bytes). Callers wire this to
// their local knowledge base — the manifest cache, the swarm, a static
// map in tests.
//
// Returns (nil, nil) when the trust root is unknown but not an error
// (WalkForkAncestry treats that as a walk termination when the parent
// is a root chain the caller doesn't know locally; it returns
// ErrForkAncestryResolverNotFound if the trust root is required to
// continue the walk).
type ForkAncestryResolver interface {
	AuthorityUCANForTrustRoot(pubkey []byte) ([]byte, error)
}

// ForkAncestryResolverFunc adapts a function to the ForkAncestryResolver
// interface. Convenient for tests + inline wiring.
type ForkAncestryResolverFunc func(pubkey []byte) ([]byte, error)

func (f ForkAncestryResolverFunc) AuthorityUCANForTrustRoot(pubkey []byte) ([]byte, error) {
	return f(pubkey)
}

// WalkForkAncestry walks the forked-from chain starting from leafAuth
// up to a root authority UCAN (one with no fork:from: capability).
// C.4's arbitrary-depth support: at each level the walker verifies
// that the resolver-supplied parent UCAN's Issuer equals the child's
// forked-from pubkey — the cryptographic link between levels. Walks
// stop when a UCAN carries no forked-from (that's a root chain) or
// when the depth cap is reached (defensive against pathological
// resolvers).
//
// Returns the ordered list of trust-root pubkeys traversed — leaf
// first, root last. Length ≥ 1: the leaf's own issuer is always the
// first element even for a UCAN with no forked-from (a self-rooted
// authority is a depth-0 walk of length 1).
//
// Cycle detection: each visited trust-root pubkey is recorded; a
// repeated pubkey (even via a longer loop) errors with
// ErrForkAncestryCycle.
//
// depthCap of 0 (or negative) uses DefaultForkAncestryDepthCap.
func WalkForkAncestry(leafAuth []byte, resolver ForkAncestryResolver, depthCap int) ([][]byte, error) {
	if resolver == nil {
		return nil, fmt.Errorf("WalkForkAncestry: nil resolver")
	}
	if depthCap <= 0 {
		depthCap = DefaultForkAncestryDepthCap
	}

	current, err := Decode(leafAuth)
	if err != nil {
		return nil, fmt.Errorf("WalkForkAncestry: decode leaf: %w", err)
	}

	visited := make(map[string]struct{}, depthCap)
	chain := make([][]byte, 0, depthCap)

	for step := 0; step <= depthCap; step++ {
		issuerCopy := append([]byte(nil), current.Issuer...)
		if _, ok := visited[string(issuerCopy)]; ok {
			return nil, fmt.Errorf("%w: revisited %x at step %d", ErrForkAncestryCycle, issuerCopy, step)
		}
		visited[string(issuerCopy)] = struct{}{}
		chain = append(chain, issuerCopy)

		forkedFrom, ok := ExtractForkedFromCapability(current)
		if !ok {
			return chain, nil
		}

		if step == depthCap {
			return nil, fmt.Errorf("%w: depth=%d cap=%d", ErrForkAncestryDepthExceeded, step+1, depthCap)
		}

		parentEncoded, rerr := resolver.AuthorityUCANForTrustRoot(forkedFrom)
		if rerr != nil {
			return nil, fmt.Errorf("%w: %v", ErrForkAncestryResolverNotFound, rerr)
		}
		if len(parentEncoded) == 0 {
			return nil, fmt.Errorf("%w: resolver returned empty for %x", ErrForkAncestryResolverNotFound, forkedFrom)
		}

		parent, derr := Decode(parentEncoded)
		if derr != nil {
			return nil, fmt.Errorf("%w: %v", ErrForkAncestryParentDecodeFailed, derr)
		}
		if parent.ParentHash != nil {
			return nil, fmt.Errorf("%w: at step %d", ErrForkAncestryParentNotRoot, step+1)
		}
		if !bytes.Equal(parent.Issuer, forkedFrom) {
			return nil, fmt.Errorf("%w: forked-from=%x issuer=%x", ErrForkAncestryIssuerMismatch, forkedFrom, parent.Issuer)
		}
		current = parent
	}

	return nil, fmt.Errorf("%w: exhausted %d steps without termination", ErrForkAncestryDepthExceeded, depthCap)
}

// AcceptSetFromAncestry returns the terminal trust-root pubkey — the
// last element of a walk chain, which is the root chain's trust root.
// A caller uses this to check the walked cascade against a bare
// accept-set (via AcceptSet).
func AcceptSetFromAncestry(chain [][]byte) []byte {
	if len(chain) == 0 {
		return nil
	}
	return chain[len(chain)-1]
}
