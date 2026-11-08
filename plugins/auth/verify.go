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

package auth

import (
	"context"
	"fmt"
)

// Verifier validates UCAN tokens and delegation chains.
type Verifier struct {
	signers  []Signer
	resolver TokenResolver
	store    Store // optional — if set, revoked tokens are rejected during verification
}

// Signer verifies signatures for a specific DID method.
type Signer interface {
	// CanVerify returns true if this signer handles the given DID.
	CanVerify(did string) bool

	// Verify checks a signature over payload bytes for the given issuer DID.
	Verify(ctx context.Context, payload []byte, signature []byte, issuerDID string) (bool, error)
}

// TokenResolver resolves proof token CIDs to full tokens.
// Implementations may use local cache, MDBX, or torrent download.
type TokenResolver interface {
	// Resolve returns the token for the given CID.
	// Returns nil, nil if the token is not found.
	Resolve(ctx context.Context, cid CID) (*Token, error)
}

// NewVerifier creates a verifier with the given signers and token resolver.
// If store is non-nil, revoked tokens are rejected during verification.
func NewVerifier(resolver TokenResolver, store Store, signers ...Signer) *Verifier {
	return &Verifier{
		signers:  signers,
		resolver: resolver,
		store:    store,
	}
}

// Verify validates a UCAN token:
//  1. Structure validation (required fields)
//  2. Time bounds (not expired, not before nbf)
//  3. Signature verification (dispatched to correct signer)
//  4. Delegation chain (each proof verified recursively, attenuation checked)
//  5. Capability check (token authorizes the required capability)
func (v *Verifier) Verify(ctx context.Context, token *Token, required Capability) error {
	return v.verifyToken(ctx, token, required, 0)
}

const maxChainDepth = 64

func (v *Verifier) verifyToken(ctx context.Context, token *Token, required Capability, depth int) error {
	if depth >= maxChainDepth {
		return fmt.Errorf("delegation chain too deep (max %d)", maxChainDepth)
	}

	// 1. Structure
	if err := token.ValidateStructure(); err != nil {
		return fmt.Errorf("invalid token structure: %w", err)
	}

	// 2. Time bounds
	if !token.IsActive() {
		if token.IsExpired() {
			return fmt.Errorf("token expired (exp=%d)", token.Exp)
		}
		return fmt.Errorf("token not yet valid (nbf=%d)", token.Nbf)
	}

	// 3. Signature
	if err := v.verifySignature(ctx, token); err != nil {
		return fmt.Errorf("signature verification failed: %w", err)
	}

	// 3b. Revocation check — if a store is configured, reject revoked tokens.
	if v.store != nil {
		cid := token.ComputeCID()
		revoked, err := v.store.IsRevoked(ctx, cid, token.Issuer, token.Iat)
		if err != nil {
			return fmt.Errorf("revocation check failed: %w", err)
		}
		if revoked {
			return fmt.Errorf("token %x has been revoked", cid)
		}
	}

	// 4. Capability check — does this token authorize the required capability?
	tokenCap := token.Capability()
	if !tokenCap.Covers(required) {
		return fmt.Errorf("token capability %q does not cover required %q", tokenCap.Command, required.Command)
	}

	// 5. Delegation chain — verify each proof
	if len(token.Proofs) > 0 {
		return v.verifyChain(ctx, token, required, depth)
	}

	// No proofs — this is a root token. The issuer IS the root authority.
	return nil
}

func (v *Verifier) verifySignature(ctx context.Context, token *Token) error {
	payload, err := token.PayloadBytes()
	if err != nil {
		return fmt.Errorf("failed to encode payload: %w", err)
	}

	for _, signer := range v.signers {
		if signer.CanVerify(token.Issuer) {
			valid, err := signer.Verify(ctx, payload, token.Signature, token.Issuer)
			if err != nil {
				return fmt.Errorf("signer error for %s: %w", token.Issuer, err)
			}
			if !valid {
				return fmt.Errorf("invalid signature for issuer %s", token.Issuer)
			}
			return nil
		}
	}

	return fmt.Errorf("no signer available for DID: %s", token.Issuer)
}

func (v *Verifier) verifyChain(ctx context.Context, token *Token, required Capability, depth int) error {
	for _, proofCID := range token.Proofs {
		// Resolve the proof token
		proof, err := v.resolver.Resolve(ctx, proofCID)
		if err != nil {
			return fmt.Errorf("failed to resolve proof %x: %w", proofCID[:8], err)
		}
		if proof == nil {
			return fmt.Errorf("proof not found: %x", proofCID[:8])
		}

		// The proof's audience must be this token's issuer (delegation chain)
		if proof.Audience != token.Issuer {
			return fmt.Errorf("proof audience %q != token issuer %q (broken chain)", proof.Audience, token.Issuer)
		}

		// Attenuation check — this token's capability must be narrower than the proof's
		proofCap := proof.Capability()
		tokenCap := token.Capability()
		if !tokenCap.Attenuates(proofCap) {
			return fmt.Errorf("token capability %q does not attenuate proof capability %q", tokenCap.Command, proofCap.Command)
		}

		// Recursively verify the proof token
		if err := v.verifyToken(ctx, proof, required, depth+1); err != nil {
			return fmt.Errorf("proof chain broken: %w", err)
		}
	}

	return nil
}
