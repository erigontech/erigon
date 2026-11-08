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
	"crypto/ecdsa"
	"fmt"
	"strings"

	"github.com/erigontech/erigon/common/crypto"
)

// EOASigner verifies UCAN token signatures from Ethereum EOA accounts.
// It uses ERC-191 personal_sign format: keccak256("\x19Ethereum Signed Message:\n32" + hash).
type EOASigner struct{}

// NewEOASigner creates an EOA signature verifier.
func NewEOASigner() *EOASigner {
	return &EOASigner{}
}

// CanVerify returns true for did:pkh DIDs (Ethereum addresses).
func (s *EOASigner) CanVerify(did string) bool {
	return strings.HasPrefix(did, "did:pkh:eip155:")
}

// Verify checks that the signature over payload was produced by the address
// in the issuer DID. Uses ERC-191 personal_sign prefix.
func (s *EOASigner) Verify(_ context.Context, payload []byte, signature []byte, issuerDID string) (bool, error) {
	did, err := ParseDID(issuerDID)
	if err != nil {
		return false, fmt.Errorf("invalid issuer DID: %w", err)
	}
	if !did.IsPKH() {
		return false, fmt.Errorf("EOASigner only handles did:pkh, got %q", did.Method)
	}

	expectedAddr := did.Address()

	// ERC-191 personal_sign: prefix the hash
	hash := crypto.Keccak256(payload)
	prefixed := personalSignHash(hash)

	// Recover the public key from the signature
	if len(signature) != 65 {
		return false, fmt.Errorf("invalid signature length: %d (expected 65)", len(signature))
	}

	// Normalize V value (27/28 → 0/1 for crypto.SigToPub)
	sig := make([]byte, 65)
	copy(sig, signature)
	if sig[64] >= 27 {
		sig[64] -= 27
	}

	pubKey, err := crypto.SigToPub(prefixed, sig)
	if err != nil {
		return false, fmt.Errorf("signature recovery failed: %w", err)
	}

	recoveredAddr := crypto.PubkeyToAddress(*pubKey)
	return recoveredAddr == expectedAddr, nil
}

// Sign produces an ERC-191 personal_sign signature over the payload.
// Used for creating UCAN tokens (delegation).
func (s *EOASigner) Sign(payload []byte, key *ecdsa.PrivateKey) ([]byte, error) {
	hash := crypto.Keccak256(payload)
	prefixed := personalSignHash(hash)

	sig, err := crypto.Sign(prefixed, key)
	if err != nil {
		return nil, fmt.Errorf("signing failed: %w", err)
	}

	// Normalize V to 27/28 (Ethereum convention)
	if sig[64] < 27 {
		sig[64] += 27
	}

	return sig, nil
}

// personalSignHash applies the ERC-191 personal_sign prefix to a 32-byte hash.
func personalSignHash(hash []byte) []byte {
	prefix := fmt.Sprintf("\x19Ethereum Signed Message:\n%d", len(hash))
	return crypto.Keccak256([]byte(prefix), hash)
}
