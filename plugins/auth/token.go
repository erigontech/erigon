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
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
)

// Token represents a UCAN authorization token.
//
// A token asserts that the issuer delegates the specified capability to the
// audience, optionally referencing proof tokens that form a delegation chain
// back to the root authority (subject).
type Token struct {
	Issuer    string   `json:"iss"`           // DID of the signer
	Audience  string   `json:"aud"`           // DID of the recipient
	Subject   string   `json:"sub,omitempty"` // DID of the root authority
	Command   string   `json:"cmd"`           // capability command path
	Policy    []Policy `json:"pol,omitempty"` // capability constraints
	Nonce     []byte   `json:"nonce"`         // replay protection
	Iat       uint64   `json:"iat"`           // issued-at (unix timestamp, mandatory)
	Exp       uint64   `json:"exp,omitempty"` // expiry (unix timestamp, 0 = no expiry)
	Nbf       uint64   `json:"nbf,omitempty"` // not-before (unix timestamp, 0 = immediate)
	Proofs    []CID    `json:"prf,omitempty"` // CIDs of proof tokens (delegation chain)
	Signature []byte   `json:"sig"`           // signature by issuer
}

// CID is a content identifier for a UCAN token (SHA-256 of the encoded payload).
type CID = common.Hash

// Capability returns the token's capability.
func (t *Token) Capability() Capability {
	return Capability{
		Command: t.Command,
		Policy:  t.Policy,
	}
}

// IsExpired returns true if the token has expired.
func (t *Token) IsExpired() bool {
	if t.Exp == 0 {
		return false // no expiry
	}
	return uint64(time.Now().Unix()) > t.Exp
}

// IsActive returns true if the token is within its validity window.
func (t *Token) IsActive() bool {
	now := uint64(time.Now().Unix())
	if t.Exp > 0 && now > t.Exp {
		return false
	}
	if t.Nbf > 0 && now < t.Nbf {
		return false
	}
	return true
}

// ComputeCID computes the content identifier (SHA-256 hash) of the token's
// payload (everything except the signature).
func (t *Token) ComputeCID() CID {
	payload := tokenPayload{
		Issuer:   t.Issuer,
		Audience: t.Audience,
		Subject:  t.Subject,
		Command:  t.Command,
		Policy:   t.Policy,
		Nonce:    t.Nonce,
		Iat:      t.Iat,
		Exp:      t.Exp,
		Nbf:      t.Nbf,
		Proofs:   t.Proofs,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(data)
}

// tokenPayload is the unsigned portion of a token (for CID computation and signing).
type tokenPayload struct {
	Issuer   string   `json:"iss"`
	Audience string   `json:"aud"`
	Subject  string   `json:"sub,omitempty"`
	Command  string   `json:"cmd"`
	Policy   []Policy `json:"pol,omitempty"`
	Nonce    []byte   `json:"nonce"`
	Iat      uint64   `json:"iat"`
	Exp      uint64   `json:"exp,omitempty"`
	Nbf      uint64   `json:"nbf,omitempty"`
	Proofs   []CID    `json:"prf,omitempty"`
}

// PayloadBytes returns the canonical bytes of the token payload for signing.
func (t *Token) PayloadBytes() ([]byte, error) {
	payload := tokenPayload{
		Issuer:   t.Issuer,
		Audience: t.Audience,
		Subject:  t.Subject,
		Command:  t.Command,
		Policy:   t.Policy,
		Nonce:    t.Nonce,
		Iat:      t.Iat,
		Exp:      t.Exp,
		Nbf:      t.Nbf,
		Proofs:   t.Proofs,
	}
	return json.Marshal(payload)
}

// ValidateStructure checks that the token has all required fields.
// Does NOT verify the signature or delegation chain.
func (t *Token) ValidateStructure() error {
	if t.Issuer == "" {
		return errors.New("token missing issuer (iss)")
	}
	if t.Audience == "" {
		return errors.New("token missing audience (aud)")
	}
	if t.Command == "" {
		return errors.New("token missing command (cmd)")
	}
	if len(t.Nonce) == 0 {
		return errors.New("token missing nonce")
	}
	if t.Iat == 0 {
		return errors.New("token missing issued-at timestamp (iat)")
	}
	if len(t.Signature) == 0 {
		return errors.New("token missing signature (sig)")
	}
	if _, err := ParseDID(t.Issuer); err != nil {
		return fmt.Errorf("invalid issuer DID: %w", err)
	}
	if _, err := ParseDID(t.Audience); err != nil {
		return fmt.Errorf("invalid audience DID: %w", err)
	}
	if t.Subject != "" {
		if _, err := ParseDID(t.Subject); err != nil {
			return fmt.Errorf("invalid subject DID: %w", err)
		}
	}
	return nil
}
