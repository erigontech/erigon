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
)

// Store persists UCAN tokens and revocation state.
// The initial implementation uses MDBX (core component, always available).
type Store interface {
	// PutToken stores a token by its CID.
	PutToken(ctx context.Context, cid CID, token *Token) error

	// GetToken retrieves a token by its CID. Returns nil, nil if not found.
	GetToken(ctx context.Context, cid CID) (*Token, error)

	// IsRevoked checks if a token CID has been revoked, either directly
	// or via a bulk revocation (all tokens from issuer before timestamp).
	IsRevoked(ctx context.Context, cid CID, issuerDID string, issuedAt uint64) (bool, error)

	// Revoke marks a specific token as revoked.
	Revoke(ctx context.Context, cid CID) error

	// RevokeAllBefore revokes all tokens from the given issuer issued before timestamp.
	// Used for key compromise recovery.
	RevokeAllBefore(ctx context.Context, issuerDID string, timestamp uint64) error
}

// MemoryStore is an in-memory Store implementation for testing.
type MemoryStore struct {
	tokens       map[CID]*Token
	revoked      map[CID]bool
	revokedAfter map[string]uint64 // issuerDID → timestamp
}

// NewMemoryStore creates an in-memory token store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		tokens:       make(map[CID]*Token),
		revoked:      make(map[CID]bool),
		revokedAfter: make(map[string]uint64),
	}
}

func (s *MemoryStore) PutToken(_ context.Context, cid CID, token *Token) error {
	s.tokens[cid] = token
	return nil
}

func (s *MemoryStore) GetToken(_ context.Context, cid CID) (*Token, error) {
	return s.tokens[cid], nil
}

func (s *MemoryStore) IsRevoked(_ context.Context, cid CID, issuerDID string, issuedAt uint64) (bool, error) {
	if s.revoked[cid] {
		return true, nil
	}
	if after, ok := s.revokedAfter[issuerDID]; ok && issuedAt < after {
		return true, nil
	}
	return false, nil
}

func (s *MemoryStore) Revoke(_ context.Context, cid CID) error {
	s.revoked[cid] = true
	return nil
}

func (s *MemoryStore) RevokeAllBefore(_ context.Context, issuerDID string, timestamp uint64) error {
	s.revokedAfter[issuerDID] = timestamp
	return nil
}

// StoreResolver adapts a Store into a TokenResolver for the Verifier.
type StoreResolver struct {
	store Store
}

// NewStoreResolver wraps a Store as a TokenResolver.
func NewStoreResolver(store Store) *StoreResolver {
	return &StoreResolver{store: store}
}

func (r *StoreResolver) Resolve(ctx context.Context, cid CID) (*Token, error) {
	return r.store.GetToken(ctx, cid)
}
