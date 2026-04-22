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
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
)

// MDBX table names for the auth plugin.
const (
	TableTokens       = "AuthTokens"       // CID → JSON-encoded Token
	TableRevoked      = "AuthRevoked"      // CID → empty (presence = revoked)
	TableRevokedAfter = "AuthRevokedAfter" // keccak(issuerDID) → uint64 timestamp
)

// AuthTablesCfg returns the MDBX table configuration for the auth plugin.
func AuthTablesCfg(defaultBuckets kv.TableCfg) kv.TableCfg {
	defaultBuckets[TableTokens] = kv.TableCfgItem{}
	defaultBuckets[TableRevoked] = kv.TableCfgItem{}
	defaultBuckets[TableRevokedAfter] = kv.TableCfgItem{}
	return defaultBuckets
}

// MDBXStore implements Store backed by MDBX.
type MDBXStore struct {
	db kv.RwDB
}

// NewMDBXStore creates a Store backed by an MDBX database.
func NewMDBXStore(db kv.RwDB) *MDBXStore {
	return &MDBXStore{db: db}
}

func (s *MDBXStore) PutToken(ctx context.Context, cid CID, token *Token) error {
	data, err := json.Marshal(token)
	if err != nil {
		return fmt.Errorf("failed to marshal token: %w", err)
	}
	return s.db.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put(TableTokens, cid[:], data)
	})
}

func (s *MDBXStore) GetToken(ctx context.Context, cid CID) (*Token, error) {
	var token *Token
	err := s.db.View(ctx, func(tx kv.Tx) error {
		data, err := tx.GetOne(TableTokens, cid[:])
		if err != nil {
			return err
		}
		if data == nil {
			return nil // not found
		}
		token = &Token{}
		return json.Unmarshal(data, token)
	})
	return token, err
}

func (s *MDBXStore) IsRevoked(ctx context.Context, cid CID, issuerDID string, issuedAt uint64) (bool, error) {
	var revoked bool
	err := s.db.View(ctx, func(tx kv.Tx) error {
		// Check specific token revocation
		data, err := tx.GetOne(TableRevoked, cid[:])
		if err != nil {
			return err
		}
		if data != nil {
			revoked = true
			return nil
		}

		// Check bulk revocation
		issuerHash := crypto.Keccak256([]byte(issuerDID))
		data, err = tx.GetOne(TableRevokedAfter, issuerHash)
		if err != nil {
			return err
		}
		if data != nil && len(data) == 8 {
			timestamp := binary.BigEndian.Uint64(data)
			if issuedAt < timestamp {
				revoked = true
			}
		}
		return nil
	})
	return revoked, err
}

func (s *MDBXStore) Revoke(ctx context.Context, cid CID) error {
	return s.db.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put(TableRevoked, cid[:], []byte{1})
	})
}

func (s *MDBXStore) RevokeAllBefore(ctx context.Context, issuerDID string, timestamp uint64) error {
	issuerHash := crypto.Keccak256([]byte(issuerDID))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], timestamp)
	return s.db.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put(TableRevokedAfter, issuerHash, buf[:])
	})
}
