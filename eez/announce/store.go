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

package announce

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/execution/types"
)

// table maps a block number to the block's hash and its announcement.
const table = "EezAnnouncement"

// Store keeps announcements by block number, the block hash checked on read.
type Store struct {
	db kv.RwDB
}

// Open opens the store in dir/announcements.
func Open(ctx context.Context, dir string, logger log.Logger) (*Store, error) {
	// About a million announcements; the default 2 TB map exhausts the race
	// detector's address space.
	db, err := mdbx.New(dbcfg.TemporaryDB, logger).Path(filepath.Join(dir, "announcements")).
		GrowthStep(datasize.MB).MapSize(256 * datasize.MB).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.TableCfg{table: {}} }).
		Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open announcement store: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the store.
func (s *Store) Close() { s.db.Close() }

func key(number uint64) []byte { return binary.BigEndian.AppendUint64(nil, number) }

// Put keeps the announcement env of block number with hash, replacing that
// of another block at that number.
func (s *Store) Put(ctx context.Context, number uint64, hash common.Hash, env []byte) error {
	return s.db.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put(table, key(number), append(hash[:], env...))
	})
}

// Get returns the announcement of block number when the store holds that of
// the block with hash.
func (s *Store) Get(ctx context.Context, number uint64, hash common.Hash) ([]byte, error) {
	var out []byte
	err := s.db.View(ctx, func(tx kv.Tx) error {
		v, err := tx.GetOne(table, key(number))
		if err != nil || len(v) < len(hash) || !bytes.Equal(v[:len(hash)], hash[:]) {
			return err
		}
		out = bytes.Clone(v[len(hash):])
		return nil
	})
	return out, err
}

// Prune drops the announcements of the blocks below number.
func (s *Store) Prune(ctx context.Context, number uint64) error {
	return s.db.Update(ctx, func(tx kv.RwTx) error {
		c, err := tx.RwCursor(table)
		if err != nil {
			return err
		}
		defer c.Close()
		for k, _, err := c.First(); ; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			if k == nil || binary.BigEndian.Uint64(k) >= number {
				return nil
			}
			if err := c.DeleteCurrent(); err != nil {
				return err
			}
		}
	})
}

// keeper keeps announcements by block number.
type keeper interface {
	Put(ctx context.Context, number uint64, hash common.Hash, env []byte) error
	Get(ctx context.Context, number uint64, hash common.Hash) ([]byte, error)
	Prune(ctx context.Context, number uint64) error
}

// Chain is the chain whose blocks a Signer announces.
type Chain interface {
	// CanonicalHash returns the hash of the canonical block number.
	CanonicalHash(ctx context.Context, number uint64) (common.Hash, bool, error)
	// Settled returns the block the latest settlement ended at.
	Settled() uint64
}

// Signer signs the blocks a sequencer produces into a Store, keeping the
// announcements L1 finality does not cover yet.
type Signer struct {
	key     *ecdsa.PrivateKey
	chainID *uint256.Int
	store   keeper
	chain   Chain
	// finalized returns the finalized block.
	finalized func() uint64
	logger    log.Logger
	// gap is set while an unsettled block may lack its announcement: from
	// start, as a previous run may have left some, and after a failure.
	gap bool
}

// NewSigner returns a signer of chain's blocks, on chain id chainID, with key
// into store.
func NewSigner(key *ecdsa.PrivateKey, chainID *uint256.Int, store *Store, chain Chain, finalized func() uint64, logger log.Logger) *Signer {
	return &Signer{key: key, chainID: chainID, store: store, chain: chain, finalized: finalized, logger: logger, gap: true}
}

// Address is the producer identity the signer's announcements recover to.
func (s *Signer) Address() common.Address { return crypto.PubkeyToAddress(s.key.PublicKey) }

// Announce signs and keeps the announcement of block h alone. It reads
// nothing of the chain, which derivation may be replacing.
func (s *Signer) Announce(ctx context.Context, h *types.Header) error {
	return s.sign(ctx, h.Number.Uint64(), h.Hash())
}

// Record signs and keeps the announcement of block h, then those of the
// canonical blocks between the settled one and h that lack one. Only h's
// failure is returned. Record must have one caller at a time.
func (s *Signer) Record(ctx context.Context, h *types.Header) error {
	if err := s.sign(ctx, h.Number.Uint64(), h.Hash()); err != nil {
		s.gap = true
		return err
	}
	if s.gap {
		if err := s.backfill(ctx, h.Number.Uint64()); err != nil {
			s.logger.Error("[eez] unsettled blocks left without an announcement", "event", "eez.sequencer.announcement.backfill_failed", "err", err)
		} else {
			s.gap = false
		}
	}
	if err := s.store.Prune(ctx, s.finalized()); err != nil {
		s.logger.Warn("[eez] finalized announcements not pruned", "event", "eez.sequencer.announcement.prune_failed", "err", err)
	}
	return nil
}

// sign keeps the announcement of block number with hash, unless kept.
func (s *Signer) sign(ctx context.Context, number uint64, hash common.Hash) error {
	if env, err := s.store.Get(ctx, number, hash); err != nil || env != nil {
		return err
	}
	env, err := Sign(s.key, s.chainID, hash)
	if err != nil {
		return err
	}
	return s.store.Put(ctx, number, hash, env)
}

// backfill signs the canonical blocks above the settled one and below number
// that lack an announcement; derivation writes none of them.
func (s *Signer) backfill(ctx context.Context, number uint64) error {
	for n := s.chain.Settled() + 1; n < number; n++ {
		hash, ok, err := s.chain.CanonicalHash(ctx, n)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("no canonical block %d", n)
		}
		if err := s.sign(ctx, n, hash); err != nil {
			return fmt.Errorf("block %d: %w", n, err)
		}
	}
	return nil
}
