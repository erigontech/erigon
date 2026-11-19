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
	"math"
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

// Window bounds the distance between the blocks of any two announcements the
// store keeps: a put drops those of the blocks more than Window from its own,
// so that the store holds at most Window+1 however long settlement stalls.
// App. E.2 sets no retention bound.
const Window = 1 << 19

// Store keeps announcements by block number, the block hash checked on read.
type Store struct {
	db     kv.RwDB
	window uint64
}

// Open opens the store in dir/announcements.
func Open(ctx context.Context, dir string, logger log.Logger) (*Store, error) {
	return OpenWindow(ctx, dir, Window, logger)
}

// OpenWindow opens the store in dir/announcements with a window of window
// blocks.
func OpenWindow(ctx context.Context, dir string, window uint64, logger log.Logger) (*Store, error) {
	// The map holds about 1.5 million announcements, nearly three times
	// Window+1; the default 2 TB map exhausts the race detector's address
	// space.
	return open(ctx, dir, 256*datasize.MB, window, logger)
}

func open(ctx context.Context, dir string, size datasize.ByteSize, window uint64, logger log.Logger) (*Store, error) {
	db, err := mdbx.New(dbcfg.TemporaryDB, logger).Path(filepath.Join(dir, "announcements")).
		GrowthStep(datasize.MB).MapSize(size).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.TableCfg{table: {}} }).
		Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open announcement store: %w", err)
	}
	return &Store{db: db, window: window}, nil
}

// Close closes the store.
func (s *Store) Close() { s.db.Close() }

func key(number uint64) []byte { return binary.BigEndian.AppendUint64(nil, number) }

// Floor is the lowest block whose announcement a put of that of block number
// keeps.
func (s *Store) Floor(number uint64) uint64 { return number - min(number, s.window) }

// Put keeps the announcement env of block number with hash, replacing that
// of another block at that number, and drops those of the blocks more than
// the window from number.
func (s *Store) Put(ctx context.Context, number uint64, hash common.Hash, env []byte) error {
	return s.db.Update(ctx, func(tx kv.RwTx) error {
		if err := drop(tx, s.Floor(number), number+min(s.window, math.MaxUint64-number)); err != nil {
			return err
		}
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
	return s.db.Update(ctx, func(tx kv.RwTx) error { return drop(tx, number, math.MaxUint64) })
}

// drop deletes the announcements of the blocks below lo and above hi.
func drop(tx kv.RwTx, lo, hi uint64) error {
	c, err := tx.RwCursor(table)
	if err != nil {
		return err
	}
	defer c.Close()
	for k, _, err := c.First(); ; k, _, err = c.Next() {
		if err != nil {
			return err
		}
		if k == nil || binary.BigEndian.Uint64(k) >= lo {
			break
		}
		if err := c.DeleteCurrent(); err != nil {
			return err
		}
	}
	if hi == math.MaxUint64 {
		return nil
	}
	for k, _, err := c.Seek(key(hi + 1)); ; k, _, err = c.Next() {
		if err != nil || k == nil {
			return err
		}
		if err := c.DeleteCurrent(); err != nil {
			return err
		}
	}
}

// keeper keeps announcements by block number.
type keeper interface {
	Put(ctx context.Context, number uint64, hash common.Hash, env []byte) error
	Get(ctx context.Context, number uint64, hash common.Hash) ([]byte, error)
	Prune(ctx context.Context, number uint64) error
	Floor(number uint64) uint64
}

// Chain is the chain whose blocks a Signer announces.
type Chain interface {
	// CanonicalHash returns the hash of the canonical block number.
	CanonicalHash(ctx context.Context, number uint64) (common.Hash, bool, error)
	// Settled returns the block the latest settlement ended at.
	Settled() uint64
}

// Signer signs the blocks a sequencer produces into a Store, keeping the
// announcements L1 finality does not cover yet, within the store's window.
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

// Record drops the announcements L1 finality passes, then signs and keeps
// that of block h and those of the canonical blocks between the settled one
// and h, within the store's window, that lack one. Only h's failure is
// returned. Record must have one caller at a time.
func (s *Signer) Record(ctx context.Context, h *types.Header) error {
	if err := s.store.Prune(ctx, s.finalized()); err != nil {
		s.logger.Warn("[eez] finalized announcements not pruned", "event", "eez.sequencer.announcement.prune_failed", "err", err)
	}
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
// that lack an announcement; derivation writes none of them. It starts at the
// store's floor for number, as a put below it would drop number's.
func (s *Signer) backfill(ctx context.Context, number uint64) error {
	for n := max(s.chain.Settled()+1, s.store.Floor(number)); n < number; n++ {
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
