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

// Package witness keeps the execution witnesses of the blocks a sequencer
// produces until L1 finality covers them.
package witness

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

// table maps a block number to the block's hash and its witness in JSON.
const table = "EezWitness"

// Store keeps witnesses by block number, the block hash checked on read.
type Store struct {
	db kv.RwDB
}

// Open opens the store in dir/witnesses. Its commits are durable: a witness
// needs a pin of its block's parent taken before the block commits, so one
// lost in a crash cannot be rebuilt.
func Open(ctx context.Context, dir string, logger log.Logger) (*Store, error) {
	db, err := mdbx.New(dbcfg.TemporaryDB, logger).Path(filepath.Join(dir, "witnesses")).
		Durable().GrowthStep(16 * datasize.MB).MapSize(datasize.GB).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.TableCfg{table: {}} }).
		Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open witness store: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the store.
func (s *Store) Close() { s.db.Close() }

func key(number uint64) []byte { return binary.BigEndian.AppendUint64(nil, number) }

// Put keeps the witness w of block number with hash, replacing the witness
// of another block at that number.
func (s *Store) Put(ctx context.Context, number uint64, hash common.Hash, w *jsonrpc.ExecutionWitnessResult) error {
	enc, err := json.Marshal(w)
	if err != nil {
		return err
	}
	return s.db.Update(ctx, func(tx kv.RwTx) error {
		return tx.Put(table, key(number), append(hash[:], enc...))
	})
}

// Get returns the witness of block number when the store holds that of the
// block with hash.
func (s *Store) Get(ctx context.Context, number uint64, hash common.Hash) (json.RawMessage, error) {
	var out json.RawMessage
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

// Prune drops the witnesses of the blocks below number.
func (s *Store) Prune(ctx context.Context, number uint64) error {
	return s.db.Update(ctx, func(tx kv.RwTx) error {
		c, err := tx.RwCursor(table)
		if err != nil {
			return err
		}
		defer c.Close()
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			if binary.BigEndian.Uint64(k) >= number {
				return nil
			}
			if err := c.DeleteCurrent(); err != nil {
				return err
			}
		}
		return nil
	})
}

// Capture records the witnesses of the blocks a sequencer produces into a
// Store, keeping those L1 finality does not cover yet.
type Capture struct {
	builder *jsonrpc.HeadWitnesses
	store   *Store
	// finalized returns the finalized block.
	finalized func() uint64
}

// NewCapture returns a capture building with builder into store.
func NewCapture(builder *jsonrpc.HeadWitnesses, store *Store, finalized func() uint64) *Capture {
	return &Capture{builder: builder, store: store, finalized: finalized}
}

// Pin snapshots the head's state for the witness of its child.
func (c *Capture) Pin(ctx context.Context) (interface{ Close() }, error) {
	pin, err := c.builder.Pin(ctx)
	if err != nil {
		return nil, err
	}
	return pin, nil
}

// Record builds and keeps the witness of block h, a child of pin's head.
func (c *Capture) Record(ctx context.Context, pin interface{ Close() }, h *types.Header) error {
	p, ok := pin.(*jsonrpc.WitnessPin)
	if !ok {
		return errors.New("not a witness pin")
	}
	w, err := c.builder.Build(ctx, p, h.Hash())
	if err != nil {
		return err
	}
	// Pruning first lets a full store take witnesses again once finality
	// advances.
	if err := c.store.Prune(ctx, c.finalized()); err != nil {
		return err
	}
	return c.store.Put(ctx, h.Number.Uint64(), h.Hash(), w)
}
