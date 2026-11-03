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

package jsonrpc

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/db/rawdb"
)

func TestIndexedStateReadFollowsRemoteRenewal(t *testing.T) {
	for _, endpoint := range []string{"account", "storage"} {
		for _, phase := range []string{"bounds", "close", "pruning", "bounds_error"} {
			t.Run(endpoint+"/"+phase, func(t *testing.T) {
				apis, chainInfo := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
				apis.debug._txNumReader = rawdbv3.TxNums
				ref := chainInfo.old

				synctest.Test(t, func(t *testing.T) {
					ctx := t.Context()
					db, _ := remoteHistoryDB(t, apis.debug.db)
					tx, err := db.BeginTemporalRo(ctx) //nolint:gocritic // Setup defers rollback; the endpoint takes ownership below.
					require.NoError(t, err)
					handedOff := false
					defer func() {
						if !handedOff {
							tx.Rollback()
						}
					}()
					oldViewID := tx.ViewID()
					minTxNum, err := rawdbv3.TxNums.Min(ctx, tx, ref.num)
					require.NoError(t, err)
					view := &renewingStateReadTx{TemporalTx: tx, phase: phase, renew: func() {
						require.NoError(t, apis.rwDB.Update(ctx, func(tx kv.RwTx) error {
							if phase == "bounds_error" {
								// A shorter replacement chain has valid bounds, but combining
								// its maximum with the old minimum produces an empty interval.
								require.Greater(t, minTxNum, uint64(2))
								if err := tx.Put(kv.MaxTxNum, hexutil.EncodeTs(ref.num-1), hexutil.EncodeTs(minTxNum-2)); err != nil {
									return err
								}
								if err := tx.Put(kv.MaxTxNum, hexutil.EncodeTs(ref.num), hexutil.EncodeTs(minTxNum)); err != nil {
									return err
								}
							}
							return rawdb.WriteCanonicalHash(tx, common.Hash{1}, ref.num)
						}))
						time.Sleep(remotedbserver.MaxTxTTL + time.Nanosecond)
					}}
					apis.debug.db = historyViewDB{TemporalRoDB: db, tx: view}
					handedOff = true
					switch endpoint {
					case "account":
						_, err = apis.debug.AccountAt(ctx, ref.hash, 0, testAddr)
					case "storage":
						_, err = apis.debug.StorageRangeAt(ctx, ref.hash, 0, testAddr, nil, 10)
					}
					require.Nil(t, view.renew, "the requested renewal point must be reached")
					require.NotEqual(t, oldViewID, tx.ViewID())
					require.ErrorContains(t, err, "canonical", "a renewed read must recheck the requested block hash")
				})
			})
		}
	}
}

type renewingStateReadTx struct {
	kv.TemporalTx
	phase        string
	renew        func()
	boundsClosed bool
}

func (tx *renewingStateReadTx) trigger(phase string) {
	if tx.phase == phase && tx.renew != nil {
		renew := tx.renew
		tx.renew = nil
		renew()
	}
}

func (tx *renewingStateReadTx) Cursor(bucket string) (kv.Cursor, error) {
	c, err := tx.TemporalTx.Cursor(bucket) //nolint:gocritic // Ownership passes to the caller.
	if err != nil || bucket != kv.MaxTxNum {
		return c, err
	}
	return &renewingBoundsCursor{Cursor: c, tx: tx}, nil
}

func (tx *renewingStateReadTx) GetOne(bucket string, key []byte) ([]byte, error) {
	if tx.boundsClosed && bucket == kv.LastForkchoice {
		tx.trigger("pruning")
	}
	return tx.TemporalTx.GetOne(bucket, key)
}

type renewingBoundsCursor struct {
	kv.Cursor
	tx    *renewingStateReadTx
	seeks int
}

func (c *renewingBoundsCursor) SeekExact(key []byte) ([]byte, []byte, error) {
	if c.seeks == 1 {
		c.tx.trigger("bounds")
		c.tx.trigger("bounds_error")
	}
	c.seeks++
	return c.Cursor.SeekExact(key)
}

func (c *renewingBoundsCursor) Close() {
	c.tx.trigger("close")
	c.Cursor.Close()
	c.tx.boundsClosed = true
}
