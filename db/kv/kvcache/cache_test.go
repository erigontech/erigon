// Copyright 2021 The Erigon Authors
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

package kvcache

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

func TestEvictionInUnexpectedOrder(t *testing.T) {
	// Order: View - 2, OnNewBlock - 2, View - 5, View - 6, OnNewBlock - 3, OnNewBlock - 4, View - 5, OnNewBlock - 5, OnNewBlock - 100
	require := require.New(t)
	cfg := DefaultCoherentConfig
	cfg.CacheSize = 3
	cfg.NewBlockWait = 0
	c := New(cfg)
	c.selectOrCreateRoot(2)
	require.Len(c.roots, 1)
	require.Zero(int(c.latestStateVersionID))

	c.add([]byte{1}, nil, c.roots[2], 2)
	require.Zero(c.stateEvict.Len())

	c.advanceRoot(2)
	require.Len(c.roots, 1)
	require.Equal(2, int(c.latestStateVersionID))

	c.add([]byte{1}, nil, c.roots[2], 2)
	require.Equal(1, c.stateEvict.Len())

	c.selectOrCreateRoot(5)
	require.Len(c.roots, 2)
	require.Equal(2, int(c.latestStateVersionID))

	c.add([]byte{2}, nil, c.roots[5], 5) // not added to evict list
	require.Equal(1, c.stateEvict.Len())
	c.add([]byte{2}, nil, c.roots[2], 2) // added to evict list, because it's latest view
	require.Equal(2, c.stateEvict.Len())

	c.selectOrCreateRoot(6)
	require.Len(c.roots, 3)
	require.Equal(2, int(c.latestStateVersionID))

	c.advanceRoot(3)
	require.Len(c.roots, 4)
	require.Equal(3, int(c.latestStateVersionID))

	c.advanceRoot(4)
	require.Len(c.roots, 5)
	require.Equal(4, int(c.latestStateVersionID))

	c.selectOrCreateRoot(5)
	require.Len(c.roots, 5)
	require.Equal(4, int(c.latestStateVersionID))

	c.advanceRoot(5)
	require.Len(c.roots, 5)
	require.Equal(5, int(c.latestStateVersionID))

	c.advanceRoot(100)
	require.Len(c.roots, 6)
	require.Equal(100, int(c.latestStateVersionID))

	//c.add([]byte{1}, nil, c.roots[2], 2)
	require.Equal(0, c.latestStateView.cache.Len())
	require.Equal(0, c.stateEvict.Len())
}

func TestEviction(t *testing.T) {
	require, ctx := require.New(t), t.Context()
	cfg := DefaultCoherentConfig
	cfg.CacheSize = 21
	cfg.NewBlockWait = 0
	c := New(cfg)

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	k1, k2 := [20]byte{1}, [20]byte{2}

	var id uint64
	_ = db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		id = tx.ViewID()
		var versionID [8]byte
		binary.BigEndian.PutUint64(versionID[:], id)
		_ = tx.Put(kv.Sequence, kv.PlainStateVersion, versionID[:])
		cacheView, _ := c.View(ctx, tx)
		view := cacheView.(*CoherentView)
		_, _ = c.Get(k1[:], tx, view.stateVersionID)
		_, _ = c.Get([]byte{1}, tx, view.stateVersionID)
		_, _ = c.Get([]byte{2}, tx, view.stateVersionID)
		_, _ = c.Get([]byte{3}, tx, view.stateVersionID)
		//require.Equal(c.roots[c.latestViewID].cache.Len(), c.stateEvict.Len())
		return nil
	})
	require.Equal(0, c.stateEvict.Len())
	//require.Equal(c.roots[c.latestViewID].cache.Len(), c.stateEvict.Len())
	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId: id + 1,
		ChangeBatch: []*remoteproto.StateChange{
			{
				Direction: remoteproto.Direction_FORWARD,
				Changes: []*remoteproto.AccountChange{{
					Action:  remoteproto.Action_UPSERT,
					Address: gointerfaces.ConvertAddressToH160(k1),
					Data:    []byte{2},
				}},
			},
		},
	})
	require.Equal(21, c.stateEvict.Size())
	require.Equal(1, c.stateEvict.Len())
	require.Equal(c.roots[c.latestStateVersionID].cache.Len(), c.stateEvict.Len())
	_ = db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		id = tx.ViewID()
		cacheView, _ := c.View(ctx, tx)
		var versionID [8]byte
		binary.BigEndian.PutUint64(versionID[:], id)
		_ = tx.Put(kv.Sequence, kv.PlainStateVersion, versionID[:])
		view := cacheView.(*CoherentView)
		_, _ = c.Get(k1[:], tx, view.stateVersionID)
		_, _ = c.Get(k2[:], tx, view.stateVersionID)
		_, _ = c.Get([]byte{5}, tx, view.stateVersionID)
		_, _ = c.Get([]byte{6}, tx, view.stateVersionID)
		return nil
	})
	require.Equal(c.roots[c.latestStateVersionID].cache.Len(), c.stateEvict.Len())
	require.Equal(int(cfg.CacheSize.Bytes()), c.stateEvict.Size())
}

// Canonical roots must start from their own batch only: the state-change
// producers do not announce every mutation (e.g. account deletions), so
// entries inherited from the previous root could stay stale forever.
func TestCanonicalRootsStartFresh(t *testing.T) {
	require := require.New(t)
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

	k1 := [20]byte{1}
	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId: 2,
		ChangeBatch: []*remoteproto.StateChange{{
			Direction: remoteproto.Direction_FORWARD,
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(k1),
				Data:    []byte{1},
			}},
		}},
	})
	require.Equal(1, c.roots[2].cache.Len())

	c.OnNewBlock(&remoteproto.StateChangeBatch{StateVersionId: 3})
	require.Zero(c.roots[3].cache.Len())
}

func TestRetainedRootsShareCacheBudgets(t *testing.T) {
	require := require.New(t)
	cfg := DefaultCoherentConfig
	cfg.CacheSize = 21
	cfg.CodeCacheSize = 21
	cfg.NewBlockWait = 0
	c := New(cfg)

	addVersion := func(version uint64, addr [20]byte) {
		c.OnNewBlock(&remoteproto.StateChangeBatch{
			StateVersionId: version,
			ChangeBatch: []*remoteproto.StateChange{{
				Direction: remoteproto.Direction_FORWARD,
				Changes: []*remoteproto.AccountChange{{
					Action:  remoteproto.Action_UPSERT_CODE,
					Address: gointerfaces.ConvertAddressToH160(addr),
					Data:    []byte{byte(version)},
					Code:    []byte{byte(version)},
				}},
			}},
		})
	}

	addVersion(1, [20]byte{1})
	addVersion(2, [20]byte{2})
	require.Len(c.roots, 2)

	var stateSize, codeSize int
	for _, root := range c.roots {
		root.cache.Scan(func(element *Element) bool {
			stateSize += element.Size()
			return true
		})
		root.codeCache.Scan(func(element *Element) bool {
			codeSize += element.Size()
			return true
		})
	}
	require.LessOrEqual(stateSize, int(cfg.CacheSize.Bytes()))
	require.LessOrEqual(codeSize, int(cfg.CodeCacheSize.Bytes()))
}

// Batch-fed storage entries must be stored under the key shape readers use:
// address+location (see state.CachedReader3.ReadAccountStorage).
func TestOnNewBlockStorageKeysMatchReaders(t *testing.T) {
	require, ctx := require.New(t), t.Context()
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))

	addr, loc := [20]byte{1}, [32]byte{2}
	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId: 2,
		ChangeBatch: []*remoteproto.StateChange{{
			Direction: remoteproto.Direction_FORWARD,
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_STORAGE,
				Address: gointerfaces.ConvertAddressToH160(addr),
				StorageChanges: []*remoteproto.StorageChange{{
					Location: gointerfaces.ConvertHashToH256(loc),
					Data:     []byte{42},
				}},
			}},
		}},
	})

	err := db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		k := append(addr[:], loc[:]...)
		v, err := c.Get(k, tx, 2)
		require.NoError(err)
		require.Equal([]byte{42}, v)
		return nil
	})
	require.NoError(err)
}

// Batch-fed code entries must be stored under the key shape readers use:
// the account address, which is the E3 CodeDomain key
// (see state.CachedReader3.ReadAccountCode).
func TestOnNewBlockCodeKeysMatchReaders(t *testing.T) {
	require, ctx := require.New(t), t.Context()
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))

	addr := [20]byte{1}
	code := []byte{0x60, 0x60}
	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId: 2,
		ChangeBatch: []*remoteproto.StateChange{{
			Direction: remoteproto.Direction_FORWARD,
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_CODE,
				Address: gointerfaces.ConvertAddressToH160(addr),
				Code:    code,
			}},
		}},
	})

	err := db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		v, err := c.GetCode(addr[:], tx, 2)
		require.NoError(err)
		require.Equal(code, v)
		return nil
	})
	require.NoError(err)
}

// A cache hit on the code domain must refresh the entry's position in the
// code eviction list — otherwise hot code is evicted in insertion order.
func TestCodeHitRefreshesCodeEvictLRU(t *testing.T) {
	require, ctx := require.New(t), t.Context()
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))

	addr1, addr2 := [20]byte{1}, [20]byte{2}
	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId: 2,
		ChangeBatch: []*remoteproto.StateChange{{
			Direction: remoteproto.Direction_FORWARD,
			Changes: []*remoteproto.AccountChange{
				{Action: remoteproto.Action_CODE, Address: gointerfaces.ConvertAddressToH160(addr1), Code: []byte{1}},
				{Action: remoteproto.Action_CODE, Address: gointerfaces.ConvertAddressToH160(addr2), Code: []byte{2}},
			},
		}},
	})
	require.Equal(addr1[:], c.codeEvict.Oldest().K)

	err := db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		_, err := c.GetCode(addr1[:], tx, 2)
		return err
	})
	require.NoError(err)
	require.Equal(addr2[:], c.codeEvict.Oldest().K)
}

// A request whose cache view outlives KeepViews state-version advances (e.g. a
// long eth_call) must fall back to its own tx snapshot, not error out.
func TestViewSurvivesRootEviction(t *testing.T) {
	require, ctx := require.New(t), t.Context()
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	k1 := [20]byte{1}

	err := db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		cacheView, err := c.View(ctx, tx)
		require.NoError(err)
		view := cacheView.(*CoherentView)

		for i := uint64(1); i <= cfg.KeepViews+2; i++ {
			c.OnNewBlock(&remoteproto.StateChangeBatch{StateVersionId: view.stateVersionID + i})
		}
		_, rootAlive := c.roots[view.stateVersionID]
		require.False(rootAlive, "root must be evicted for this test to be meaningful")

		v, err := c.Get(k1[:], tx, view.stateVersionID)
		require.NoError(err)
		require.Empty(v)

		code, err := c.GetCode(k1[:], tx, view.stateVersionID)
		require.NoError(err)
		require.Empty(code)
		return nil
	})
	require.NoError(err)
}

// Reads through a view whose version is not the latest (pre-commit window, or
// no state-change stream at all) bypass eviction accounting, so they must not
// grow the root either — otherwise memory is unbounded.
func TestNonLatestViewReadsAreNotCached(t *testing.T) {
	require, ctx := require.New(t), t.Context()
	cfg := DefaultCoherentConfig
	cfg.NewBlockWait = 0
	c := New(cfg)

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	k1 := [20]byte{1}
	acc := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(11), CodeHash: accounts.EmptyCodeHash}
	accEnc := accounts.SerialiseV3(&acc)

	err := db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		d, err := execctx.NewSharedDomains(ctx, tx, log.New())
		if err != nil {
			return err
		}
		defer d.Close()
		if err := d.DomainPut(kv.AccountsDomain, tx, k1[:], accEnc, 0, nil); err != nil {
			return err
		}
		return d.Flush(ctx, tx)
	})
	require.NoError(err)

	err = db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		cacheView, err := c.View(ctx, tx)
		require.NoError(err)
		view := cacheView.(*CoherentView)
		require.NotEqual(c.latestStateVersionID, view.stateVersionID)

		v, err := c.Get(k1[:], tx, view.stateVersionID)
		require.NoError(err)
		require.Equal(accEnc, v)

		require.Zero(c.roots[view.stateVersionID].cache.Len())
		require.Zero(c.stateEvict.Len())
		return nil
	})
	require.NoError(err)
}

func TestAPI(t *testing.T) {
	require := require.New(t)

	// Create a context with timeout for the entire test
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	c := New(DefaultCoherentConfig)
	k1, k2 := [20]byte{1}, [20]byte{2}
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))

	acc := accounts.Account{
		Nonce:       1,
		Balance:     *uint256.NewInt(11),
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 2,
	}
	account1Enc := accounts.SerialiseV3(&acc)
	acc.Incarnation = 3
	account2Enc := accounts.SerialiseV3(&acc)
	acc.Incarnation = 5
	account4Enc := accounts.SerialiseV3(&acc)

	get := func(key [20]byte, expectTxnID uint64) (res [1]chan []byte) {
		wg := sync.WaitGroup{}
		for i := range len(res) {
			wg.Add(1)
			res[i] = make(chan []byte, 1) // Buffered channel to prevent deadlock
			go func(out chan []byte) {
				defer wg.Done()
				err := db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
					if expectTxnID != tx.ViewID() {
						panic(fmt.Sprintf("epxected: %d, got: %d", expectTxnID, tx.ViewID()))
					}
					cacheView, err := c.View(ctx, tx)
					if err != nil {
						panic(fmt.Sprintf("View error: %v", err))
					}
					view := cacheView.(*CoherentView)
					v, err := c.Get(key[:], tx, view.stateVersionID)
					if err != nil {
						panic(fmt.Sprintf("Get error: %v", err))
					}

					select {
					case out <- common.Copy(v):
					case <-ctx.Done():
						panic("Context done while sending result")
					}
					return nil
				})
				if err != nil {
					panic(fmt.Sprintf("Database error: %v", err))
				}
			}(res[i])
		}
		wg.Wait() // ensure that all goroutines started their transactions
		return res
	}

	put := func(k, v []byte) uint64 {
		var txID uint64
		err := db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
			txID = tx.ViewID()
			d, err := execctx.NewSharedDomains(ctx, tx, log.New())
			if err != nil {
				return err
			}
			defer d.Close()
			txNum := uint64(0)
			if err := d.DomainPut(kv.AccountsDomain, tx, k, v, txNum, nil); err != nil {
				return err
			}
			return d.Flush(ctx, tx)
		})
		require.NoError(err)
		return txID
	}
	// block 1 - represents existing state (no notifications about this data will come to client)
	txID1 := put(k2[:], account1Enc)

	wg := sync.WaitGroup{}

	res1, res2 := get(k1, txID1), get(k2, txID1) // will return immediately
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range res1 {
			select {
			case v := <-res1[i]:
				if v != nil {
					panic(fmt.Sprintf("expected nil, got: %x", v))
				}
			case <-ctx.Done():
				panic("Context done while checking res1")
			}
		}
		for i := range res2 {
			select {
			case v := <-res2[i]:
				if !bytes.Equal(account1Enc, v) {
					panic(fmt.Sprintf("expected: %x, got: %x", account1Enc, v))
				}
			case <-ctx.Done():
				panic("Context done while checking res2")
			}
		}

		fmt.Printf("done1: \n")
	}()

	txID2 := put(k1[:], account2Enc)
	fmt.Printf("-----1 %d, %d\n", txID1, txID2)
	res3, res4 := get(k1, txID2), get(k2, txID2) // will see View of transaction 2
	txID3 := put(k1[:], account2Enc)             // even if core already on block 3

	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId:      txID2,
		PendingBlockBaseFee: 1,
		ChangeBatch: []*remoteproto.StateChange{
			{
				Direction:   remoteproto.Direction_FORWARD,
				BlockHeight: 2,
				BlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
				Changes: []*remoteproto.AccountChange{{
					Action:  remoteproto.Action_UPSERT,
					Address: gointerfaces.ConvertAddressToH160(k1),
					Data:    account2Enc,
				}},
			},
		},
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range res3 {
			select {
			case v := <-res3[i]:
				if !bytes.Equal(account2Enc, v) {
					panic(fmt.Sprintf("expected: %x, got: %x", account2Enc, v))
				}
			case <-ctx.Done():
				panic("Context done while checking res3")
			}
		}
		for i := range res4 {
			select {
			case v := <-res4[i]:
				if !bytes.Equal(account1Enc, v) {
					panic(fmt.Sprintf("expected: %x, got: %x", account1Enc, v))
				}
			case <-ctx.Done():
				panic("Context done while checking res4")
			}
		}
		fmt.Printf("done2: \n")
	}()
	fmt.Printf("-----2\n")

	res5, res6 := get(k1, txID3), get(k2, txID3) // will see View of transaction 3, even if notification has not enough changes
	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId:      txID3,
		PendingBlockBaseFee: 1,
		ChangeBatch: []*remoteproto.StateChange{
			{
				Direction:   remoteproto.Direction_FORWARD,
				BlockHeight: 3,
				BlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
				Changes: []*remoteproto.AccountChange{{
					Action:  remoteproto.Action_UPSERT,
					Address: gointerfaces.ConvertAddressToH160(k1),
					Data:    account2Enc,
				}},
			},
		},
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range res5 {
			select {
			case v := <-res5[i]:
				if !bytes.Equal(account2Enc, v) {
					panic(fmt.Sprintf("expected: %x, got: %x", account2Enc, v))
				}
			case <-ctx.Done():
				panic("Context done while checking res5")
			}
		}
		fmt.Printf("-----21\n")

		for i := range res6 {
			select {
			case v := <-res6[i]:
				if !bytes.Equal(account1Enc, v) {
					panic(fmt.Sprintf("expected: %x, got: %x", account1Enc, v))
				}
			case <-ctx.Done():
				panic("Context done while checking res6")
			}
		}
		fmt.Printf("done3: \n")
	}()
	fmt.Printf("-----3\n")
	txID4 := put(k1[:], account2Enc)

	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId:      txID4,
		PendingBlockBaseFee: 1,
		ChangeBatch: []*remoteproto.StateChange{
			{
				Direction:   remoteproto.Direction_UNWIND,
				BlockHeight: 2,
				BlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
				Changes: []*remoteproto.AccountChange{{
					Action:  remoteproto.Action_UPSERT,
					Address: gointerfaces.ConvertAddressToH160(k1),
					Data:    account4Enc,
				}},
			},
		},
	})
	fmt.Printf("-----4\n")
	txID5 := put(k1[:], account4Enc) // reorg to new chain
	c.OnNewBlock(&remoteproto.StateChangeBatch{
		StateVersionId:      txID5,
		PendingBlockBaseFee: 1,
		ChangeBatch: []*remoteproto.StateChange{
			{
				Direction:   remoteproto.Direction_FORWARD,
				BlockHeight: 3,
				BlockHash:   gointerfaces.ConvertHashToH256([32]byte{2}),
				Changes: []*remoteproto.AccountChange{{
					Action:  remoteproto.Action_UPSERT,
					Address: gointerfaces.ConvertAddressToH160(k1),
					Data:    account4Enc,
				}},
			},
		},
	})
	fmt.Printf("-----5\n")

	res7, res8 := get(k1, txID5), get(k2, txID5) // will see View of transaction 3, even if notification has not enough changes

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range res7 {
			select {
			case v := <-res7[i]:
				if !bytes.Equal(account4Enc, v) {
					panic(fmt.Sprintf("expected: %x, got: %x", account4Enc, v))
				}
			case <-ctx.Done():
				panic("Context done while checking res7")
			}
		}
		for i := range res8 {
			select {
			case v := <-res8[i]:
				if !bytes.Equal(account1Enc, v) {
					panic(fmt.Sprintf("expected: %x, got: %x", account1Enc, v))
				}
			case <-ctx.Done():
				panic("Context done while checking res8")
			}
		}
		fmt.Printf("done4: \n")
	}()
	// TODO: Used in other places too cant modify this.
	// err := db.View(t.Context(), func(tx kv.Tx) error {
	// 	_, err := AssertCheckValues(t.Context(), tx, c)
	// 	require.NoError(err)
	// 	return nil
	// })
	// require.NoError(err)

	// Wait for all goroutines to complete or timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines completed successfully
	case <-ctx.Done():
		t.Error("Test timed out waiting for goroutines to complete")
	}
}

func TestCode(t *testing.T) {
	require, ctx := require.New(t), t.Context()
	c := New(DefaultCoherentConfig)
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	k1 := [20]byte{1}
	code := []byte{0x60, 0x00, 0x60, 0x00}

	require.NoError(db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		d, err := execctx.NewSharedDomains(ctx, tx, log.New())
		if err != nil {
			return err
		}
		defer d.Close()
		if err := d.DomainPut(kv.CodeDomain, tx, k1[:], code, 0, nil); err != nil {
			return err
		}
		return d.Flush(ctx, tx)
	}))

	require.NoError(db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		cacheView, err := c.View(ctx, tx)
		require.NoError(err)
		view := cacheView.(*CoherentView)

		v, err := c.GetCode(k1[:], tx, view.stateVersionID)
		require.NoError(err)
		require.Equal(code, v)

		// second read is served from the cache
		v, err = c.GetCode(k1[:], tx, view.stateVersionID)
		require.NoError(err)
		require.Equal(code, v)
		return nil
	}))
}
