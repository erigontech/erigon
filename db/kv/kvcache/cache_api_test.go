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

package kvcache_test

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
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

func TestEviction(t *testing.T) {
	require, ctx := require.New(t), context.Background()
	cfg := kvcache.DefaultCoherentConfig
	cfg.CacheSize = 21
	cfg.NewBlockWait = 0
	c := kvcache.New(cfg)

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	k1, k2 := [20]byte{1}, [20]byte{2}

	var id uint64
	_ = db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		_ = tx.Put(kv.PlainState, k1[:], []byte{1})
		id = tx.ViewID()
		var versionID [8]byte
		binary.BigEndian.PutUint64(versionID[:], id)
		_ = tx.Put(kv.Sequence, kv.PlainStateVersion, versionID[:])
		cacheView, _ := c.View(ctx, tx)
		view := cacheView.(*kvcache.CoherentView)
		_, _ = c.Get(k1[:], tx, kvcache.StateVersionID(view))
		_, _ = c.Get([]byte{1}, tx, kvcache.StateVersionID(view))
		_, _ = c.Get([]byte{2}, tx, kvcache.StateVersionID(view))
		_, _ = c.Get([]byte{3}, tx, kvcache.StateVersionID(view))
		//require.Equal(c.roots[c.latestViewID].cache.Len(), c.stateEvict.Len())
		return nil
	})
	require.Equal(0, kvcache.StateEvict(c).Len())
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
	require.Equal(21, kvcache.StateEvict(c).Size())
	require.Equal(1, kvcache.StateEvict(c).Len())
	require.Equal(kvcache.RootCache(kvcache.Roots(c)[kvcache.LatestStateVersionID(c)]).Len(), kvcache.StateEvict(c).Len())
	_ = db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		_ = tx.Put(kv.PlainState, k1[:], []byte{1})
		id = tx.ViewID()
		cacheView, _ := c.View(ctx, tx)
		var versionID [8]byte
		binary.BigEndian.PutUint64(versionID[:], id)
		_ = tx.Put(kv.Sequence, kv.PlainStateVersion, versionID[:])
		view := cacheView.(*kvcache.CoherentView)
		_, _ = c.Get(k1[:], tx, kvcache.StateVersionID(view))
		_, _ = c.Get(k2[:], tx, kvcache.StateVersionID(view))
		_, _ = c.Get([]byte{5}, tx, kvcache.StateVersionID(view))
		_, _ = c.Get([]byte{6}, tx, kvcache.StateVersionID(view))
		return nil
	})
	require.Equal(kvcache.RootCache(kvcache.Roots(c)[kvcache.LatestStateVersionID(c)]).Len(), kvcache.StateEvict(c).Len())
	require.Equal(int(cfg.CacheSize.Bytes()), kvcache.StateEvict(c).Size())
}

func TestAPI(t *testing.T) {
	t.Skip()
	require := require.New(t)

	// Create a context with timeout for the entire test
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c := kvcache.New(kvcache.DefaultCoherentConfig)
	k1, k2 := accounts.InternAddress(common.Address{1}), accounts.InternAddress(common.Address{2})
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))

	acc := accounts.Account{
		Nonce:       1,
		Balance:     *uint256.NewInt(11),
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 2,
	}
	account1 := acc
	account1Enc := accounts.SerialiseV3(&account1)
	acc.Incarnation = 3
	account2 := acc
	account2Enc := accounts.SerialiseV3(&account2)
	acc.Incarnation = 5
	account4 := acc
	account4Enc := accounts.SerialiseV3(&account4)

	get := func(key accounts.Address, expectTxnID uint64) (res [1]chan []byte) {
		wg := sync.WaitGroup{}
		for i := 0; i < len(res); i++ {
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
					view := cacheView.(*kvcache.CoherentView)
					kv := key.Value()
					v, err := c.Get(kv[:], tx, kvcache.StateVersionID(view))
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

	put := func(k accounts.Address, v *accounts.Account) uint64 {
		var txID uint64
		err := db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
			txID = tx.ViewID()
			d, err := state.NewExecutionContext(ctx, tx, log.New())
			if err != nil {
				return err
			}
			defer d.Close()
			txNum := uint64(0)
			if err := d.PutAccount(ctx, k, v, tx, txNum); err != nil {
				return err
			}
			return d.Flush(ctx, tx)
		})
		require.NoError(err)
		return txID
	}
	// block 1 - represents existing state (no notifications about this data will come to client)
	txID1 := put(k2, &account1)

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

	txID2 := put(k1, &account2)
	fmt.Printf("-----1 %d, %d\n", txID1, txID2)
	res3, res4 := get(k1, txID2), get(k2, txID2) // will see View of transaction 2
	txID3 := put(k1, &account2)                  // even if core already on block 3

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
					Address: gointerfaces.ConvertAddressToH160(k1.Value()),
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
					Address: gointerfaces.ConvertAddressToH160(k1.Value()),
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
	txID4 := put(k1, &account2)

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
					Address: gointerfaces.ConvertAddressToH160(k1.Value()),
					Data:    account4Enc,
				}},
			},
		},
	})
	fmt.Printf("-----4\n")
	txID5 := put(k1, &account4) // reorg to new chain
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
					Address: gointerfaces.ConvertAddressToH160(k1.Value()),
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
	// err := db.View(context.Background(), func(tx kv.Tx) error {
	// 	_, err := AssertCheckValues(context.Background(), tx, c)
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
	t.Skip("TODO: use state reader/writer instead of Put()")
	require, ctx := require.New(t), context.Background()
	c := kvcache.New(kvcache.DefaultCoherentConfig)
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	k1, k2 := [20]byte{1}, [20]byte{2}

	_ = db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		//todo: use kv.CodeDomain
		//_ = tx.Put(kv.Code, k1[:], k2[:])
		cacheView, _ := c.View(ctx, tx)
		view := cacheView.(*kvcache.CoherentView)

		v, err := c.GetCode(k1[:], tx, kvcache.StateVersionID(view))
		require.NoError(err)
		require.Equal(k2[:], v)

		v, err = c.GetCode(k1[:], tx, kvcache.StateVersionID(view))
		require.NoError(err)
		require.Equal(k2[:], v)

		//require.Equal(c.roots[c.latestViewID].cache.Len(), c.stateEvict.Len())
		return nil
	})
}
