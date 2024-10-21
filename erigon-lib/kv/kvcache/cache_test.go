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
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types"
)

func TestEvictionInUnexpectedOrder(t *testing.T) {
	// Order: View - 2, OnNewBlock - 2, View - 5, View - 6, OnNewBlock - 3, OnNewBlock - 4, View - 5, OnNewBlock - 5, OnNewBlock - 100
	require := require.New(t)
	cfg := DefaultCoherentConfig
	cfg.CacheSize = 3
	cfg.NewBlockWait = 0
	c := New(cfg)
	c.selectOrCreateRoot(2)
	require.Equal(1, len(c.roots))
	require.Equal(0, int(c.latestStateVersionID))
	require.False(c.roots[2].isCanonical)

	c.add([]byte{1}, nil, c.roots[2], 2)
	require.Equal(0, c.stateEvict.Len())

	c.advanceRoot(2)
	require.Equal(1, len(c.roots))
	require.Equal(2, int(c.latestStateVersionID))
	require.True(c.roots[2].isCanonical)

	c.add([]byte{1}, nil, c.roots[2], 2)
	require.Equal(1, c.stateEvict.Len())

	c.selectOrCreateRoot(5)
	require.Equal(2, len(c.roots))
	require.Equal(2, int(c.latestStateVersionID))
	require.False(c.roots[5].isCanonical)

	c.add([]byte{2}, nil, c.roots[5], 5) // not added to evict list
	require.Equal(1, c.stateEvict.Len())
	c.add([]byte{2}, nil, c.roots[2], 2) // added to evict list, because it's latest view
	require.Equal(2, c.stateEvict.Len())

	c.selectOrCreateRoot(6)
	require.Equal(3, len(c.roots))
	require.Equal(2, int(c.latestStateVersionID))
	require.False(c.roots[6].isCanonical) // parrent exists, but parent has isCanonical=false

	c.advanceRoot(3)
	require.Equal(4, len(c.roots))
	require.Equal(3, int(c.latestStateVersionID))
	require.True(c.roots[3].isCanonical)

	c.advanceRoot(4)
	require.Equal(5, len(c.roots))
	require.Equal(4, int(c.latestStateVersionID))
	require.True(c.roots[4].isCanonical)

	c.selectOrCreateRoot(5)
	require.Equal(5, len(c.roots))
	require.Equal(4, int(c.latestStateVersionID))
	require.False(c.roots[5].isCanonical)

	c.advanceRoot(5)
	require.Equal(5, len(c.roots))
	require.Equal(5, int(c.latestStateVersionID))
	require.True(c.roots[5].isCanonical)

	c.advanceRoot(100)
	require.Equal(6, len(c.roots))
	require.Equal(100, int(c.latestStateVersionID))
	require.True(c.roots[100].isCanonical)

	//c.add([]byte{1}, nil, c.roots[2], 2)
	require.Equal(0, c.latestStateView.cache.Len())
	require.Equal(0, c.stateEvict.Len())
}

func TestEviction(t *testing.T) {
	require, ctx := require.New(t), context.Background()
	cfg := DefaultCoherentConfig
	cfg.CacheSize = 21
	cfg.NewBlockWait = 0
	c := New(cfg)

	dirs := datadir.New(t.TempDir())
	db, _ := temporaltest.NewTestDB(t, dirs)
	k1, k2 := [20]byte{1}, [20]byte{2}

	var id uint64
	_ = db.Update(ctx, func(tx kv.RwTx) error {
		_ = tx.Put(kv.PlainState, k1[:], []byte{1})
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
	c.OnNewBlock(&remote.StateChangeBatch{
		StateVersionId: id + 1,
		ChangeBatch: []*remote.StateChange{
			{
				Direction: remote.Direction_FORWARD,
				Changes: []*remote.AccountChange{{
					Action:  remote.Action_UPSERT,
					Address: gointerfaces.ConvertAddressToH160(k1),
					Data:    []byte{2},
				}},
			},
		},
	})
	require.Equal(21, c.stateEvict.Size())
	require.Equal(1, c.stateEvict.Len())
	require.Equal(c.roots[c.latestStateVersionID].cache.Len(), c.stateEvict.Len())
	_ = db.Update(ctx, func(tx kv.RwTx) error {
		_ = tx.Put(kv.PlainState, k1[:], []byte{1})
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

func TestAPI(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("fix me on win please")
	}
	require := require.New(t)
	c := New(DefaultCoherentConfig)
	k1, k2 := [20]byte{1}, [20]byte{2}
	db, _ := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	account1Enc := types.EncodeAccountBytesV3(1, uint256.NewInt(11), make([]byte, 32), 2)
	account2Enc := types.EncodeAccountBytesV3(1, uint256.NewInt(11), make([]byte, 32), 3)
	account4Enc := types.EncodeAccountBytesV3(1, uint256.NewInt(11), make([]byte, 32), 5)

	get := func(key [20]byte, expectTxnID uint64) (res [1]chan []byte) {

		wg := sync.WaitGroup{}
		for i := 0; i < len(res); i++ {
			wg.Add(1)
			res[i] = make(chan []byte)
			go func(out chan []byte) {
				require.NoError(db.View(context.Background(), func(tx kv.Tx) error {
					if expectTxnID != tx.ViewID() {
						panic(fmt.Sprintf("epxected: %d, got: %d", expectTxnID, tx.ViewID()))
					}
					wg.Done()
					cacheView, err := c.View(context.Background(), tx)
					view := cacheView.(*CoherentView)
					if err != nil {
						panic(err)
					}
					v, err := c.Get(key[:], tx, view.stateVersionID)
					if err != nil {
						panic(err)
					}
					fmt.Println("get", key, v)
					out <- common.Copy(v)
					return nil
				}))
			}(res[i])
		}
		wg.Wait() // ensure that all goroutines started their transactions
		return res
	}
	counter := atomic.Int64{}
	prevVals := map[string][]byte{}
	put := func(k, v []byte) uint64 {
		var txID uint64
		require.NoError(db.Update(context.Background(), func(tx kv.RwTx) error {
			txID = tx.ViewID()
			d, err := state.NewSharedDomains(tx, log.New())
			if err != nil {
				return err
			}
			defer d.Close()
			if err := d.DomainPut(kv.AccountsDomain, k, nil, v, prevVals[string(k)], uint64(counter.Load())); err != nil {
				return err
			}
			prevVals[string(k)] = v
			counter.Add(1)
			return d.Flush(context.Background(), tx)
		}))
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
			require.Nil(<-res1[i])
		}
		for i := range res2 {
			require.Equal(account1Enc, <-res2[i])
		}
		fmt.Printf("done1: \n")
	}()

	txID2 := put(k1[:], account2Enc)
	fmt.Printf("-----1 %d, %d\n", txID1, txID2)
	res3, res4 := get(k1, txID2), get(k2, txID2) // will see View of transaction 2
	txID3 := put(k1[:], account2Enc)             // even if core already on block 3

	c.OnNewBlock(&remote.StateChangeBatch{
		StateVersionId:      txID2,
		PendingBlockBaseFee: 1,
		ChangeBatch: []*remote.StateChange{
			{
				Direction:   remote.Direction_FORWARD,
				BlockHeight: 2,
				BlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
				Changes: []*remote.AccountChange{{
					Action:  remote.Action_UPSERT,
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
			require.Equal(account2Enc, <-res3[i])
		}
		for i := range res4 {
			require.Equal(account1Enc, <-res4[i])
		}
		fmt.Printf("done2: \n")
	}()
	fmt.Printf("-----2\n")

	res5, res6 := get(k1, txID3), get(k2, txID3) // will see View of transaction 3, even if notification has not enough changes
	c.OnNewBlock(&remote.StateChangeBatch{
		StateVersionId:      txID3,
		PendingBlockBaseFee: 1,
		ChangeBatch: []*remote.StateChange{
			{
				Direction:   remote.Direction_FORWARD,
				BlockHeight: 3,
				BlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
				Changes: []*remote.AccountChange{{
					Action:  remote.Action_UPSERT,
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
			require.Equal(account2Enc, <-res5[i])
		}
		fmt.Printf("-----21\n")
		for i := range res6 {
			require.Equal(account1Enc, <-res6[i])
		}
		fmt.Printf("done3: \n")
	}()
	fmt.Printf("-----3\n")
	txID4 := put(k1[:], account2Enc)
	time.Sleep(10 * time.Second)

	c.OnNewBlock(&remote.StateChangeBatch{
		StateVersionId:      txID4,
		PendingBlockBaseFee: 1,
		ChangeBatch: []*remote.StateChange{
			{
				Direction:   remote.Direction_UNWIND,
				BlockHeight: 2,
				BlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
				Changes: []*remote.AccountChange{{
					Action:  remote.Action_UPSERT,
					Address: gointerfaces.ConvertAddressToH160(k1),
					Data:    account4Enc,
				}},
			},
		},
	})
	fmt.Printf("-----4\n")
	txID5 := put(k1[:], account4Enc) // reorg to new chain
	c.OnNewBlock(&remote.StateChangeBatch{
		StateVersionId:      txID5,
		PendingBlockBaseFee: 1,
		ChangeBatch: []*remote.StateChange{
			{
				Direction:   remote.Direction_FORWARD,
				BlockHeight: 3,
				BlockHash:   gointerfaces.ConvertHashToH256([32]byte{2}),
				Changes: []*remote.AccountChange{{
					Action:  remote.Action_UPSERT,
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
			require.Equal(account4Enc, <-res7[i])
		}
		for i := range res8 {
			require.Equal(account1Enc, <-res8[i])
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

	wg.Wait()
}

func TestCode(t *testing.T) {
	t.Skip("TODO: use state reader/writer instead of Put()")
	require, ctx := require.New(t), context.Background()
	c := New(DefaultCoherentConfig)
	db, _ := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	k1, k2 := [20]byte{1}, [20]byte{2}

	_ = db.Update(ctx, func(tx kv.RwTx) error {
		_ = tx.Put(kv.Code, k1[:], k2[:])
		cacheView, _ := c.View(ctx, tx)
		view := cacheView.(*CoherentView)

		v, err := c.GetCode(k1[:], tx, view.stateVersionID)
		require.NoError(err)
		require.Equal(k2[:], v)

		v, err = c.GetCode(k1[:], tx, view.stateVersionID)
		require.NoError(err)
		require.Equal(k2[:], v)

		//require.Equal(c.roots[c.latestViewID].cache.Len(), c.stateEvict.Len())
		return nil
	})
}
