/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package kvcache

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"
)

func bigEndian(n uint64) []byte {
	num := [8]byte{}
	binary.BigEndian.PutUint64(num[:], n)
	return num[:]
}

func TestAPI(t *testing.T) {
	require := require.New(t)
	c := New(DefaultCoherentCacheConfig)
	k1, k2 := [20]byte{1}, [20]byte{2}
	db := memdb.NewTestDB(t)
	get := func(key [20]byte, expectBlockN uint64) (res [1]chan []byte) {
		wg := sync.WaitGroup{}
		for i := 0; i < len(res); i++ {
			wg.Add(1)
			res[i] = make(chan []byte)
			go func(out chan []byte) {
				require.NoError(db.View(context.Background(), func(tx kv.Tx) error {
					encBlockNum, err := tx.GetOne(kv.SyncStageProgress, []byte("Finish"))
					if err != nil {
						return err
					}
					n := binary.BigEndian.Uint64(encBlockNum)
					if expectBlockN != n {
						panic(fmt.Sprintf("epxected: %d, got: %d", expectBlockN, n))
					}
					wg.Done()
					cache, err := c.View(context.Background(), tx)
					if err != nil {
						panic(err)
					}
					v, err := cache.Get(key[:], tx)
					if err != nil {
						panic(err)
					}
					out <- common.Copy(v)
					return nil
				}))
			}(res[i])
		}
		wg.Wait() // ensure that all goroutines started their transactions
		return res
	}
	put := func(blockNum uint64, blockHash [32]byte, k, v []byte) {
		require.NoError(db.Update(context.Background(), func(tx kv.RwTx) error {
			_ = tx.Put(kv.SyncStageProgress, []byte("Finish"), bigEndian(blockNum))
			_ = tx.Put(kv.HeaderCanonical, bigEndian(blockNum), blockHash[:])
			_ = tx.Put(kv.PlainState, k, v)
			return nil
		}))
	}
	// block 1 - represents existing state (no notifications about this data will come to client)
	put(1, [32]byte{}, k2[:], []byte{42})

	wg := sync.WaitGroup{}

	res1, res2 := get(k1, 1), get(k2, 1) // will return immediately
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range res1 {
			require.Nil(<-res1[i])
		}
		for i := range res2 {
			require.Equal([]byte{42}, <-res2[i])
		}
		fmt.Printf("done1: \n")
	}()

	put(2, [32]byte{}, k1[:], []byte{2})
	fmt.Printf("-----1\n")
	res3, res4 := get(k1, 2), get(k2, 2) // will see View of transaction 2
	put(3, [32]byte{}, k1[:], []byte{3}) // even if core already on block 3

	c.OnNewBlock(&remote.StateChange{
		Direction:       remote.Direction_FORWARD,
		PrevBlockHeight: 1,
		PrevBlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
		BlockHeight:     2,
		BlockHash:       gointerfaces.ConvertHashToH256([32]byte{}),
		Changes: []*remote.AccountChange{{
			Action:  remote.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(k1),
			Data:    []byte{2},
		}},
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range res3 {
			require.Equal([]byte{2}, <-res3[i])
		}
		for i := range res4 {
			require.Equal([]byte{42}, <-res4[i])
		}
		fmt.Printf("done2: \n")
	}()
	fmt.Printf("-----2\n")

	res5, res6 := get(k1, 3), get(k2, 3) // will see View of transaction 3, even if notification has not enough changes
	c.OnNewBlock(&remote.StateChange{
		Direction:       remote.Direction_FORWARD,
		PrevBlockHeight: 2,
		PrevBlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
		BlockHeight:     3,
		BlockHash:       gointerfaces.ConvertHashToH256([32]byte{}),
		Changes: []*remote.AccountChange{{
			Action:  remote.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(k1),
			Data:    []byte{3},
		}},
	})

	fmt.Printf("-----20\n")
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range res5 {
			require.Equal([]byte{3}, <-res5[i])
		}
		fmt.Printf("-----21\n")
		for i := range res6 {
			require.Equal([]byte{42}, <-res6[i])
		}
		fmt.Printf("done3: \n")
	}()
	fmt.Printf("-----3\n")
	put(2, [32]byte{}, k1[:], []byte{2})
	c.OnNewBlock(&remote.StateChange{
		Direction:       remote.Direction_UNWIND,
		PrevBlockHeight: 3,
		PrevBlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
		BlockHeight:     2,
		BlockHash:       gointerfaces.ConvertHashToH256([32]byte{}),
		Changes: []*remote.AccountChange{{
			Action:  remote.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(k1),
			Data:    []byte{2},
		}},
	})
	fmt.Printf("-----4\n")
	put(3, [32]byte{2}, k1[:], []byte{4}) // reorg to new chain
	c.OnNewBlock(&remote.StateChange{
		Direction:       remote.Direction_FORWARD,
		PrevBlockHeight: 2,
		PrevBlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
		BlockHeight:     3,
		BlockHash:       gointerfaces.ConvertHashToH256([32]byte{2}),
		Changes: []*remote.AccountChange{{
			Action:  remote.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(k1),
			Data:    []byte{4},
		}},
	})
	fmt.Printf("-----5\n")

	res7, res8 := get(k1, 3), get(k2, 3) // will see View of transaction 3, even if notification has not enough changes

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range res7 {
			require.Equal([]byte{4}, <-res7[i])
		}
		for i := range res8 {
			require.Equal([]byte{42}, <-res8[i])
		}
		fmt.Printf("done4: \n")
	}()
	err := db.View(context.Background(), func(tx kv.Tx) error {
		_, err := AssertCheckValues(context.Background(), tx, c)
		require.NoError(err)
		return nil
	})
	require.NoError(err)

	wg.Wait()
}
