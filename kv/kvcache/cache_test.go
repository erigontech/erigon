package kvcache

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

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
	c := New()
	k1, k2 := [20]byte{1}, [20]byte{2}
	db := memdb.NewTestDB(t)
	get := func(key [20]byte) (res [10]chan []byte) {
		wg := sync.WaitGroup{}
		for i := 0; i < len(res); i++ {
			wg.Add(1)
			res[i] = make(chan []byte)
			go func(out chan []byte) {
				require.NoError(db.View(context.Background(), func(tx kv.Tx) error {
					wg.Done()
					cache, err := c.View(tx)
					if err != nil {
						panic(err)
					}
					v, err := cache.Get(key[:], tx)
					if err != nil {
						panic(err)
					}
					out <- copyBytes(v)
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

	res1, res2 := get(k1), get(k2) // will return immediately
	for i := range res1 {
		require.Nil(<-res1[i])
	}
	for i := range res2 {
		require.Equal([]byte{42}, <-res2[i])
	}
	put(2, [32]byte{}, k1[:], []byte{2})
	fmt.Printf("-----1\n")
	res3, res4 := get(k1), get(k2)       // will see View of transaction 2
	put(3, [32]byte{}, k1[:], []byte{3}) // even if core already on block 3

	c.OnNewBlock(&remote.StateChange{
		Direction:   remote.Direction_FORWARD,
		BlockHeight: 2,
		BlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
		Changes: []*remote.AccountChange{{
			Action:  remote.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(k1),
			Data:    []byte{1},
		}},
	})

	for i := range res3 {
		require.Equal([]byte{2}, <-res3[i])
	}
	for i := range res4 {
		require.Equal([]byte{42}, <-res4[i])
	}
	fmt.Printf("-----2\n")

	res5, res6 := get(k1), get(k2) // will see View of transaction 3, even if notification has not enough changes
	c.OnNewBlock(&remote.StateChange{
		Direction:   remote.Direction_FORWARD,
		BlockHeight: 3,
		BlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
		Changes: []*remote.AccountChange{{
			Action:  remote.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(k1),
			Data:    []byte{3},
		}},
	})

	fmt.Printf("-----20\n")
	for i := range res5 {
		require.Equal([]byte{3}, <-res5[i])
	}
	fmt.Printf("-----21\n")
	for i := range res6 {
		require.Equal([]byte{42}, <-res6[i])
	}
	fmt.Printf("-----3\n")
	put(2, [32]byte{}, k1[:], []byte{2})
	c.OnNewBlock(&remote.StateChange{
		Direction:   remote.Direction_UNWIND,
		BlockHeight: 2,
		BlockHash:   gointerfaces.ConvertHashToH256([32]byte{}),
		Changes: []*remote.AccountChange{{
			Action:  remote.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(k1),
			Data:    []byte{2},
		}},
	})
	fmt.Printf("-----4\n")
	put(3, [32]byte{2}, k1[:], []byte{4}) // reorg to new chain
	c.OnNewBlock(&remote.StateChange{
		Direction:   remote.Direction_FORWARD,
		BlockHeight: 3,
		BlockHash:   gointerfaces.ConvertHashToH256([32]byte{2}),
		Changes: []*remote.AccountChange{{
			Action:  remote.Action_UPSERT,
			Address: gointerfaces.ConvertAddressToH160(k1),
			Data:    []byte{4},
		}},
	})
	fmt.Printf("-----5\n")

	res7, res8 := get(k1), get(k2) // will see View of transaction 3, even if notification has not enough changes

	for i := range res7 {
		require.Equal([]byte{4}, <-res7[i])
	}
	for i := range res8 {
		require.Equal([]byte{42}, <-res8[i])
	}
	_ = db.View(context.Background(), func(tx kv.Tx) error {
		require.NoError(AssertCheckValues(tx, c))
		return nil
	})
}
