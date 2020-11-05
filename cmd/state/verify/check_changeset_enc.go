package verify

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/changeset"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"golang.org/x/sync/errgroup"
)

type Walker interface {
	Walk(f func(k, v []byte) error) error
	Find(k []byte) ([]byte, error)
}

func CheckEnc(chaindata string) error {
	db := ethdb.MustOpen(chaindata)
	defer db.Close()
	var (
		currentSize uint64
		newSize     uint64
	)
	//set test methods
	chainDataStorageDecoder := changeset.DecodeStorage
	testStorageEncoder := changeset.EncodeStorage
	testStorageDecoder := changeset.DecodeStorage
	testWalker := func(b []byte) Walker {
		return changeset.StorageChangeSetBytes(b)
	}

	startTime := time.Now()
	ch := make(chan struct {
		k []byte
		v []byte
	})
	stop := make(chan struct{})
	//run workers
	g, ctx := errgroup.WithContext(context.Background())
	for i := 0; i < runtime.NumCPU()-1; i++ {
		g.Go(func() error {
			for {
				select {
				case v := <-ch:
					blockNum, _ := dbutils.DecodeTimestamp(v.k)
					cs, innerErr := chainDataStorageDecoder(v.v)
					if innerErr != nil {
						return innerErr
					}

					data, innerErr := testStorageEncoder(cs)
					if innerErr != nil {
						return innerErr
					}
					atomic.AddUint64(&currentSize, uint64(len(v.v)))
					atomic.AddUint64(&newSize, uint64(len(data)))

					cs2, innerErr := testStorageDecoder(data)
					if innerErr != nil {
						return innerErr
					}

					if !reflect.DeepEqual(cs, cs2) {
						return fmt.Errorf("not identical changesets. block %d", blockNum)
					}

					walker := testWalker(data)
					for _, val := range cs.Changes {
						value, findErr := walker.Find(val.Key)
						if findErr != nil {
							return findErr
						}
						if !bytes.Equal(value, val.Value) {
							return fmt.Errorf("block: %d. incorrect value for %v. Returned:%v", blockNum, common.Bytes2Hex(val.Key), common.Bytes2Hex(value))
						}
					}
					j := 0

					err := walker.Walk(func(kk, vv []byte) error {
						if !bytes.Equal(kk, cs2.Changes[j].Key) {
							return fmt.Errorf("incorrect order. block: %d, element: %v", blockNum, j)
						}
						if !bytes.Equal(vv, cs2.Changes[j].Value) {
							return fmt.Errorf("incorrect value. block: %d, key:%v", blockNum, common.Bytes2Hex(cs.Changes[j].Key))
						}
						j++
						return nil
					})
					if err != nil {
						return err
					}
				case <-ctx.Done():
					return nil
				case <-stop:
					return nil
				}
			}
		})
	}

	g.Go(func() error {
		var i uint64
		defer func() {
			close(stop)
		}()

		return db.Walk(dbutils.StorageChangeSetBucket2, []byte{}, 0, func(k, v []byte) (b bool, e error) {
			if i%100_000 == 0 {
				blockNum, _ := dbutils.DecodeTimestamp(k)
				fmt.Printf("Processed %dK, block number %d, current %d, new %d, time %s\n",
					i/1000,
					blockNum,
					atomic.LoadUint64(&currentSize),
					atomic.LoadUint64(&newSize),
					time.Since(startTime))
			}

			select {
			case <-ctx.Done():
				return false, nil
			default:

			}

			i++
			ch <- struct {
				k []byte
				v []byte
			}{k: k, v: v}

			return true, nil
		})
	})
	err := g.Wait()
	if err != nil {
		return err
	}

	fmt.Println("-- Final size --")
	fmt.Println("Current:", currentSize)
	fmt.Println("New:", newSize)

	return nil
}
