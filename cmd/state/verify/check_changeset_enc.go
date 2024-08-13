// Copyright 2024 The Erigon Authors
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

package verify

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	historyv22 "github.com/erigontech/erigon-lib/kv/temporal/historyv2"
	"golang.org/x/sync/errgroup"
)

type Walker interface {
	Walk(f func(k, v []byte) error) error
	Find(k []byte) ([]byte, error)
}

func CheckEnc(chaindata string) error {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	var (
		currentSize uint64
		newSize     uint64
	)

	//set test methods
	chainDataStorageDecoder := historyv22.Mapper[kv.StorageChangeSet].Decode
	testStorageEncoder := historyv22.Mapper[kv.StorageChangeSet].Encode
	testStorageDecoder := historyv22.Mapper[kv.StorageChangeSet].Decode

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
					blockNum, kk, vv, err := chainDataStorageDecoder(v.k, v.v)
					if err != nil {
						return err
					}
					cs := historyv22.NewStorageChangeSet()
					_ = cs.Add(v.k, v.v)
					atomic.AddUint64(&currentSize, uint64(len(v.v)))
					innerErr := testStorageEncoder(blockNum, cs, func(k, v []byte) error {
						atomic.AddUint64(&newSize, uint64(len(v)))
						_, a, b, err := testStorageDecoder(k, v)
						if err != nil {
							return err
						}
						if !bytes.Equal(kk, a) {
							return fmt.Errorf("incorrect order. block: %d", blockNum)
						}
						if !bytes.Equal(vv, b) {
							return fmt.Errorf("incorrect value. block: %d, key: %x", blockNum, a)
						}
						return nil
					})
					if innerErr != nil {
						return innerErr
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
		return db.View(context.Background(), func(tx kv.Tx) error {
			return tx.ForEach(kv.StorageChangeSet, []byte{}, func(k, v []byte) error {
				if i%100_000 == 0 {
					blockNum := binary.BigEndian.Uint64(k)
					fmt.Printf("Processed %s, block number %d, current %d, new %d, time %s\n",
						common.PrettyCounter(i),
						blockNum,
						atomic.LoadUint64(&currentSize),
						atomic.LoadUint64(&newSize),
						time.Since(startTime))
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				default:

				}

				i++
				ch <- struct {
					k []byte
					v []byte
				}{k: k, v: v}
				return nil
			})
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
