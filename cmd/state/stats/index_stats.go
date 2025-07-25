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

package stats

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
)

func IndexStats(chaindata string, indexBucket string, statsFile string) error {
	db := mdbx.MustOpen(chaindata)
	startTime := time.Now()
	lenOfKey := length.Addr
	if strings.HasPrefix(indexBucket, kv.E2StorageHistory) {
		lenOfKey = length.Addr + length.Hash + length.Incarnation
	}

	more1index := 0
	more10index := make(map[string]uint64)
	more50index := make(map[string]uint64)
	more100index := make(map[string]uint64)
	more200index := make(map[string]uint64)
	more500index := make(map[string]uint64)
	more1000index := make(map[string]uint64)

	prevKey := []byte{}
	count := uint64(1)
	added := false
	i := uint64(0)
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = tx.ForEach(indexBucket, []byte{}, func(k, v []byte) error {
		if i%100_000 == 0 {
			fmt.Printf("Processed %s, %s\n", common.PrettyCounter(i), time.Since(startTime))
		}
		if bytes.Equal(k[:lenOfKey], prevKey) {
			count++
			if count > 1 && !added {
				more1index++
				added = true
			}
			if count > 10 {
				more10index[string(common.Copy(k[:lenOfKey]))] = count
			}
			if count > 50 {
				more50index[string(common.Copy(k[:lenOfKey]))] = count
			}
			if count > 100 {
				more100index[string(common.Copy(k[:lenOfKey]))] = count
			}
			if count > 200 {
				more200index[string(common.Copy(k[:lenOfKey]))] = count
			}
			if count > 500 {
				more500index[string(common.Copy(k[:lenOfKey]))] = count
			}
			if count > 1000 {
				more1000index[string(common.Copy(k[:lenOfKey]))] = count
			}
		} else {
			added = false
			count = 1
			prevKey = common.Copy(k[:length.Addr])
		}

		return nil
	}); err != nil {
		return err
	}

	fmt.Println("more1", more1index)
	fmt.Println("more10", len(more10index))
	fmt.Println("more50", len(more50index))
	fmt.Println("more100", len(more100index))
	fmt.Println("more200", len(more200index))
	fmt.Println("more500", len(more500index))
	fmt.Println("more1000", len(more1000index))

	if statsFile != "" {
		f, err := os.Create(statsFile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close() //nolint
		save10 := make([]struct {
			Address      string
			Hash         string
			NumOfIndexes uint64
		}, 0, len(more10index))
		for hash, v := range more10index {
			p := []byte(hash)[:length.Addr]
			if len(p) == 0 {
				p = make([]byte, 20)
			}
			save10 = append(save10, struct {
				Address      string
				Hash         string
				NumOfIndexes uint64
			}{
				Address:      common.BytesToAddress(p).String(),
				NumOfIndexes: v,
				Hash:         common.Bytes2Hex([]byte(hash)),
			})

		}
		sort.Slice(save10, func(i, j int) bool {
			return save10[i].NumOfIndexes > save10[j].NumOfIndexes
		})

		csvWriter := csv.NewWriter(f)
		err = csvWriter.Write([]string{"hash", "address", "num"})
		if err != nil {
			return err
		}
		for _, v := range save10 {
			err = csvWriter.Write([]string{v.Hash, v.Address, strconv.FormatUint(v.NumOfIndexes, 10)})
			if err != nil {
				return err
			}
		}
	}
	return nil
}
