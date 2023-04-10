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

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"

	"github.com/ledgerwatch/erigon/common"
)

func IndexStats(chaindata string, indexBucket string, statsFile string) error {
	db := mdbx.MustOpen(chaindata)
	startTime := time.Now()
	lenOfKey := length.Addr
	if strings.HasPrefix(indexBucket, kv.StorageHistory) {
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
			fmt.Printf("Processed %dK, %s\n", i/1000, time.Since(startTime))
		}
		if bytes.Equal(k[:lenOfKey], prevKey) {
			count++
			if count > 1 && !added {
				more1index++
				added = true
			}
			if count > 10 {
				more10index[string(libcommon.Copy(k[:lenOfKey]))] = count
			}
			if count > 50 {
				more50index[string(libcommon.Copy(k[:lenOfKey]))] = count
			}
			if count > 100 {
				more100index[string(libcommon.Copy(k[:lenOfKey]))] = count
			}
			if count > 200 {
				more200index[string(libcommon.Copy(k[:lenOfKey]))] = count
			}
			if count > 500 {
				more500index[string(libcommon.Copy(k[:lenOfKey]))] = count
			}
			if count > 1000 {
				more1000index[string(libcommon.Copy(k[:lenOfKey]))] = count
			}
		} else {
			added = false
			count = 1
			prevKey = libcommon.Copy(k[:length.Addr])
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
				Address:      libcommon.BytesToAddress(p).String(),
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
