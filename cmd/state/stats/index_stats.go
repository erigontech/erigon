package stats

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func IndexStats(chaindata string, indexBucket []byte, statsFile string) error {
	db := ethdb.MustOpen(chaindata)
	startTime := time.Now()
	lenOfKey := common.HashLength
	if bytes.HasPrefix(indexBucket, dbutils.StorageHistoryBucket) {
		lenOfKey = common.HashLength*2 + common.IncarnationLength
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
	if err := db.Walk(indexBucket, []byte{}, 0, func(k, v []byte) (b bool, e error) {
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
				more10index[string(common.CopyBytes(k[:lenOfKey]))] = count
			}
			if count > 50 {
				more50index[string(common.CopyBytes(k[:lenOfKey]))] = count
			}
			if count > 100 {
				more100index[string(common.CopyBytes(k[:lenOfKey]))] = count
			}
			if count > 200 {
				more200index[string(common.CopyBytes(k[:lenOfKey]))] = count
			}
			if count > 500 {
				more500index[string(common.CopyBytes(k[:lenOfKey]))] = count
			}
			if count > 1000 {
				more1000index[string(common.CopyBytes(k[:lenOfKey]))] = count
			}
		} else {
			added = false
			count = 1
			prevKey = common.CopyBytes(k[:common.HashLength])
		}

		return true, nil
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
			p, innerErr := db.Get(dbutils.PreimagePrefix, []byte(hash)[:common.HashLength])
			if innerErr != nil {
				return innerErr
			}
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
