package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"sort"
	"strings"
)

func formatBucketKVPair(k, v []byte, bucket string) string {
	// switch statement on bucket (found in tables.go)
	switch bucket {
	case kv.SyncStageProgress:
		val := binary.BigEndian.Uint64(v)
		return fmt.Sprintf("%s %d", string(k), val)

	case kv.Sequence:
		return fmt.Sprintf("%s %x", k, v)

	default:
		return fmt.Sprintf("%x %x", k, v)
	}
}

func printBuckets(chaindata, buckets string) {
	if buckets == "" {
		buckets = kv.EthTx
	}
	for _, b := range strings.Split(buckets, ",") {
		printBucket(chaindata, b)
	}
}

func countAccounts(chaindata string) error {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()

	var count uint64
	var keys []string

	if err := db.View(context.Background(), func(tx kv.Tx) error {
		return tx.ForEach(kv.PlainState, nil, func(k, v []byte) error {
			if len(k) == 20 {
				count++
				keys = append(keys, common.Bytes2Hex(k))
			}
			return nil
		})
	}); err != nil {
		return err
	}

	fmt.Printf("count=%d\n", count)
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("%s\n", k)
	}

	return nil
}
