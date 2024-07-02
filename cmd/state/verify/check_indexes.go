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
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv/dbutils"

	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/historyv2"
	"github.com/ledgerwatch/erigon/common"
)

func CheckIndex(ctx context.Context, chaindata string, changeSetBucket string, indexBucket string) error {
	db := mdbx.MustOpen(chaindata)
	defer db.Close()
	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	startTime := time.Now()

	i := 0
	if err := historyv2.ForEach(tx, changeSetBucket, nil, func(blockN uint64, k, v []byte) error {
		i++
		if i%100_000 == 0 {
			fmt.Printf("Processed %dK, %s\n", blockN/1000, time.Since(startTime))
		}
		select {
		default:
		case <-ctx.Done():
			return ctx.Err()
		}

		bm, innerErr := bitmapdb.Get64(tx, indexBucket, dbutils.CompositeKeyWithoutIncarnation(k), blockN-1, blockN+1)
		if innerErr != nil {
			return innerErr
		}
		if !bm.Contains(blockN) {
			return fmt.Errorf("%v,%v", blockN, common.Bytes2Hex(k))
		}
		return nil
	}); err != nil {
		return err
	}

	fmt.Println("Check was successful")
	return nil
}
