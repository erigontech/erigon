// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rawdb

import (
	"fmt"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
)

// DeleteHeader - dangerous, use DeleteAncientBlocks/TruncateBlocks methods
func DeleteHeader(db kv.Deleter, hash libcommon.Hash, number uint64) error {
	if err := db.Delete(kv.Headers, dbutils.HeaderKey(number, hash)); err != nil {
		return fmt.Errorf("failed to delete header: %v", err)
	}
	if err := db.Delete(kv.HeaderNumber, hash.Bytes()); err != nil {
		return fmt.Errorf("failed to delete hash to number mapping: %v", err)
	}

	return nil
}

func DeleteSenders(db kv.Deleter, hash libcommon.Hash, number uint64) error {
	if err := db.Delete(kv.Senders, dbutils.BlockBodyKey(number, hash)); err != nil {
		return fmt.Errorf("failed to delete block senders: %w", err)
	}
	return nil
}

func TruncateSenders(db kv.RwTx, fromBlockNum, toBlockNum uint64) error {
	for i := fromBlockNum; i <= toBlockNum; i++ {
		hash, err := rawdb.ReadCanonicalHash(db, i)
		if err != nil {
			return err
		}

		if err = DeleteSenders(db, hash, i); err != nil {
			return err
		}
	}

	return nil
}
