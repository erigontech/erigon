package rawdb

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

import (
	"math/big"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func BenchmarkLookupStorage(b *testing.B) {
	for _, tc := range DBVersions {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			for n := 0; n < b.N; n++ {
				db := ethdb.NewMemDatabase()

				tx1 := types.NewTransaction(1, common.BytesToAddress([]byte{0x11}), big.NewInt(111), 1111, big.NewInt(11111), []byte{0x11, 0x11, 0x11})
				tx2 := types.NewTransaction(2, common.BytesToAddress([]byte{0x22}), big.NewInt(222), 2222, big.NewInt(22222), []byte{0x22, 0x22, 0x22})
				tx3 := types.NewTransaction(3, common.BytesToAddress([]byte{0x33}), big.NewInt(333), 3333, big.NewInt(33333), []byte{0x33, 0x33, 0x33})
				txs := []*types.Transaction{tx1, tx2, tx3}

				block := types.NewBlock(&types.Header{Number: big.NewInt(314)}, txs, nil, nil)
				// Insert all the transactions into the database, and verify contents
				WriteCanonicalHash(db, block.Hash(), block.NumberU64())
				WriteBlock(db, block)
				batch := db.NewBatch()
				b.StartTimer()
				tc.writeTxLookupEntries(batch, block)
				_, _ = batch.Commit()
				b.StopTimer()
			}
		})
	}
}
