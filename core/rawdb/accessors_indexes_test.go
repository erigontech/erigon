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
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// Tests that positional lookup metadata can be stored and retrieved.
func TestLookupStorage(t *testing.T) {
	tests := []struct {
		name                 string
		writeTxLookupEntries func(ethdb.Putter, *types.Block)
	}{
		{
			"DatabaseV6",
			func(db ethdb.Putter, block *types.Block) {
				WriteTxLookupEntries(db, block)
			},
		},
		// Turbo-Geth: older databases are removed, no backward compatibility
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := ethdb.NewMemDatabase()
			defer db.Close()
			ctx := context.Background()

			tx1 := types.NewTransaction(1, common.BytesToAddress([]byte{0x11}), uint256.NewInt().SetUint64(111), 1111, uint256.NewInt().SetUint64(11111), []byte{0x11, 0x11, 0x11})
			tx2 := types.NewTransaction(2, common.BytesToAddress([]byte{0x22}), uint256.NewInt().SetUint64(222), 2222, uint256.NewInt().SetUint64(22222), []byte{0x22, 0x22, 0x22})
			tx3 := types.NewTransaction(3, common.BytesToAddress([]byte{0x33}), uint256.NewInt().SetUint64(333), 3333, uint256.NewInt().SetUint64(33333), []byte{0x33, 0x33, 0x33})
			txs := []*types.Transaction{tx1, tx2, tx3}

			block := types.NewBlock(&types.Header{Number: big.NewInt(314)}, txs, nil, nil)

			// Check that no transactions entries are in a pristine database
			for i, tx := range txs {
				if txn, _, _, _ := ReadTransaction(db, tx.Hash()); txn != nil {
					t.Fatalf("tx #%d [%x]: non existent transaction returned: %v", i, tx.Hash(), txn)
				}
			}
			// Insert all the transactions into the database, and verify contents
			if err := WriteCanonicalHash(db, block.Hash(), block.NumberU64()); err != nil {
				t.Fatal(err)
			}
			if err := WriteBlock(ctx, db, block); err != nil {
				t.Fatal(err)
			}
			tc.writeTxLookupEntries(db, block)

			for i, tx := range txs {
				if txn, hash, number, index := ReadTransaction(db, tx.Hash()); txn == nil {
					t.Fatalf("tx #%d [%x]: transaction not found", i, tx.Hash())
				} else {
					if hash != block.Hash() || number != block.NumberU64() || index != uint64(i) {
						t.Fatalf("tx #%d [%x]: positional metadata mismatch: have %x/%d/%d, want %x/%v/%v", i, tx.Hash(), hash, number, index, block.Hash(), block.NumberU64(), i)
					}
					if tx.Hash() != txn.Hash() {
						t.Fatalf("tx #%d [%x]: transaction mismatch: have %v, want %v", i, tx.Hash(), txn, tx)
					}
				}
			}
			// Delete the transactions and check purge
			for i, tx := range txs {
				if err := DeleteTxLookupEntry(db, tx.Hash()); err != nil {
					t.Fatal(err)
				}
				if txn, _, _, _ := ReadTransaction(db, tx.Hash()); txn != nil {
					t.Fatalf("tx #%d [%x]: deleted transaction returned: %v", i, tx.Hash(), txn)
				}
			}
		})
	}
}
