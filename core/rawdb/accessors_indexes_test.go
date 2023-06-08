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

package rawdb_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

// Tests that positional lookup metadata can be stored and retrieved.
func TestLookupStorage(t *testing.T) {
	tests := []struct {
		name                 string
		writeTxLookupEntries func(kv.Putter, *types.Block)
	}{
		{
			"DatabaseV6",
			func(db kv.Putter, block *types.Block) {
				rawdb.WriteTxLookupEntries(db, block)
			},
		},
		// Erigon: older databases are removed, no backward compatibility
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := stages.Mock(t)
			br, bw := m.NewBlocksIO()
			tx, err := m.DB.BeginRw(m.Ctx)
			require.NoError(t, err)
			defer tx.Rollback()

			tx1 := types.NewTransaction(1, libcommon.BytesToAddress([]byte{0x11}), uint256.NewInt(111), 1111, uint256.NewInt(11111), []byte{0x11, 0x11, 0x11})
			tx2 := types.NewTransaction(2, libcommon.BytesToAddress([]byte{0x22}), uint256.NewInt(222), 2222, uint256.NewInt(22222), []byte{0x22, 0x22, 0x22})
			tx3 := types.NewTransaction(3, libcommon.BytesToAddress([]byte{0x33}), uint256.NewInt(333), 3333, uint256.NewInt(33333), []byte{0x33, 0x33, 0x33})
			txs := []types.Transaction{tx1, tx2, tx3}

			block := types.NewBlock(&types.Header{Number: big.NewInt(314)}, txs, nil, nil, nil)

			// Check that no transactions entries are in a pristine database
			for i, txn := range txs {
				if txn2, _, _, _, _ := readTransactionByHash(tx, txn.Hash(), br); txn2 != nil {
					t.Fatalf("txn #%d [%x]: non existent transaction returned: %v", i, txn.Hash(), txn2)
				}
			}
			// Insert all the transactions into the database, and verify contents
			if err := rawdb.WriteCanonicalHash(tx, block.Hash(), block.NumberU64()); err != nil {
				t.Fatal(err)
			}
			if err := bw.WriteBlock(tx, block); err != nil {
				t.Fatal(err)
			}
			if err := rawdb.WriteSenders(tx, block.Hash(), block.NumberU64(), block.Body().SendersFromTxs()); err != nil {
				t.Fatal(err)
			}
			tc.writeTxLookupEntries(tx, block)

			for i, txn := range txs {
				if txn2, hash, number, index, _ := readTransactionByHash(tx, txn.Hash(), br); txn2 == nil {
					t.Fatalf("txn #%d [%x]: transaction not found", i, txn.Hash())
				} else {
					if hash != block.Hash() || number != block.NumberU64() || index != uint64(i) {
						t.Fatalf("txn #%d [%x]: positional metadata mismatch: have %x/%d/%d, want %x/%v/%v", i, txn.Hash(), hash, number, index, block.Hash(), block.NumberU64(), i)
					}
					if txn.Hash() != txn2.Hash() {
						t.Fatalf("txn #%d [%x]: transaction mismatch: have %v, want %v", i, txn.Hash(), txn, txn2)
					}
				}
			}
			// Delete the transactions and check purge
			for i, txn := range txs {
				if err := rawdb.DeleteTxLookupEntry(tx, txn.Hash()); err != nil {
					t.Fatal(err)
				}
				if txn2, _, _, _, _ := readTransactionByHash(tx, txn.Hash(), br); txn2 != nil {
					t.Fatalf("txn #%d [%x]: deleted transaction returned: %v", i, txn.Hash(), txn2)
				}
			}
		})
	}
}

// ReadTransactionByHash retrieves a specific transaction from the database, along with
// its added positional metadata.
func readTransactionByHash(db kv.Tx, hash libcommon.Hash, br services.FullBlockReader) (types.Transaction, libcommon.Hash, uint64, uint64, error) {
	blockNumber, err := rawdb.ReadTxLookupEntry(db, hash)
	if err != nil {
		return nil, libcommon.Hash{}, 0, 0, err
	}
	if blockNumber == nil {
		return nil, libcommon.Hash{}, 0, 0, nil
	}
	blockHash, err := br.CanonicalHash(context.Background(), db, *blockNumber)
	if err != nil {
		return nil, libcommon.Hash{}, 0, 0, err
	}
	if blockHash == (libcommon.Hash{}) {
		return nil, libcommon.Hash{}, 0, 0, nil
	}
	body, _ := br.BodyWithTransactions(context.Background(), db, blockHash, *blockNumber)
	if body == nil {
		log.Error("Transaction referenced missing", "number", blockNumber, "hash", blockHash)
		return nil, libcommon.Hash{}, 0, 0, nil
	}
	senders, err1 := rawdb.ReadSenders(db, blockHash, *blockNumber)
	if err1 != nil {
		return nil, libcommon.Hash{}, 0, 0, err1
	}
	body.SendersToTxs(senders)
	for txIndex, tx := range body.Transactions {
		if tx.Hash() == hash {
			return tx, blockHash, *blockNumber, uint64(txIndex), nil
		}
	}
	log.Error("Transaction not found", "number", blockNumber, "hash", blockHash, "txhash", hash)
	return nil, libcommon.Hash{}, 0, 0, nil
}
