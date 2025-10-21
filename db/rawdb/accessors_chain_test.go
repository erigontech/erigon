// Copyright 2025 The Erigon Authors
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

package rawdb

import (
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

func TestWriteRawTransactions(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	rawTx1 := []byte("raw_transaction_1")
	rawTx2 := []byte("raw_transaction_2")
	rawTx3 := []byte("raw_transaction_3")
	rawTxs := [][]byte{rawTx1, rawTx2, rawTx3}

	baseTxnID := types.BaseTxnID(100)

	err := WriteRawTransactions(tx, rawTxs, baseTxnID)
	require.NoError(t, err)

	for i, expectedRawTx := range rawTxs {
		txnID := baseTxnID.At(i)
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, txnID)

		actualRawTx, err := tx.GetOne(kv.EthTx, key)
		require.NoError(t, err)
		require.Equal(t, expectedRawTx, actualRawTx)
	}
}

func TestWriteRawTransactions_EmptySlice(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	// Test with empty slice
	err := WriteRawTransactions(tx, [][]byte{}, types.BaseTxnID(100))
	require.NoError(t, err)

	// Verify no transactions were written
	cursor, err := tx.Cursor(kv.EthTx)
	require.NoError(t, err)
	defer cursor.Close()

	k, _, err := cursor.First()
	require.NoError(t, err)
	require.Nil(t, k) // No data should be present
}

func TestWriteTransactions(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	tx1 := types.NewTransaction(0, common.HexToAddress("0x1234"), uint256.NewInt(100), 21000, uint256.NewInt(1000000000), []byte{})
	tx2 := types.NewTransaction(1, common.HexToAddress("0x5678"), uint256.NewInt(200), 21000, uint256.NewInt(2000000000), []byte{})
	tx3 := types.NewTransaction(2, common.HexToAddress("0x9abc"), uint256.NewInt(300), 21000, uint256.NewInt(3000000000), []byte{})
	txs := []types.Transaction{tx1, tx2, tx3}

	baseTxnID := types.BaseTxnID(200)

	err := WriteTransactions(tx, txs, baseTxnID)
	require.NoError(t, err)

	for i, expectedTx := range txs {
		txnID := baseTxnID.At(i)
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, txnID)

		rawTxData, err := tx.GetOne(kv.EthTx, key)
		require.NoError(t, err)
		require.NotEmpty(t, rawTxData)

		decodedTx, err := types.DecodeTransaction(rawTxData)
		require.NoError(t, err)

		require.Equal(t, expectedTx.GetNonce(), decodedTx.GetNonce())
		require.Equal(t, expectedTx.GetTo(), decodedTx.GetTo())
		require.Equal(t, expectedTx.GetValue(), decodedTx.GetValue())
		require.Equal(t, expectedTx.GetGasLimit(), decodedTx.GetGasLimit())
		require.Equal(t, expectedTx.GetFeeCap(), decodedTx.GetFeeCap())
		require.Equal(t, expectedTx.GetData(), decodedTx.GetData())
	}
}

func TestWriteTransactions_EmptySlice(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	err := WriteTransactions(tx, []types.Transaction{}, types.BaseTxnID(300))
	require.NoError(t, err)

	cursor, err := tx.Cursor(kv.EthTx)
	require.NoError(t, err)
	defer cursor.Close()

	k, _, err := cursor.First()
	require.NoError(t, err)
	require.Nil(t, k) // No data should be present
}

func TestWriteTransactions_WrapperBehavior(t *testing.T) {
	// This test verifies that WriteTransactions is truly a wrapper around WriteRawTransactions
	// by comparing the results of both functions with the same input

	_, tx1 := memdb.NewTestTx(t)
	defer tx1.Rollback()

	_, tx2 := memdb.NewTestTx(t)
	defer tx2.Rollback()

	tx := types.NewTransaction(1337, common.HexToAddress("0x1234"), uint256.NewInt(100), 21000, uint256.NewInt(1000000000), []byte{})
	txs := []types.Transaction{tx}

	baseTxnID := types.BaseTxnID(400)

	{
		err := WriteTransactions(tx1, txs, baseTxnID)
		require.NoError(t, err)
	}

	encodedTx, err := rlp.EncodeToBytes(tx)
	require.NoError(t, err)
	rawTxs := [][]byte{encodedTx}

	err = WriteRawTransactions(tx2, rawTxs, baseTxnID)
	require.NoError(t, err)

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, baseTxnID.At(0))

	data1, err := tx1.GetOne(kv.EthTx, key)
	require.NoError(t, err)

	data2, err := tx2.GetOne(kv.EthTx, key)
	require.NoError(t, err)

	require.Equal(t, data1, data2, "WriteTransactions and WriteRawTransactions should produce identical results")
}

func TestWriteTransactions_SequentialTxnIDs(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	txs := make([]types.Transaction, 5)
	for i := 0; i < 5; i++ {
		txs[i] = types.NewTransaction(uint64(i), common.HexToAddress("0x1234"), uint256.NewInt(uint64(i*100)), 21000, uint256.NewInt(1000000000), []byte{})
	}

	baseTxnID := types.BaseTxnID(500)

	err := WriteTransactions(tx, txs, baseTxnID)
	require.NoError(t, err)

	// Verify each transaction was stored with the correct sequential ID
	for i := 0; i < 5; i++ {
		expectedTxnID := baseTxnID.At(i)
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, expectedTxnID)

		rawTxData, err := tx.GetOne(kv.EthTx, key)
		require.NoError(t, err)
		require.NotEmpty(t, rawTxData, "Transaction %d should be stored", i)

		decodedTx, err := types.DecodeTransaction(rawTxData)
		require.NoError(t, err)
		require.Equal(t, uint64(i), decodedTx.GetNonce(), "Transaction %d should have correct nonce", i)
	}
}

func TestWriteRawTransactions_SequentialTxnIDs(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	// Create test raw transaction data
	rawTxs := [][]byte{
		[]byte("transaction_0"),
		[]byte("transaction_1"),
		[]byte("transaction_2"),
	}

	baseTxnID := types.BaseTxnID(600)

	err := WriteRawTransactions(tx, rawTxs, baseTxnID)
	require.NoError(t, err)

	// Verify each transaction was stored with the correct sequential ID
	for i, expectedData := range rawTxs {
		expectedTxnID := baseTxnID.At(i)
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, expectedTxnID)

		actualData, err := tx.GetOne(kv.EthTx, key)
		require.NoError(t, err)
		require.Equal(t, expectedData, actualData, "Transaction %d should have correct data", i)
	}
}

func TestWriteTransactions_UniqueKeys(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	txs := make([]types.Transaction, 5)
	for i := 0; i < 5; i++ {
		txs[i] = types.NewTransaction(uint64(i), common.HexToAddress("0x1234"), uint256.NewInt(uint64(i*100)), 21000, uint256.NewInt(1000000000), []byte{})
	}

	baseTxnID := types.BaseTxnID(700)

	err := WriteTransactions(tx, txs, baseTxnID)
	require.NoError(t, err)

	usedKeys := make(map[string]bool)
	keysList := make([][]byte, 0, 5)

	for i := 0; i < 5; i++ {
		expectedTxnID := baseTxnID.At(i)
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, expectedTxnID)

		keyStr := string(key)
		require.False(t, usedKeys[keyStr], "Key %x should be unique, but was already used for transaction %d", key, i)
		usedKeys[keyStr] = true
		keysList = append(keysList, append([]byte(nil), key...)) // Copy the key

		rawTxData, err := tx.GetOne(kv.EthTx, key)
		require.NoError(t, err)
		require.NotEmpty(t, rawTxData, "Transaction %d should be stored", i)
	}

	// Verify the keys are sequential (which also ensures they're unique)
	for i := 1; i < len(keysList); i++ {
		prevID := binary.BigEndian.Uint64(keysList[i-1])
		currID := binary.BigEndian.Uint64(keysList[i])
		require.Equal(t, prevID+1, currID, "Transaction IDs should be sequential (ID %d should be %d but got %d)", i, prevID+1, currID)
	}
}

func TestWriteRawTransactions_UniqueKeys(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	defer tx.Rollback()

	rawTxs := [][]byte{
		[]byte("transaction_0"),
		[]byte("transaction_1"),
		[]byte("transaction_2"),
		[]byte("transaction_3"),
	}

	baseTxnID := types.BaseTxnID(800)

	err := WriteRawTransactions(tx, rawTxs, baseTxnID)
	require.NoError(t, err)

	usedKeys := make(map[string]bool)
	keysList := make([][]byte, 0, len(rawTxs))

	for i, expectedData := range rawTxs {
		expectedTxnID := baseTxnID.At(i)
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, expectedTxnID)

		keyStr := string(key)
		require.False(t, usedKeys[keyStr], "Key %x should be unique, but was already used for transaction %d", key, i)
		usedKeys[keyStr] = true
		keysList = append(keysList, append([]byte(nil), key...)) // Copy the key

		actualData, err := tx.GetOne(kv.EthTx, key)
		require.NoError(t, err)
		require.Equal(t, expectedData, actualData, "Transaction %d should have correct data", i)
	}

	// Verify the keys are sequential (which also ensures they're unique)
	for i := 1; i < len(keysList); i++ {
		prevID := binary.BigEndian.Uint64(keysList[i-1])
		currID := binary.BigEndian.Uint64(keysList[i])
		require.Equal(t, prevID+1, currID, "Transaction IDs should be sequential (ID %d should be %d but got %d)", i, prevID+1, currID)
	}
}
