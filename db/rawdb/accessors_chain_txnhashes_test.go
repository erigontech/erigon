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

package rawdb_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/rawdb"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/types"
)

// txnHashesTestTxns builds one txn of each storage shape the hash path has to
// handle: a legacy txn, stored as a bare RLP list, and typed ones, stored as
// that list wrapped in an RLP string.
func txnHashesTestTxns(t *testing.T, n int) []types.Transaction {
	t.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	to := crypto.PubkeyToAddress(key.PublicKey)
	chainID := uint256.NewInt(chainspec.Mainnet.Config.ChainID.Uint64())
	signer := types.LatestSignerForChainID(chainID)

	txns := make([]types.Transaction, 0, n)
	for i := range n {
		var unsigned types.Transaction
		if i%2 == 0 {
			unsigned = types.NewTransaction(uint64(i), to, &u256.Num1, 21000, &u256.Num1, []byte{byte(i)})
		} else {
			unsigned = types.NewEIP1559Transaction(*chainID, uint64(i), to, &u256.Num1, 21000, &u256.Num1, &u256.Num1, &u256.Num1, []byte{byte(i)})
		}
		signed, err := types.SignTx(unsigned, *signer, key)
		require.NoError(t, err)
		txns = append(txns, signed)
	}
	return txns
}

func writeTxnHashesTestTxns(t *testing.T, tx kv.RwTx, baseTxnID uint64, txns []types.Transaction) {
	t.Helper()
	require.NoError(t, rawdb.WriteTransactions(tx, txns, types.BaseTxnID(baseTxnID)))
}

// TestCanonicalTransactionHashesMatchesDecode pins the hash-without-decoding path
// to the decode-then-Hash path it replaces, over the stored bytes rather than an
// in-memory encoding, so a change in what WriteTransactions persists breaks it.
func TestCanonicalTransactionHashesMatchesDecode(t *testing.T) {
	t.Parallel()
	_, tx := memdb.NewTestTx(t)

	const baseTxnID = 7
	txns := txnHashesTestTxns(t, 6)
	writeTxnHashesTestTxns(t, tx, baseTxnID, txns)

	want, err := rawdb.CanonicalTransactions(tx, baseTxnID, uint32(len(txns)))
	require.NoError(t, err)
	require.Len(t, want, len(txns))

	got, err := rawdb.CanonicalTransactionHashes(tx, baseTxnID, uint32(len(txns)))
	require.NoError(t, err)
	require.Len(t, got, len(txns))

	for i := range want {
		require.Equal(t, want[i].Hash(), got[i], "txn %d (type %d)", i, want[i].Type())
	}
}

func TestCanonicalTransactionHashesEmpty(t *testing.T) {
	t.Parallel()
	_, tx := memdb.NewTestTx(t)

	// Non-nil empty, not nil: callers read nil as "body not found".
	got, err := rawdb.CanonicalTransactionHashes(tx, 1, 0)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Empty(t, got)
}

// TestCanonicalTransactionHashesShortRead pins the truncation CanonicalTransactions
// also does: asking for more than the db holds returns what was found, not an error.
func TestCanonicalTransactionHashesShortRead(t *testing.T) {
	t.Parallel()
	_, tx := memdb.NewTestTx(t)

	const baseTxnID = 1
	txns := txnHashesTestTxns(t, 2)
	writeTxnHashesTestTxns(t, tx, baseTxnID, txns)

	got, err := rawdb.CanonicalTransactionHashes(tx, baseTxnID, 5)
	require.NoError(t, err)
	require.Len(t, got, len(txns))
}

// TestReadBodyTxnHashesNilVsEmpty pins the contract the txn lookup stage branches
// on: a missing body is nil (warn and skip), a body with no txns is a non-nil
// empty slice (index nothing, no warning).
func TestReadBodyTxnHashesNilVsEmpty(t *testing.T) {
	t.Parallel()
	_, tx := memdb.NewTestTx(t)

	missing, err := rawdb.ReadBodyTxnHashes(tx, common.Hash{0xaa}, 1)
	require.NoError(t, err)
	require.Nil(t, missing)

	header := &types.Header{Number: *common.Num1}
	require.NoError(t, rawdb.WriteBody(tx, header.Hash(), 1, &types.Body{}))

	empty, err := rawdb.ReadBodyTxnHashes(tx, header.Hash(), 1)
	require.NoError(t, err)
	require.NotNil(t, empty)
	require.Empty(t, empty)
}

// TestReadBodyTxnHashesMatchesBodyWithTransactions pins the two readers against
// each other on a written body, so the stage's switch from one to the other
// cannot change which hashes land in the index.
func TestReadBodyTxnHashesMatchesBodyWithTransactions(t *testing.T) {
	t.Parallel()
	_, tx := memdb.NewTestTx(t)

	header := &types.Header{Number: *common.Num1}
	body := &types.Body{Transactions: txnHashesTestTxns(t, 4)}
	require.NoError(t, rawdb.WriteBody(tx, header.Hash(), 1, body))

	full, err := rawdb.ReadBodyWithTransactions(tx, header.Hash(), 1)
	require.NoError(t, err)
	require.NotNil(t, full)

	hashes, err := rawdb.ReadBodyTxnHashes(tx, header.Hash(), 1)
	require.NoError(t, err)
	require.Len(t, hashes, len(full.Transactions))

	for i, txn := range full.Transactions {
		require.Equal(t, txn.Hash(), hashes[i], "txn %d (type %d)", i, txn.Type())
	}
}
