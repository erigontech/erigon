// Copyright 2018 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"bytes"
	"slices"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
	"github.com/stretchr/testify/require"
)

func txNumsOfBlock(bn uint64, b *types.RawBody) (res []uint64) {
	txns := uint64(types.TxCountToTxAmount(len(b.Transactions)))
	s := bn * txns
	for i := uint64(0); i < txns; i++ {
		res = append(res, s+i)
	}
	return res
}

// Tests block header storage and retrieval operations.
func TestCanonicalIter(t *testing.T) {
	t.Parallel()
	m, require := mock.Mock(t), require.New(t)

	txn := &types.DynamicFeeTransaction{Tip: u256.N1, FeeCap: u256.N1, ChainID: u256.N1, CommonTx: types.CommonTx{Value: u256.N1, Gas: 1, Nonce: 1}}
	buf := bytes.NewBuffer(nil)
	err := txn.MarshalBinary(buf)
	require.NoError(err)
	rlpTxn := buf.Bytes()
	b := &types.RawBody{Transactions: [][]byte{rlpTxn, rlpTxn, rlpTxn, rlpTxn}}

	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(err)
	defer tx.Rollback()

	// write 2 forks - 3 blocks in each fork
	_, err = rawdb.WriteRawBodyIfNotExists(tx, common.Hash{10}, 0, b)
	require.NoError(err)
	_, err = rawdb.WriteRawBodyIfNotExists(tx, common.Hash{20}, 0, b)
	require.NoError(err)

	_, err = rawdb.WriteRawBodyIfNotExists(tx, common.Hash{11}, 1, b)
	require.NoError(err)
	_, err = rawdb.WriteRawBodyIfNotExists(tx, common.Hash{21}, 1, b)
	require.NoError(err)

	_, err = rawdb.WriteRawBodyIfNotExists(tx, common.Hash{12}, 2, b)
	require.NoError(err)
	_, err = rawdb.WriteRawBodyIfNotExists(tx, common.Hash{22}, 2, b)
	require.NoError(err)

	//it, err := rawdb.TxnIdsOfCanonicalBlocks(tx, 0, -1, order.Asc, -1)
	//require.NoError(err)
	//require.Equal(true, it.HasNext())
	//
	//// tx already contains genesis block of 2 transactions
	//t.Logf("genesis: %v", iter.ToArrU64Must(it))
	//

	//mark 3 blocks as canonical
	require.NoError(rawdb.WriteCanonicalHash(tx, common.Hash{10}, 0))
	require.NoError(rawdb.WriteCanonicalHash(tx, common.Hash{11}, 1))
	require.NoError(rawdb.WriteCanonicalHash(tx, common.Hash{12}, 2))
	require.NoError(rawdb.AppendCanonicalTxNums(tx, 0))

	//it, err := rawdb.TxnIdsOfCanonicalBlocks(tx, 0, int(types.TxCountToTxAmount(len(b.Transactions))), order.Asc, -1)
	//require.NoError(err)
	//require.Equal(true, it.HasNext())
	//exp := txNumsOfBlock(0, b)
	//t.Logf("expected full block 0: %v", exp)
	//require.Equal(exp, iter.ToArrU64Must(it))

	it, err := rawdb.TxnIdsOfCanonicalBlocks(tx, 0, -1, order.Asc, -1)
	require.NoError(err)
	require.Equal(true, it.HasNext())
	exp := append(append(txNumsOfBlock(0, b), txNumsOfBlock(2, b)...), txNumsOfBlock(4, b)...)
	t.Logf("expected %v", exp)
	require.Equal(exp, iter.ToArrU64Must(it))

	{ //start from middle of block
		it, err = rawdb.TxnIdsOfCanonicalBlocks(tx, 3, -1, order.Asc, -1)
		require.NoError(err)
		require.Equal(true, it.HasNext())
		t.Logf("reverse expected %v", exp)
		require.Equal(exp[1:], iter.ToArrU64Must(it))
	}

	//reverse
	rit, err := rawdb.TxnIdsOfCanonicalBlocks(tx, -1, -1, order.Desc, -1)
	require.NoError(err)
	require.Equal(true, rit.HasNext())
	slices.Reverse(exp)
	t.Logf("reverse expected %v", exp)
	require.Equal(exp, iter.ToArrU64Must(rit))
	{ //start from middle of block
	}

}
