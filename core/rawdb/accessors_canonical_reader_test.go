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
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/turbo/stages/mock"
	"github.com/stretchr/testify/require"
)

func txNumsOfBlock(bn uint64, b *types.RawBody) (res []uint64) {
	var txns uint64 = 2 // genesis amount of txs
	s := uint64(0)
	if bn > 0 {
		txns = uint64(types.TxCountToTxAmount(len(b.Transactions)))
		s = 2 + (bn-1)*txns
	}
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
	////tx already contains genesis block of 2 transactions
	//t.Logf("genesis: %v", stream.ToArrU64Must(it))

	//mark 3 blocks as canonical
	require.NoError(rawdb.WriteCanonicalHash(tx, common.Hash{11}, 1))
	require.NoError(rawdb.WriteCanonicalHash(tx, common.Hash{12}, 2))
	require.NoError(rawdb.AppendCanonicalTxNums(tx, 0))

	//start-end in teh middle fo block
	it, err := rawdb.TxnIdsOfCanonicalBlocks(tx, rawdbv3.TxNums, 1, 6, order.Asc, -1)
	require.NoError(err)
	require.Equal(true, it.HasNext())
	require.Equal([]uint64{1, 2, 3, 4, 5}, stream.ToArrU64Must(it))

	it, err = rawdb.TxnIdsOfCanonicalBlocks(tx, rawdbv3.TxNums, 0, -1, order.Asc, -1)
	require.NoError(err)
	require.Equal(true, it.HasNext())
	exp := append(append(txNumsOfBlock(0, b), txNumsOfBlock(1, b)...), txNumsOfBlock(3, b)...)
	require.Equal(exp, stream.ToArrU64Must(it))

	//TODO: create .ap files

	//reverse
	//TODO: reverse
	//rit, err := rawdb.TxnIdsOfCanonicalBlocks(tx, rawdbv3.TxNums, -1, -1, order.Desc, -1)
	//require.NoError(err)
	//require.Equal(true, rit.HasNext())
	//slices.Reverse(exp)
	//t.Logf("reverse expected %v", exp)
	//require.Equal(exp, stream.ToArrU64Must(rit))
	//{ //start from middle of block
	//}
}
