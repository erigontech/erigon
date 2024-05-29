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
	"bytes"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
	"github.com/stretchr/testify/require"
)

// Tests block header storage and retrieval operations.
func TestCanonicalIter(t *testing.T) {
	t.Parallel()
	m, require := mock.Mock(t), require.New(t)

	txn := &types.DynamicFeeTransaction{Tip: u256.N1, FeeCap: u256.N1, ChainID: u256.N1, CommonTx: types.CommonTx{Value: u256.N1, Gas: 1, Nonce: 1}}
	buf := bytes.NewBuffer(nil)
	err := txn.MarshalBinary(buf)
	require.NoError(err)
	rlpTxn := buf.Bytes()
	b := &types.RawBody{Transactions: [][]byte{rlpTxn, rlpTxn, rlpTxn}}

	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(err)
	defer tx.Rollback()

	// write 2 forks - 3 blocks in each fork
	_, err = rawdb.WriteRawBodyIfNotExists(tx, libcommon.Hash{10}, 0, b)
	require.NoError(err)
	_, err = rawdb.WriteRawBodyIfNotExists(tx, libcommon.Hash{20}, 0, b)
	require.NoError(err)

	_, err = rawdb.WriteRawBodyIfNotExists(tx, libcommon.Hash{11}, 1, b)
	require.NoError(err)
	_, err = rawdb.WriteRawBodyIfNotExists(tx, libcommon.Hash{21}, 1, b)
	require.NoError(err)

	_, err = rawdb.WriteRawBodyIfNotExists(tx, libcommon.Hash{12}, 2, b)
	require.NoError(err)
	_, err = rawdb.WriteRawBodyIfNotExists(tx, libcommon.Hash{22}, 2, b)
	require.NoError(err)

	//mark 3 blocks as canonical
	require.NoError(rawdb.WriteCanonicalHash(tx, libcommon.Hash{10}, 0))
	require.NoError(rawdb.WriteCanonicalHash(tx, libcommon.Hash{11}, 1))
	require.NoError(rawdb.WriteCanonicalHash(tx, libcommon.Hash{12}, 2))

	//it, err := rawdb.TxnIdsOfCanonicalBlocks(tx, 0, 1, order.Asc, -1)
	//require.NoError(err)
	//require.Equal(false, it.HasNext())
	//require.Equal(0, len(iter.ToArrU64Must(it)))

	txNumsOfBlock := func(bn uint64) []uint64 {
		return []uint64{2 + (4 * bn) + 0, 2 + (4 * bn) + 1, 2 + (4 * bn) + 2, 2 + (4 * bn) + 3}
	}
	it, err := rawdb.TxnIdsOfCanonicalBlocks(tx, 0, -1, order.Asc, -1)
	require.NoError(err)
	require.Equal(true, it.HasNext())
	require.Equal(append(append(txNumsOfBlock(0), txNumsOfBlock(2)...), txNumsOfBlock(4)...), iter.ToArrU64Must(it))

	t.Fatal("TODO: add order.Desc support")
}
