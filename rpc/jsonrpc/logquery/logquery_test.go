// Copyright 2026 The Erigon Authors
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

package logquery_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/jsonrpc/logquery"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
)

// hidingReader serves no transaction of one block, as a node missing its
// body would.
type hidingReader struct {
	dbservices.FullBlockReader
	block uint64
}

func (r hidingReader) TxnByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, i int) (types.Transaction, bool, error) {
	if blockNum == r.block {
		return nil, false, nil
	}
	return r.FullBlockReader.TxnByIdxInBlock(ctx, tx, blockNum, i)
}

func (r hidingReader) TxnHashByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum, txNum uint64, i int) (common.Hash, bool, error) {
	if blockNum == r.block {
		return common.Hash{}, false, nil
	}
	return r.FullBlockReader.TxnHashByIdxInBlock(ctx, tx, blockNum, txNum, i)
}

// TestMissingTransaction: a matched log whose transaction cannot be read
// fails the query instead of vanishing from it.
func TestMissingTransaction(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	const block = 10
	query := func(reader dbservices.FullBlockReader) logquery.Query {
		return logquery.Query{
			TxNums: reader.TxnumReader(), Headers: reader, Txns: reader,
			Receipts: receipts.NewGenerator(m.Dirs, reader, m.Engine, nil, time.Minute),
		}
	}

	q := query(m.BlockReader)
	all, err := q.Logs(t.Context(), tx, m.ChainConfig, block, block, filters.FilterCriteria{}, 0)
	require.NoError(t, err)
	require.NotEmpty(t, all)

	q = query(hidingReader{m.BlockReader, block})
	_, err = q.Logs(t.Context(), tx, m.ChainConfig, block, block, filters.FilterCriteria{}, 0)
	require.ErrorContains(t, err, "transaction not found")
}
