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

package l1_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/l1test"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
)

// TestNativeConformance is T-L1-1 for the native reader over an in-process
// Erigon L1.
func TestNativeConformance(t *testing.T) {
	m, model := l1test.ErigonL1(t)
	l1test.Conformance(t, l1.NewNative(m.DB, m.BlockReader, m.ReceiptsReader, m.ChainConfig), model, l1test.SampleFilters)
}

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

func (r hidingReader) TxnHashByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, i int) (common.Hash, bool, error) {
	if blockNum == r.block {
		return common.Hash{}, false, nil
	}
	return r.FullBlockReader.TxnHashByIdxInBlock(ctx, tx, blockNum, i)
}

// TestNativeMissingTransaction: a matched log whose transaction the node
// cannot read fails the query instead of vanishing from it.
func TestNativeMissingTransaction(t *testing.T) {
	m, model := l1test.ErigonL1(t)
	ctx := t.Context()
	head, err := model.Head(ctx)
	require.NoError(t, err)
	f := l1test.SampleFilters[0]
	logs, err := model.Logs(ctx, 0, head.Number, f.Address, f.Topics)
	require.NoError(t, err)
	require.NotEmpty(t, logs)

	hiding := hidingReader{m.BlockReader, uint64(logs[0].BlockNumber)}
	native := l1.NewNative(m.DB, hiding, receipts.NewGenerator(m.Dirs, hiding, m.Engine, nil, time.Minute), m.ChainConfig)
	_, err = native.Logs(ctx, 0, head.Number, f.Address, f.Topics)
	require.ErrorContains(t, err, "not found")
}
