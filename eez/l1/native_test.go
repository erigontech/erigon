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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/l1test"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
)

// TestNativeConformance is T-L1-1 for the native reader over an in-process
// Erigon L1.
func TestNativeConformance(t *testing.T) {
	m, model := l1test.ErigonL1(t)
	l1test.Conformance(t, l1.NewNative(m.DB, m.BlockReader, m.ReceiptsReader, m.Engine, m.ChainConfig), model, l1test.SampleFilters)
}

// hidingReader serves no transaction of one block, as a node missing its
// body would, and with header set no header of it either.
type hidingReader struct {
	dbservices.FullBlockReader
	block  uint64
	header bool
}

func (r hidingReader) HeaderByNumber(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.Header, error) {
	if r.header && blockNum == r.block {
		return nil, nil
	}
	return r.FullBlockReader.HeaderByNumber(ctx, tx, blockNum)
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

// TestNativeMissingBlockData: a matched log whose transaction or header the
// node cannot read fails the query instead of vanishing from it, as a block
// the source does not serve, which a scan of the range reports by number.
func TestNativeMissingBlockData(t *testing.T) {
	m, model := l1test.ErigonL1(t)
	ctx := t.Context()
	head, err := model.Head(ctx)
	require.NoError(t, err)
	f := l1test.SampleFilters[0]
	logs, err := model.Logs(ctx, 0, head.Number, f.Address, f.Topics)
	require.NoError(t, err)
	require.NotEmpty(t, logs)
	block := uint64(logs[0].BlockNumber)

	for _, c := range []struct {
		name   string
		header bool
		msg    string
	}{
		{"transaction", false, fmt.Sprintf("transaction not found at block %d, index %d", block, logs[0].TxIndex)},
		{"header", true, fmt.Sprintf("header not found: %d", block)},
	} {
		t.Run(c.name, func(t *testing.T) {
			hiding := hidingReader{FullBlockReader: m.BlockReader, block: block, header: c.header}
			native := l1.NewNative(m.DB, hiding, receipts.NewGenerator(m.Dirs, hiding, m.Engine, nil, time.Minute), m.Engine, m.ChainConfig)
			_, err := native.Logs(ctx, 0, head.Number, f.Address, f.Topics)
			require.ErrorContains(t, err, c.msg)
			require.ErrorIs(t, err, l1.ErrNotFound)
			var u *l1.UnservedError
			require.ErrorAs(t, err, &u)
			require.Equal(t, block, u.Block)
			require.ErrorAs(t, l1.UnservedLogs(ctx, native, err, 0, head.Number, f.Address, f.Topics), &u)
			require.Equal(t, block, u.Block)
		})
	}
}

// staleCanonical reports hash canonical at number, as a block reader that
// answers from a newer view than the read transaction would.
type staleCanonical struct {
	dbservices.FullBlockReader
	number uint64
	hash   common.Hash
}

func (r staleCanonical) CanonicalHash(ctx context.Context, tx kv.Getter, number uint64) (common.Hash, bool, error) {
	if number == r.number {
		return r.hash, true, nil
	}
	return r.FullBlockReader.CanonicalHash(ctx, tx, number)
}

// TestNativeCanonicalInReadTx: a block off the canonical chain of the read
// transaction is refused even when the block reader calls it canonical.
func TestNativeCanonicalInReadTx(t *testing.T) {
	m, model := l1test.ErigonL1(t)
	orphans := model.Orphans()
	require.NotEmpty(t, orphans)
	orphan := orphans[0].Header
	blocks := staleCanonical{m.BlockReader, orphan.Number, orphan.Hash}
	native := l1.NewNative(m.DB, blocks, receipts.NewGenerator(m.Dirs, blocks, m.Engine, nil, time.Minute), m.Engine, m.ChainConfig)
	_, err := native.CodeAt(t.Context(), l1test.Registry, orphan.Hash)
	require.ErrorIs(t, err, l1.ErrNotCanonical)
	_, err = native.CallAt(t.Context(), l1test.RollupsCall, orphan.Hash)
	require.ErrorIs(t, err, l1.ErrNotCanonical)
}

// TestStateAboveExecutedHead: state reads at a canonical block above the
// executed head are not served.
func TestStateAboveExecutedHead(t *testing.T) {
	m, model := l1test.ErigonL1(t)
	ctx := t.Context()
	head, err := model.Head(ctx)
	require.NoError(t, err)
	require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Execution, head.Number-1)
	}))
	for name, r := range map[string]l1.StateReader{
		"native": l1.NewNative(m.DB, m.BlockReader, m.ReceiptsReader, m.Engine, m.ChainConfig),
		"rpc":    erigonRPC(t, m),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := r.CodeAt(ctx, l1test.Registry, head.Hash)
			require.ErrorIs(t, err, l1.ErrNotFound)
			require.NotErrorIs(t, err, l1.ErrNotCanonical)
			_, err = r.CallAt(ctx, l1test.RollupsCall, head.Hash)
			require.ErrorIs(t, err, l1.ErrNotFound)
			require.NotErrorIs(t, err, l1.ErrNotCanonical)
		})
	}
}
