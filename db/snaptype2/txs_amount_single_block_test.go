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

package snaptype2

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// writeBodiesSegment lays down a bodies .seg holding one BodyForStorage per
// block, the way DumpBodies does.
func writeBodiesSegment(t *testing.T, path string, bodies []*types.BodyForStorage) *seg.Decompressor {
	t.Helper()
	c, err := seg.NewCompressor(t.Context(), "test", path, t.TempDir(), seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer c.Close()
	c.DisableFsync()
	for _, b := range bodies {
		enc, encErr := rlp.EncodeToBytes(b)
		require.NoError(t, encErr)
		require.NoError(t, c.AddWord(enc))
	}
	require.NoError(t, c.Compress())
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	t.Cleanup(d.Close)
	return d
}

// An unwind cuts a boundary segment at its target, so retire emits ranges of
// one block — and the transaction count for those is derived by walking to the
// entry at index len-1, which for a single block is index 0 and is never
// reached. lastBody keeps its zero value, the derivation reads that as a count
// running backwards, and retire fails the whole range:
//
//	DumpBlocks: negative txs count v1.1-3845999-3846000-bodies.seg:
//	  lastBody.BaseTxId=0 < firstBody.BaseTxId=135267735
func TestTxsAmountForSingleBlockSegment(t *testing.T) {
	t.Parallel()

	const base = types.BaseTxnID(135267735)
	one := &types.BodyForStorage{BaseTxnID: base, TxCount: 3}
	bodies := []*types.BodyForStorage{one}
	dec := writeBodiesSegment(t, filepath.Join(t.TempDir(), "v1.1-test-bodies.seg"), bodies)

	// Callers pass segment length minus one, as block_types.go:221 does.
	gotBase, gotCount, err := TxsAmountBasedOnBodiesSnapshots(dec, uint64(len(bodies))-1)
	require.NoError(t, err, "a one-block range is what an unwind boundary produces; it must not read as a negative count")
	require.Equal(t, base, gotBase)
	require.Equal(t, int(one.BaseTxnID.LastSystemTx(one.TxCount)+1-base.U64()), gotCount,
		"the count must cover the block's own transactions plus its system txn")
}
