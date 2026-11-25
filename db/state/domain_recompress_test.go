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

package state_test

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
)

func TestRecompressDomainFiles(t *testing.T) {
	const stepSize, txs, keys = 2, 40, 50
	ctx := t.Context()
	db, agg := testDbAndAggregatorv3(t, stepSize)
	key := func(i int) []byte { return binary.BigEndian.AppendUint64(make([]byte, 44), uint64(i)) }

	agg.SetCompressionForTest(kv.StorageDomain, seg.CompressNone)
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	for txNum := range uint64(txs) {
		for i := range keys {
			require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, key(i), binary.BigEndian.AppendUint64([]byte("value"), txNum), txNum, nil))
		}
	}
	require.NoError(t, domains.Flush(ctx, tx))
	domains.Close()
	require.NoError(t, tx.Commit())
	require.NoError(t, agg.BuildFiles(db, txs, unboundedFinalityCtx))

	latest := func() [][]byte {
		roTx, beginErr := db.BeginTemporalRo(ctx)
		require.NoError(t, beginErr)
		defer roTx.Rollback()
		var vals [][]byte
		for i := range keys {
			v, _, getErr := roTx.GetLatest(kv.StorageDomain, key(i), kv.GetLatestOptions{})
			require.NoError(t, getErr)
			vals = append(vals, v)
		}
		return vals
	}
	before := latest()

	agg.SetCompressionForTest(kv.StorageDomain, seg.CompressKeys)
	res, err := agg.RecompressDomainFiles(ctx, kv.StorageDomain, true, log.New())
	require.NoError(t, err)
	require.Equal(t, state.RecompressResult{Changed: 1, Same: 2}, res)
	backups, err := dir.Glob(filepath.Join(agg.Dirs().Snap, "patch-backup", "*"))
	require.NoError(t, err)
	require.Empty(t, backups)

	res, err = agg.RecompressDomainFiles(ctx, kv.StorageDomain, false, log.New())
	require.NoError(t, err)
	require.Equal(t, state.RecompressResult{Changed: 1, Same: 2}, res)
	require.Equal(t, before, latest())

	res, err = agg.RecompressDomainFiles(ctx, kv.StorageDomain, false, log.New())
	require.NoError(t, err)
	require.Equal(t, state.RecompressResult{Same: 3}, res)
}
