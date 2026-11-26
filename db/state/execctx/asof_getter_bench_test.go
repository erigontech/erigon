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

package execctx_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

func BenchmarkAsOfGetterLatestOnlyKeys(b *testing.B) {
	const stepSize, keyCount = 16, 1000
	ctx := b.Context()
	db := newTestDb(b, stepSize)
	stateCache := newSmallStateCache()
	defer stateCache.Close()

	keys := make([][]byte, keyCount)
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(b, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(b, err)
	defer sd.Close()
	sd.SetTxNum(5)
	for i := range keys {
		keys[i] = make([]byte, 20)
		binary.BigEndian.PutUint64(keys[i][4:], uint64(i)+1)
		require.NoError(b, sd.DomainPut(kv.AccountsDomain, rwTx, keys[i], encAccount(uint64(i)+1), 5, nil))
	}
	sd.SetTxNum(40)
	require.NoError(b, sd.DomainPut(kv.AccountsDomain, rwTx, []byte{0x01}, encAccount(1), 40, nil))
	_, err = sd.ComputeCommitment(ctx, rwTx, true, 1, 40, "", nil)
	require.NoError(b, err)
	require.NoError(b, sd.Commit(ctx, rwTx))
	sd.Close()
	require.NoError(b, rwTx.Commit())
	require.NoError(b, db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(db, stepSize, unboundedFinalityCtx))
	require.NoError(b, db.Update(ctx, func(tx kv.RwTx) error {
		if appendErr := rawdbv3.TxNums.Append(tx, 1, 40); appendErr != nil {
			return appendErr
		}
		return stages.SaveStageProgress(tx, stages.Execution, 1)
	}))
	require.NoError(b, execctx.InitStateCacheVersion(ctx, db, stateCache))

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(b, err)
	defer roTx.Rollback()
	rsd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(b, err)
	defer rsd.Close()
	rsd.BindStateCache(stateCache)
	asOf := rsd.AsOfGetter(roTx)
	b.ReportAllocs()
	for b.Loop() {
		for _, k := range keys {
			if _, _, err := asOf.GetAsOf(kv.AccountsDomain, k, 100); err != nil {
				b.Fatal(err)
			}
		}
	}
}
