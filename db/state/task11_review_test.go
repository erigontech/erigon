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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/internal/commitmenttest/temporal"
)

func TestPBinReviewMDBXRowsAreNotAbsent(t *testing.T) {
	selectPBinConvertHash(t)
	workers := dbg.PBTWorkers
	t.Cleanup(func() { dbg.PBTWorkers = workers })
	dbg.PBTWorkers = 4
	db, agg := temporal.Open(t, 8)
	writePBinConvertState(t, db)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	sourceTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	files := agg.BeginFilesRo()
	defer files.Close()
	targetDB, targetAgg := temporal.Open(t, 8)
	targetFiles := targetAgg.BeginFilesRo()
	require.Empty(t, targetFiles.Files(kv.CommitmentBinDomain))
	targetFiles.Close()
	var ops []pbt.Op
	require.NoError(t, state.ForEachPBinLeaf(files, sourceTx, true, func(leaf state.PBinLeaf) error {
		op := pbt.Op{Key: bytes.Clone(leaf.Key)}
		copy(op.Value[:], leaf.Value)
		ops = append(ops, op)
		return nil
	}))
	records := newPBinSequentialContext()
	_, err = pbt.NewTrie(records).Process(ops)
	require.NoError(t, err)

	seedTx, err := targetDB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer seedTx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	domains, err := execctx.NewSharedDomains(t.Context(), seedTx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomainOnly(kv.CommitmentBinDomain), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	defer domains.Close()
	seeded := 0
	for key, value := range records.Records {
		path, decodeErr := eip8297.DecodeBitPath([]byte(key))
		if decodeErr != nil || path.BitLen <= 12 {
			continue
		}
		require.NoError(t, domains.DomainPut(kv.CommitmentBinDomain, seedTx, []byte(key), value, 7, nil))
		seeded++
	}
	require.Positive(t, seeded)
	require.NoError(t, domains.Flush(t.Context(), seedTx))
	require.NoError(t, seedTx.Commit())

	targetTx, err := targetDB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	for key, value := range records.Records {
		path, decodeErr := eip8297.DecodeBitPath([]byte(key))
		if decodeErr != nil || path.BitLen <= 12 {
			continue
		}
		stored, _, found, readErr := targetTx.Debug().GetLatestFromDB(kv.CommitmentBinDomain, []byte(key))
		require.NoError(t, readErr)
		require.True(t, found)
		require.Equal(t, value, stored)
	}
	_, err = state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: agg, SourceTx: sourceTx,
		TargetAggregator: targetAgg, TargetTx: targetTx, TargetDB: targetDB,
		TargetDomain: kv.CommitmentBinDomain, EndTxNum: 7, Hash: eip8297.HashBytes,
	})
	if err == nil {
		t.Fatalf("convert accepted nil prev over %d MDBX-only rows: want the populated target refused, got err=%v", seeded, err)
	}
	require.ErrorContains(t, err, "already holds rows")
}
