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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/internal/commitmenttest/temporal"
)

func TestPBinVerifyStreamsRowsWithoutPointLookups(t *testing.T) {
	selectPBinBinOnlyHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertStateN(t, db, 1024)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	sourceTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	targetTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	_, err = state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: agg,
		SourceTx:         sourceTx,
		TargetAggregator: agg,
		TargetTx:         targetTx,
		TargetDomain:     kv.CommitmentDomain,
		EndTxNum:         7,
		Hash:             eip8297.HashBytes,
	})
	require.NoError(t, err)
	targetTx.Rollback()
	verifyTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer verifyTx.Rollback()
	stats := new(pbt.VerifyStats)
	_, err = state.VerifyPBinDomainRootWithStats(t.Context(), verifyTx, agg, kv.CommitmentDomain, stats)
	require.NoError(t, err)
	require.Zero(t, stats.PointLookups.Load(), "C-d must not use point lookups")
	require.Positive(t, stats.RangeReads.Load())
	require.Equal(t, stats.RangeTasks.Load(), stats.RangeReads.Load(), "each verification task must use one range read")
	require.Positive(t, stats.TaskRowsLoaded.Load())
	require.Equal(t, stats.TaskRowsLoaded.Load(), stats.TaskRowsReleased.Load(), "every verification task must release its rows")
	require.Zero(t, stats.UpperRowsAtStart.Load(), "upper rows must be verified after task rows are released")
	require.LessOrEqual(t, stats.PeakRows.Load(), stats.TaskWorkers.Load()*stats.MaxTaskRows.Load(), "verification row memory exceeded the worker bound")
}

func TestPBinVerifyRejectsUnreferencedRowInsideChainRange(t *testing.T) {
	selectPBinBinOnlyHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertStateN(t, db, 1024)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	sourceTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	targetTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	_, err = state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: agg,
		SourceTx:         sourceTx,
		TargetAggregator: agg,
		TargetTx:         targetTx,
		TargetDomain:     kv.CommitmentDomain,
		EndTxNum:         7,
		Hash:             eip8297.HashBytes,
	})
	require.NoError(t, err)
	targetTx.Rollback()
	rows := pbinCommitmentRows(t, agg)
	var extraKey, extraValue []byte
	for key, value := range rows {
		path, decodeErr := eip8297.DecodeBitPath([]byte(key))
		if decodeErr != nil || path.BitLen <= 12 {
			continue
		}
		for bit := int16(12); bit < path.BitLen; bit++ {
			candidate := path
			candidate.SetBitAt(bit, 1-candidate.Bit(bit))
			candidateKey, encodeErr := pbt.EncodeRowKey(&candidate)
			require.NoError(t, encodeErr)
			if _, exists := rows[string(candidateKey)]; !exists {
				extraKey = candidateKey
				extraValue = bytes.Clone(value)
				break
			}
		}
		if len(extraKey) != 0 {
			break
		}
	}
	require.NotEmpty(t, extraKey)
	badTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer badTx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), badTx, log.Root(), execctx.WithTrieConfig(cfg), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	require.NoError(t, domains.DomainPut(kv.CommitmentDomain, badTx, extraKey, extraValue, 9, nil))
	require.NoError(t, domains.Flush(t.Context(), badTx))
	require.NoError(t, badTx.Commit())
	domains.Close()
	verifyTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer verifyTx.Rollback()
	_, err = state.VerifyPBinDomainRoot(t.Context(), verifyTx, agg, kv.CommitmentDomain)
	require.ErrorContains(t, err, "unreferenced verification row")
}
