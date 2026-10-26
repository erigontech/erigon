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

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/internal/commitmenttest/temporal"
)

func TestPBinR18VerifyCountsActualRootPointReads(t *testing.T) {
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
	counted := &pbinR18CountingTx{TemporalTx: verifyTx}
	stats := new(pbt.VerifyStats)
	_, err = state.VerifyPBinDomainRootWithStats(t.Context(), counted, agg, kv.CommitmentDomain, stats)
	require.NoError(t, err)
	require.Zero(t, len(counted.reads), "C-d made point reads of %x while stats reported %d", counted.reads, stats.PointLookups.Load())
}

type pbinR18CountingTx struct {
	kv.TemporalTx
	reads [][]byte
}

func (tx *pbinR18CountingTx) Pin() kv.TemporalFilesPin {
	return tx.TemporalTx.(interface{ Pin() kv.TemporalFilesPin }).Pin()
}

func (tx *pbinR18CountingTx) GetLatest(domain kv.Domain, key []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	if domain == kv.CommitmentDomain && bytes.Equal(key, pbt.GlobalRootKey()) {
		tx.reads = append(tx.reads, bytes.Clone(key))
	}
	return tx.TemporalTx.GetLatest(domain, key, opts)
}
