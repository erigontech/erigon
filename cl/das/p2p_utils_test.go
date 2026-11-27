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

package das

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common/length"
)

func kzgBatchTestColumns(t *testing.T) ([]*cltypes.DataColumnSidecar, *solid.ListSSZ[*cltypes.KZGCommitment]) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&cfg)
	_, _, _, columns := recoverableFuluData(t, &cfg)
	return columns[:3], columns[0].KzgCommitments
}

func corruptCell(sidecar *cltypes.DataColumnSidecar) {
	sidecar.Column.Get(0)[31] ^= 1
}

func swapProofs(sidecar *cltypes.DataColumnSidecar) {
	first, second := sidecar.KzgProofs.Get(0), sidecar.KzgProofs.Get(1)
	*first, *second = *second, *first
}

func TestVerifyKZGProofsBatch(t *testing.T) {
	t.Run("valid columns", func(t *testing.T) {
		columns, commitments := kzgBatchTestColumns(t)
		require.True(t, verifyKZGProofsBatch(columns, commitments))
	})

	t.Run("a corrupted cell in one of several columns", func(t *testing.T) {
		columns, commitments := kzgBatchTestColumns(t)
		corruptCell(columns[1])
		require.False(t, verifyKZGProofsBatch(columns, commitments))
	})

	t.Run("a wrong proof in one of several columns", func(t *testing.T) {
		columns, commitments := kzgBatchTestColumns(t)
		swapProofs(columns[2])
		require.False(t, verifyKZGProofsBatch(columns, commitments))
	})

	t.Run("a column checked against another column index", func(t *testing.T) {
		columns, commitments := kzgBatchTestColumns(t)
		columns[1].Index = columns[2].Index
		require.False(t, verifyKZGProofsBatch(columns, commitments))
	})

	t.Run("columns whose lengths do not match the commitments", func(t *testing.T) {
		columns, commitments := kzgBatchTestColumns(t)
		long := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
		long.Index = columns[0].Index
		for i := range commitments.Len() {
			long.Column.Append(columns[0].Column.Get(i))
			long.KzgProofs.Append(columns[0].KzgProofs.Get(i))
		}
		long.Column.Append(columns[0].Column.Get(0))
		long.KzgProofs.Append(columns[0].KzgProofs.Get(0))
		short := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
		short.Index = columns[1].Index
		for i := 1; i < commitments.Len(); i++ {
			short.Column.Append(columns[1].Column.Get(i))
			short.KzgProofs.Append(columns[1].KzgProofs.Get(i))
		}
		require.False(t, verifyKZGProofsBatch([]*cltypes.DataColumnSidecar{long, short}, commitments))
	})

	t.Run("no columns", func(t *testing.T) {
		require.True(t, verifyKZGProofsBatch(nil, nil))
	})

	t.Run("no commitments", func(t *testing.T) {
		columns, _ := kzgBatchTestColumns(t)
		require.False(t, verifyKZGProofsBatch(columns, solid.NewStaticListSSZ[*cltypes.KZGCommitment](1, length.Bytes48)))
	})
}

func TestSplitByKZGProofs(t *testing.T) {
	t.Run("valid columns", func(t *testing.T) {
		columns, commitments := kzgBatchTestColumns(t)
		valid, invalid := splitByKZGProofs(columns, commitments)
		require.Equal(t, columns, valid)
		require.Empty(t, invalid)
	})

	t.Run("only the bad columns are dropped", func(t *testing.T) {
		columns, commitments := kzgBatchTestColumns(t)
		corruptCell(columns[0])
		swapProofs(columns[2])
		valid, invalid := splitByKZGProofs(columns, commitments)
		require.Equal(t, []*cltypes.DataColumnSidecar{columns[1]}, valid)
		require.Equal(t, []*cltypes.DataColumnSidecar{columns[0], columns[2]}, invalid)
	})

	t.Run("every column is bad", func(t *testing.T) {
		columns, commitments := kzgBatchTestColumns(t)
		for _, column := range columns {
			corruptCell(column)
		}
		valid, invalid := splitByKZGProofs(columns, commitments)
		require.Empty(t, valid)
		require.Equal(t, columns, invalid)
	})

	t.Run("no columns", func(t *testing.T) {
		valid, invalid := splitByKZGProofs(nil, nil)
		require.Empty(t, valid)
		require.Empty(t, invalid)
	})
}
