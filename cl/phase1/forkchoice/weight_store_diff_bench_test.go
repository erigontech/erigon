// Copyright 2024 The Erigon Authors
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

package forkchoice

import (
	_ "embed"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

// BenchmarkHeadWeight_DeltaTreeVsFullScan compares the maintained tree against
// the full-scan store on the same scenario.
func BenchmarkHeadWeight_DeltaTreeVsFullScan(b *testing.B) {
	f := buildExAnteStore(b)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(b, err)
	require.NotNil(b, cs)
	node := ForkChoiceNode{Root: justified.Root, PayloadStatus: cltypes.PayloadStatusPending}

	b.Run("delta-tree", func(b *testing.B) {
		f.mu.Lock()
		defer f.mu.Unlock()
		tree := f.gloasWeightTree.prepare(justified, cs)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = tree.GetAttestationScore(node)
		}
	})
	b.Run("fullscan", func(b *testing.B) {
		full := NewWeightStore(f) // constructed outside the lock (getCheckpointState is cached)
		f.mu.Lock()
		defer f.mu.Unlock()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = full.GetAttestationScore(node)
		}
	})
}

func BenchmarkGloasWeightTreePrepare(b *testing.B) {
	f := buildExAnteStore(b)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(b, err)
	require.NotNil(b, cs)

	f.mu.Lock()
	defer f.mu.Unlock()
	f.gloasWeightTree.prepare(justified, cs)

	dirtyOne := uint64(0)
	dirtyTenPercent := make([]uint64, 0, cs.validatorSetSize/10)
	dirtyAll := make([]uint64, 0, cs.validatorSetSize)
	for i := 0; i < cs.validatorSetSize; i++ {
		vi := uint64(i)
		if len(dirtyTenPercent) < cs.validatorSetSize/10 {
			dirtyTenPercent = append(dirtyTenPercent, vi)
		}
		dirtyAll = append(dirtyAll, vi)
	}

	b.Run("clean", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			f.gloasWeightTree.prepare(justified, cs)
		}
	})
	b.Run("dirty-one", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			f.gloasWeightTree.markDirty(dirtyOne)
			f.gloasWeightTree.prepare(justified, cs)
		}
	})
	b.Run("dirty-10pct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, vi := range dirtyTenPercent {
				f.gloasWeightTree.markDirty(vi)
			}
			f.gloasWeightTree.prepare(justified, cs)
		}
	})
	b.Run("dirty-all", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, vi := range dirtyAll {
				f.gloasWeightTree.markDirty(vi)
			}
			f.gloasWeightTree.prepare(justified, cs)
		}
	})
	b.Run("full-rebuild", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			f.gloasWeightTree.markAllDirty()
			f.gloasWeightTree.prepare(justified, cs)
		}
	})
}
