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

package state

import (
	"testing"

	"github.com/erigontech/erigon/db/kv"
)

func BenchmarkDemotionCandidateWithManyColdFiles(b *testing.B) {
	const files = 200
	ranges := make([]testFileRange, files)
	for i := range ranges {
		ranges[i] = testFileRange{uint64(i), uint64(i + 1)}
	}
	db, agg := testDbAndAggregatorv3(b, 10)
	dirs := agg.Dirs()
	generateAccountsFile(b, dirs, ranges)
	generateStorageFile(b, dirs, ranges)
	generateCodeFile(b, dirs, ranges)
	generateCommitmentFile(b, dirs, ranges)
	if err := agg.setDemoteColdDir(b.TempDir()); err != nil {
		b.Fatal(err)
	}
	if err := agg.OpenFolder(db); err != nil {
		b.Fatal(err)
	}
	for i := range files - 1 {
		if err := agg.DemoteFile(b.Context(), kv.CommitmentDomain, kv.Step(i), kv.Step(i+1), agg.demoteColdDir); err != nil {
			b.Fatal(err)
		}
	}
	boundary := uint64(files-1) * agg.StepSize()
	b.ResetTimer()
	for b.Loop() {
		if _, _, ok := agg.demotionCandidate(boundary); ok {
			b.Fatal("all files below the boundary are cold")
		}
	}
}
