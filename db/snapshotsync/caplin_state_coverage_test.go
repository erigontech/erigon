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

package snapshotsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	dir2 "github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

func TestCaplinStateContiguousCoverageEndContiguous(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 50_000, logger)
	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 50_000, 100_000, logger)

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)
	require.Equal(t, uint64(100_000), s.ContiguousCoverageEnd(table))
}

func TestCaplinStateContiguousCoverageEndStopsAtGap(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 50_000, logger)
	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 60_000, 100_000, logger)

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)
	require.Equal(t, uint64(50_000), s.ContiguousCoverageEnd(table))
}

func TestCaplinStateContiguousCoverageEndNoCoverage(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)
	require.Zero(t, s.ContiguousCoverageEnd(table))
	require.Zero(t, s.ContiguousCoverageEnd("unknown-type"))
}

func TestCaplinStateContiguousCoverageEndNotRootedAtGenesis(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 50_000, 100_000, logger)

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)
	require.Zero(t, s.ContiguousCoverageEnd(table))
}

func TestCaplinStateDumpPlanUsesDirtyCoverageAcrossGap(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot
	const blocksPerFile = uint64(50_000)

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, blocksPerFile, logger)
	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 2*blocksPerFile, 3*blocksPerFile, logger)

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)
	jobs := planStateDump(map[string][]Range{table: s.coveredRangesForType(table)}, 3*blocksPerFile, blocksPerFile)

	require.Equal(t, []caplinStateDumpJob{{name: table, from: blocksPerFile, to: 2 * blocksPerFile}}, jobs)
}

func TestCaplinStateDumpCoverageExcludesUnindexedSegment(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.BlockRoot

	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 50_000, logger)
	_, unindexedIdx := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 50_000, 100_000, logger)
	require.NoError(t, dir2.RemoveFile(unindexedIdx))

	s := openTestCaplinStateSnapshots(t, dirs, table, logger)
	require.Equal(t, []Range{{from: 0, to: 50_000}}, s.coveredRangesForType(table))
}
