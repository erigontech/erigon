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
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

func caplinStateVisibleRanges(segments VisibleSegments) []Range {
	ranges := make([]Range, 0, len(segments))
	for _, segment := range segments {
		ranges = append(ranges, segment.Range)
	}
	return ranges
}

func TestCaplinStateViewPinsVisibleSegments(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.PendingDepositsDump

	firstSeg, _ := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 50_000, logger)
	writeCaplinStateFixture(t, dirs.SnapCaplin, table, 50_000, 100_000, logger)
	s := openTestCaplinStateSnapshots(t, dirs, table, logger)

	view := s.View()
	defer view.Close()
	require.Equal(t, []Range{{from: 0, to: 50_000}, {from: 50_000, to: 100_000}},
		caplinStateVisibleRanges(view.VisibleSegments(table)))

	require.NoError(t, s.OpenList([]string{filepath.Base(firstSeg)}, false))
	require.Equal(t, []Range{{from: 0, to: 50_000}, {from: 50_000, to: 100_000}},
		caplinStateVisibleRanges(view.VisibleSegments(table)))
}

func TestCaplinStateOpenListRecalculateRace(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	table := kv.PendingDepositsDump

	firstSeg, _ := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 50_000, logger)
	secondSeg, _ := writeCaplinStateFixture(t, dirs.SnapCaplin, table, 50_000, 100_000, logger)
	s := openTestCaplinStateSnapshots(t, dirs, table, logger)

	all := []string{filepath.Base(firstSeg), filepath.Base(secondSeg)}
	subset := []string{filepath.Base(firstSeg)}
	const iterations = 100
	errs := make(chan error, iterations*2)

	var wg sync.WaitGroup
	for range iterations {
		wg.Add(2)
		go func() {
			defer wg.Done()
			errs <- s.OpenList(all, false)
		}()
		go func() {
			defer wg.Done()
			errs <- s.OpenList(subset, false)
		}()
		wg.Wait()
	}
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}
