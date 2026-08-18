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
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

func caplinStateVisibleRanges(segments VisibleSegments) []Range {
	ranges := make([]Range, 0, len(segments))
	for _, segment := range segments {
		ranges = append(ranges, segment.Range)
	}
	return ranges
}

func openTestCaplinStateSnapshotsWithTables(t *testing.T, dirs datadir.Dirs, tables []string, logger log.Logger) *CaplinStateSnapshots {
	t.Helper()
	types := SnapshotTypes{
		KeyValueGetters: make(map[string]KeyValueGetter, len(tables)),
		Compression:     map[string]bool{},
	}
	for _, table := range tables {
		types.KeyValueGetters[table] = nil
	}
	s := NewCaplinStateSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, nil, dirs, types, logger)
	t.Cleanup(s.Close)
	require.NoError(t, s.OpenFolder())
	return s
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

func TestCaplinStateBlocksAvailableEqualHeight(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	tables := []string{kv.BlockRoot, kv.PendingDepositsDump}
	const to = uint64(100_000)

	for _, table := range tables {
		writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, to, logger)
	}

	s := openTestCaplinStateSnapshotsWithTables(t, dirs, tables, logger)
	require.Equal(t, to-1, s.BlocksAvailable())
}

func TestCaplinStateBlocksAvailableUsesLowestTable(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	tables := []string{kv.BlockRoot, kv.PendingDepositsDump}

	writeCaplinStateFixture(t, dirs.SnapCaplin, kv.BlockRoot, 0, 50_000, logger)
	writeCaplinStateFixture(t, dirs.SnapCaplin, kv.PendingDepositsDump, 0, 100_000, logger)

	s := openTestCaplinStateSnapshotsWithTables(t, dirs, tables, logger)
	require.Equal(t, uint64(50_000), s.BlocksAvailable())
}

func TestCaplinStateBlocksAvailableZeroWhenTableHasNoSegments(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	tables := []string{kv.BlockRoot, kv.PendingDepositsDump}

	writeCaplinStateFixture(t, dirs.SnapCaplin, kv.BlockRoot, 0, 50_000, logger)

	s := openTestCaplinStateSnapshotsWithTables(t, dirs, tables, logger)
	require.Equal(t, uint64(0), s.BlocksAvailable())
}

func TestCaplinStateBlocksAvailableMainnetPublishedSubset(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	configured := []string{
		kv.ValidatorEffectiveBalance,
		kv.ValidatorSlashings,
		kv.ValidatorBalance,
		kv.StateEvents,
		kv.ActiveValidatorIndicies,
		kv.StateRoot,
		kv.BlockRoot,
		kv.SlotData,
		kv.EpochData,
		kv.InactivityScores,
		kv.NextSyncCommittee,
		kv.CurrentSyncCommittee,
		kv.Eth1DataVotes,
		kv.IntraRandaoMixes,
		kv.RandaoMixes,
		kv.BalancesDump,
		kv.EffectiveBalancesDump,
		kv.PendingConsolidations,
		kv.PendingPartialWithdrawals,
		kv.PendingDeposits,
		kv.PendingConsolidationsDump,
		kv.PendingPartialWithdrawalsDump,
		kv.PendingDepositsDump,
		kv.Builders,
		kv.BuildersDump,
		kv.BuilderPendingWithdrawals,
		kv.BuilderPendingWithdrawalsDump,
		kv.PayloadExpectedWithdrawals,
		kv.PayloadExpectedWithdrawalsDump,
		kv.ExecutionPayloadAvailabilityTable,
		kv.BuilderPendingPaymentsTable,
		kv.PtcWindowTable,
		kv.LatestExecutionPayloadBidTable,
	}
	require.Len(t, configured, 33)
	published := configured[:23]
	require.Len(t, published, 23)
	for _, table := range published {
		writeCaplinStateFixture(t, dirs.SnapCaplin, table, 0, 50_000, logger)
	}

	s := openTestCaplinStateSnapshotsWithTables(t, dirs, configured, logger)
	require.Equal(t, uint64(0), s.BlocksAvailable())
}
