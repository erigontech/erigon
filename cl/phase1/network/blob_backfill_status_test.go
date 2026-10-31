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

package network

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/snaptype"
)

func TestSetBackfillCompletedSetsCompletionGauge(t *testing.T) {
	b := &BlobHistoryDownloader{}

	b.setBackfillCompleted(true)
	require.Equal(t, uint64(1), blobBackfillCompleteGauge().GetValueUint64())

	b.setBackfillCompleted(false)
	require.Zero(t, blobBackfillCompleteGauge().GetValueUint64(), "a revoked completion must clear the gauge")
}

// A fresh downloader starts incomplete, so its first report is an unchanged false. The gauge
// must still be written for it.
func TestFirstRetrySlotExportsIncompleteBackfill(t *testing.T) {
	blobBackfillCompleteGauge().SetUint64(1)

	(&BlobHistoryDownloader{}).addRetrySlot(14_910_740)

	require.Zero(t, blobBackfillCompleteGauge().GetValueUint64())
}

// The first pass can run for hours, or wait for peers or sync, before reporting anything; the
// gauge must show the backfill as incomplete from the start.
func TestStartExportsBackfillStateBeforeTheFirstPass(t *testing.T) {
	blobBackfillCompleteGauge().SetUint64(1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	b := &BlobHistoryDownloader{ctx: ctx, archiveBlobs: true, logger: log.New()}

	b.Start()

	require.Eventually(t, func() bool { return !b.running.Load() }, time.Second, time.Millisecond)
	require.Zero(t, blobBackfillCompleteGauge().GetValueUint64())
}

func TestStartWithoutBlobBackfillLeavesTheGaugeAlone(t *testing.T) {
	blobBackfillCompleteGauge().SetUint64(1)

	(&BlobHistoryDownloader{ctx: t.Context(), logger: log.New()}).Start()

	require.Equal(t, uint64(1), blobBackfillCompleteGauge().GetValueUint64())
}

// snapshotEnds models the snapshot frontiers by where the last blob and block segments end.
type snapshotEnds struct{ blobsTo, blocksTo uint64 }

func (s snapshotEnds) FrozenBlobs() uint64 { return s.blobsTo }

func (s snapshotEnds) VisibleSegmentsMaxTo(t snaptype.Enum) uint64 {
	if t == snaptype.BeaconBlocks.Enum() {
		return s.blocksTo
	}
	return s.blobsTo
}

type backfillLogHandler struct {
	records []*log.Record
}

func (h *backfillLogHandler) Log(record *log.Record) error {
	h.records = append(h.records, record)
	return nil
}

func (h *backfillLogHandler) Enabled(_ context.Context, _ log.Lvl) bool {
	return true
}

func newBackfillWarningDownloader() (*BlobHistoryDownloader, *backfillLogHandler) {
	handler := &backfillLogHandler{}
	logger := log.New()
	logger.SetHandler(handler)
	b := &BlobHistoryDownloader{logger: logger, sn: snapshotEnds{blobsTo: 14_880_000, blocksTo: 15_220_000}}
	b.headSlot.Store(15_300_001)
	b.highestBackfilledSlot.Store(15_300_000)
	return b, handler
}

func TestWarnBackfillIncompleteNamesFrontiersAndUnresolvedSlots(t *testing.T) {
	b, handler := newBackfillWarningDownloader()
	for _, slot := range []uint64{14_910_740, 14_910_741, 14_911_999} {
		b.addRetrySlot(slot)
	}

	b.warnBackfillIncomplete()

	require.Len(t, handler.records, 1)
	require.Equal(t, log.LvlWarn, handler.records[0].Lvl)
	require.Equal(t, []any{
		"currentSlot", uint64(15_300_001), "highestBackfilled", uint64(15_300_000),
		"frozenBlobsTo", uint64(14_880_000), "frozenBlocksTo", uint64(15_220_000),
		"unresolvedSlots", uint64(3), "lowestUnresolved", uint64(14_910_740), "highestUnresolved", uint64(14_911_999),
	}, handler.records[0].Ctx)
}

// A pass that is still running has no unresolved slots yet; the frontiers alone describe it.
func TestWarnBackfillIncompleteWithoutUnresolvedSlots(t *testing.T) {
	b, handler := newBackfillWarningDownloader()

	b.warnBackfillIncomplete()

	require.Len(t, handler.records, 1)
	require.Equal(t, []any{
		"currentSlot", uint64(15_300_001), "highestBackfilled", uint64(15_300_000),
		"frozenBlobsTo", uint64(14_880_000), "frozenBlocksTo", uint64(15_220_000),
	}, handler.records[0].Ctx)
}
