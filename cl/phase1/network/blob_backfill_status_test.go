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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

func TestSetBackfillCompletedSetsCompletionGauge(t *testing.T) {
	b := &BlobHistoryDownloader{}

	b.setBackfillCompleted(true)
	require.Equal(t, uint64(1), metrics.GetOrCreateGauge(blobBackfillCompleteMetric).GetValueUint64())

	b.setBackfillCompleted(false)
	require.Zero(t, metrics.GetOrCreateGauge(blobBackfillCompleteMetric).GetValueUint64(), "a revoked completion must clear the gauge")
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
	b := &BlobHistoryDownloader{logger: logger}
	b.headSlot.Store(15_300_001)
	b.highestBackfilledSlot.Store(15_300_000)
	return b, handler
}

func TestWarnBackfillIncompleteNamesFrontiersAndUnresolvedSlots(t *testing.T) {
	b, handler := newBackfillWarningDownloader()
	for _, slot := range []uint64{14_910_740, 14_910_741, 14_911_999} {
		b.addRetrySlot(slot)
	}

	b.warnBackfillIncomplete(14_880_000, 15_219_999)

	require.Len(t, handler.records, 1)
	require.Equal(t, log.LvlWarn, handler.records[0].Lvl)
	require.Equal(t, []any{
		"currentSlot", uint64(15_300_001), "highestBackfilled", uint64(15_300_000),
		"frozenBlobs", uint64(14_880_000), "frozenBlocks", uint64(15_219_999),
		"unresolvedSlots", uint64(3), "lowestUnresolved", uint64(14_910_740), "highestUnresolved", uint64(14_911_999),
	}, handler.records[0].Ctx)
}

// A pass that is still running has no unresolved slots yet; the frontiers alone describe it.
func TestWarnBackfillIncompleteWithoutUnresolvedSlots(t *testing.T) {
	b, handler := newBackfillWarningDownloader()

	b.warnBackfillIncomplete(14_880_000, 15_219_999)

	require.Len(t, handler.records, 1)
	require.Equal(t, []any{
		"currentSlot", uint64(15_300_001), "highestBackfilled", uint64(15_300_000),
		"frozenBlobs", uint64(14_880_000), "frozenBlocks", uint64(15_219_999),
	}, handler.records[0].Ctx)
}
