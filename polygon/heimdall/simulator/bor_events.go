package simulator

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
)

func continueLoop(events []*heimdall.EventRecordWithTime, to time.Time, limit int) bool {
	if len(events) == 0 {
		return true
	}
	if len(events) == limit {
		return false
	}

	last := events[len(events)-1]
	return last.Time.Before(to)
}

func (h *HeimdallSimulator) getEvents(ctx context.Context, fromId uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, error) {
	var span []*heimdall.EventRecordWithTime
	var err error

	view := h.knownBorSnapshots.View()
	defer view.Close()

	for continueLoop(span, to, limit) && err == nil {
		if seg, ok := view.EventsSegment(h.lastDownloadedBlockNumber); ok {
			if err := h.downloadData(ctx, seg, snaptype.Enums.BorEvents, freezeblocks.BorEventsIdx, snaptype.BorEvents); err != nil {
				return nil, err
			}
		}
		h.lastDownloadedBlockNumber += 500000

		span, err = h.blockReader.EventsById(fromId, to, limit)
	}

	if len(span) == limit {
		return span, err
	}
	return span[:len(span)-1], err
}
