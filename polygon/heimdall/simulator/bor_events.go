package simulator

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
)

func (h *HeimdallSimulator) downloadEvents(ctx context.Context, spans *freezeblocks.Segment) error {
	fileName := snaptype.SegmentFileName(1, spans.From(), spans.To(), snaptype.Enums.BorEvents)

	h.logger.Warn(fmt.Sprintf("Downloading %s", fileName))

	err := h.downloader.Download(ctx, fileName)
	if err != nil {
		return fmt.Errorf("can't download %s: %w", fileName, err)
	}

	h.logger.Warn(fmt.Sprintf("Indexing %s", fileName))

	info, _, _ := snaptype.ParseFileName(h.downloader.LocalFsRoot(), fileName)

	h.logger.Warn(fmt.Sprintf("snapshot from: %d, to: %d", heimdall.SpanIdAt(info.From), heimdall.SpanIdAt(info.To)))

	return freezeblocks.BorEventsIdx(ctx, info, h.downloader.LocalFsRoot(), nil, log.LvlWarn, h.logger)
}

func endLoop(events []*heimdall.EventRecordWithTime, to time.Time, limit int) bool {
	if len(events) == 0 {
		return false
	}
	if len(events) == limit {
		return true
	}

	last := events[len(events)-1]
	return last.Time.After(to)
}

func (h *HeimdallSimulator) getEvents(ctx context.Context, fromId uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, error) {
	var span []*heimdall.EventRecordWithTime
	var err error

	view := h.knownBorSnapshots.View()
	defer view.Close()

	for !endLoop(span, to, limit) && err == nil {
		if seg, ok := view.EventsSegment(h.lastDownloadedBlockNumber); ok {
			if err := h.downloadEvents(ctx, seg); err != nil {
				return nil, err
			}
		}
		h.lastDownloadedBlockNumber += 500000

		h.activeBorSnapshots.ReopenSegments([]snaptype.Type{snaptype.BorEvents}, true)

		span, err = h.blockReader.EventsById(fromId, to, limit)
	}

	if len(span) == limit {
		return span, err
	}

	return span[:len(span)-1], err
}
