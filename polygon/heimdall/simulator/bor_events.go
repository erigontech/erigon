package simulator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/rlp"
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

func (h *HeimdallSimulator) downloadHeader(ctx context.Context, spans *freezeblocks.Segment) error {
	fileName := snaptype.SegmentFileName(1, spans.From(), spans.To(), snaptype.Enums.Headers)

	h.logger.Warn(fmt.Sprintf("Downloading %s", fileName))

	err := h.downloader.Download(ctx, fileName)
	if err != nil {
		return fmt.Errorf("can't download %s: %w", fileName, err)
	}

	h.logger.Warn(fmt.Sprintf("Indexing %s", fileName))

	info, _, _ := snaptype.ParseFileName(h.downloader.LocalFsRoot(), fileName)

	return freezeblocks.HeadersIdx(ctx, info, h.downloader.LocalFsRoot(), nil, log.LvlWarn, h.logger)
}

func bytestoEvents(events []rlp.RawValue, limit int) ([]*heimdall.EventRecordWithTime, error) {
	stateContract := bor.GenesisContractStateReceiverABI()
	records := make([]*heimdall.EventRecordWithTime, 0, len(events))

	for _, e := range events {
		if limit == 0 {
			break
		}
		r := heimdall.UnpackEventRecordWithTime(stateContract, e)
		if r == nil {
			return nil, errors.New("unable to unmarshal EventRecordWithTime")
		}

		records = append(records, r)
		limit--
	}

	return records, nil
}

func (h *HeimdallSimulator) getEvents(ctx context.Context, fromId uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, error) {
	var span []*heimdall.EventRecordWithTime
	var err error

	if len(span) == 0 && err == nil {
		view := h.knownBorSnapshots.View()
		defer view.Close()

		if seg, ok := view.EventsSegment(600000); ok {
			if err := h.downloadEvents(ctx, seg); err != nil {
				return nil, err
			}
		}
		if seg, ok := view.EventsSegment(0); ok {
			if err := h.downloadEvents(ctx, seg); err != nil {
				return nil, err
			}
		}

		h.activeBorSnapshots.ReopenSegments([]snaptype.Type{snaptype.BorEvents}, true)

		return h.blockReader.EventsById(fromId, to, limit)
	}

	return nil, err
}
