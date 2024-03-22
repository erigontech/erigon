package simulator

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
)

func (h *HeimdallSimulator) downloadSpans(ctx context.Context, spans *freezeblocks.Segment) error {
	fileName := snaptype.SegmentFileName(1, spans.From(), spans.To(), snaptype.Enums.BorSpans)

	h.logger.Warn(fmt.Sprintf("Downloading %s", fileName))

	err := h.downloader.Download(ctx, fileName)
	if err != nil {
		return fmt.Errorf("can't download %s: %w", fileName, err)
	}

	h.logger.Warn(fmt.Sprintf("Indexing %s", fileName))

	info, _, _ := snaptype.ParseFileName(h.downloader.LocalFsRoot(), fileName)

	h.logger.Warn(fmt.Sprintf("snapshot from: %d, to: %d", heimdall.SpanIdAt(info.From), heimdall.SpanIdAt(info.To)))

	return freezeblocks.BorSpansIdx(ctx, info, h.downloader.LocalFsRoot(), nil, log.LvlWarn, h.logger)
}

func (h *HeimdallSimulator) getSpan(ctx context.Context, spanId uint64) (heimdall.Span, error) {
	span, err := h.blockReader.Span(ctx, nil, spanId)
	if span != nil && err == nil {
		var s heimdall.Span
		if err = json.Unmarshal(span, &s); err != nil {
			return heimdall.Span{}, err
		}
		return s, err
	}

	if span == nil {
		view := h.knownBorSnapshots.View()
		defer view.Close()

		if seg, ok := view.SpansSegment(spanId); ok {
			if err := h.downloadSpans(ctx, seg); err != nil {
				return heimdall.Span{}, err
			}
		}

		h.activeBorSnapshots.ReopenSegments([]snaptype.Type{snaptype.BorSpans}, true)

		h.logger.Warn(fmt.Sprintf("%s", h.activeBorSnapshots.Files()))

		span, err = h.blockReader.Span(ctx, nil, spanId)
		if err != nil {
			return heimdall.Span{}, err
		}
	}

	var s heimdall.Span
	if err := json.Unmarshal(span, &s); err != nil {
		return heimdall.Span{}, err
	}

	return s, nil
}
