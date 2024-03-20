package simulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
)

type HeimdallSimulator struct {
	ctx                context.Context
	knownSnapshots     *freezeblocks.RoSnapshots
	activeSnapshots    *freezeblocks.RoSnapshots
	knownBorSnapshots  *freezeblocks.BorRoSnapshots
	activeBorSnapshots *freezeblocks.BorRoSnapshots
	blockReader        *freezeblocks.BlockReader
	logger             log.Logger
	downloader         *TorrentClient

	nextSpan uint64
}

func NewHeimdall(ctx context.Context, chain string, snapshotLocation string, logger log.Logger) (HeimdallSimulator, error) {
	cfg := snapcfg.KnownCfg(chain)

	knownSnapshots := freezeblocks.NewRoSnapshots(ethconfig.Defaults.Snapshot, snapshotLocation, 0, logger)
	knownBorSnapshots := freezeblocks.NewBorRoSnapshots(ethconfig.Defaults.Snapshot, snapshotLocation, 0, logger)

	files := make([]string, 0, len(cfg.Preverified))

	for _, item := range cfg.Preverified {
		files = append(files, item.Name)
	}

	knownSnapshots.InitSegments(files)
	knownBorSnapshots.InitSegments(files)

	activeSnapshots := freezeblocks.NewRoSnapshots(ethconfig.Defaults.Snapshot, snapshotLocation, 0, logger)
	activeBorSnapshots := freezeblocks.NewBorRoSnapshots(ethconfig.Defaults.Snapshot, snapshotLocation, 0, logger)

	if err := activeSnapshots.ReopenFolder(); err != nil {
		return HeimdallSimulator{}, err
	}
	if err := activeBorSnapshots.ReopenFolder(); err != nil {
		return HeimdallSimulator{}, err
	}

	downloader, err := NewTorrentClient(ctx, chain, snapshotLocation, logger)
	if err != nil {
		return HeimdallSimulator{}, err
	}

	s := HeimdallSimulator{
		ctx:                ctx,
		knownSnapshots:     knownSnapshots,
		activeSnapshots:    activeSnapshots,
		knownBorSnapshots:  knownBorSnapshots,
		activeBorSnapshots: activeBorSnapshots,
		blockReader:        freezeblocks.NewBlockReader(activeSnapshots, activeBorSnapshots),
		logger:             logger,
		downloader:         downloader,
		nextSpan:           0,
	}

	go func() {
		<-ctx.Done()
		s.Close()
	}()

	return s, nil
}

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

	//var span []byte
	//var err error

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

// FetchLatestSpan gets the next span from the snapshot
func (h *HeimdallSimulator) FetchLatestSpan(ctx context.Context) (*heimdall.Span, error) {
	span, err := h.getSpan(h.ctx, h.nextSpan)
	if err != nil {
		return nil, err
	}

	h.nextSpan++
	return &span, nil
}

func (h *HeimdallSimulator) FetchSpan(ctx context.Context, spanID uint64) (*heimdall.Span, error) {
	if spanID > h.nextSpan-1 {
		return nil, errors.New("span not found")
	}

	span, err := h.getSpan(h.ctx, spanID)
	if err != nil {
		return nil, err
	}

	return &span, err
}

func (h *HeimdallSimulator) Close() {
	h.activeSnapshots.Close()
	h.knownSnapshots.Close()
}
