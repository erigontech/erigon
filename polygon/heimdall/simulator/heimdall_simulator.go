package simulator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon/cmd/snapshots/sync"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	borsnaptype "github.com/ledgerwatch/erigon/polygon/bor/snaptype"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
)

type HeimdallSimulator struct {
	ctx                context.Context
	knownBorSnapshots  *freezeblocks.BorRoSnapshots
	activeBorSnapshots *freezeblocks.BorRoSnapshots
	blockReader        *freezeblocks.BlockReader
	logger             log.Logger
	downloader         *sync.TorrentClient
	chain              string

	iterations               []uint64 // list of final block numbers for an iteration
	lastAvailableBlockNumber uint64
}

type IndexFnType func(context.Context, snaptype.FileInfo, uint32, string, *background.Progress, log.Lvl, log.Logger) error

func NewHeimdall(ctx context.Context, chain string, snapshotLocation string, logger log.Logger, iterations []uint64) (HeimdallSimulator, error) {
	cfg := snapcfg.KnownCfg(chain)
	torrentDir := filepath.Join(snapshotLocation, "torrents", chain)

	knownBorSnapshots := freezeblocks.NewBorRoSnapshots(ethconfig.Defaults.Snapshot, torrentDir, 0, logger)

	files := make([]string, 0, len(cfg.Preverified))

	for _, item := range cfg.Preverified {
		files = append(files, item.Name)
	}

	err := knownBorSnapshots.InitSegments(files)
	if err != nil {
		return HeimdallSimulator{}, err
	}

	activeBorSnapshots := freezeblocks.NewBorRoSnapshots(ethconfig.Defaults.Snapshot, torrentDir, 0, logger)

	config := sync.NewDefaultTorrentClientConfig(chain, snapshotLocation, logger)
	downloader, err := sync.NewTorrentClient(config)
	if err != nil {
		return HeimdallSimulator{}, err
	}

	// index local files
	localFiles, err := os.ReadDir(torrentDir)
	if err != nil {
		return HeimdallSimulator{}, err
	}

	for _, file := range localFiles {
		info, _, _ := snaptype.ParseFileName(torrentDir, file.Name())
		if info.Ext == ".seg" {
			if info.Type.Enum() == borsnaptype.Enums.BorSpans {
				err = info.Type.BuildIndexes(ctx, info, nil, torrentDir, nil, log.LvlWarn, logger)
				if err != nil {
					return HeimdallSimulator{}, err
				}
			}

			if info.Type.Enum() == borsnaptype.Enums.BorEvents {
				err = info.Type.BuildIndexes(ctx, info, nil, torrentDir, nil, log.LvlWarn, logger)
				if err != nil {
					return HeimdallSimulator{}, err
				}
			}
		}
	}

	if err = activeBorSnapshots.ReopenFolder(); err != nil {
		return HeimdallSimulator{}, err
	}

	var lastAvailableBlockNum uint64
	if len(iterations) == 0 {
		lastAvailableBlockNum = 0
	} else {
		lastAvailableBlockNum = iterations[0]
		iterations = iterations[1:]
	}

	s := HeimdallSimulator{
		ctx:                ctx,
		knownBorSnapshots:  knownBorSnapshots,
		activeBorSnapshots: activeBorSnapshots,
		blockReader:        freezeblocks.NewBlockReader(nil, activeBorSnapshots),
		logger:             logger,
		downloader:         downloader,
		chain:              chain,

		iterations:               iterations,
		lastAvailableBlockNumber: lastAvailableBlockNum,
	}

	go func() {
		<-ctx.Done()
		s.Close()
	}()

	return s, nil
}

func (h *HeimdallSimulator) FetchLatestSpan(ctx context.Context) (*heimdall.Span, error) {
	latestSpan := uint64(heimdall.SpanIdAt(h.lastAvailableBlockNumber))

	span, err := h.getSpan(h.ctx, latestSpan)
	if err != nil {
		return nil, err
	}

	return &span, nil
}

func (h *HeimdallSimulator) FetchSpan(ctx context.Context, spanID uint64) (*heimdall.Span, error) {
	// move to next iteration
	if spanID == uint64(heimdall.SpanIdAt(h.lastAvailableBlockNumber)) {
		if len(h.iterations) == 0 {
			h.lastAvailableBlockNumber++
		} else {
			h.lastAvailableBlockNumber = h.iterations[0]
			h.iterations = h.iterations[1:]
		}
	}

	if spanID > uint64(heimdall.SpanIdAt(h.lastAvailableBlockNumber)) {
		return nil, errors.New("span not found")
	}

	span, err := h.getSpan(h.ctx, spanID)
	if err != nil {
		return nil, err
	}

	return &span, err
}

func (h *HeimdallSimulator) FetchStateSyncEvents(ctx context.Context, fromId uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, error) {
	events, maxTime, err := h.blockReader.EventsByIdFromSnapshot(fromId, to, limit)
	if err != nil {
		return nil, err
	}

	view := h.knownBorSnapshots.View()
	defer view.Close()

	for !maxTime && len(events) != limit {
		if seg, ok := view.EventsSegment(h.lastAvailableBlockNumber); ok {
			if err := h.downloadData(ctx, seg, borsnaptype.BorEvents); err != nil {
				return nil, err
			}
		}

		if len(h.iterations) == 0 {
			// we increment by 500k because the events we need are not in this snapshot file
			h.lastAvailableBlockNumber += 500000
		} else {
			h.lastAvailableBlockNumber = h.iterations[0]
			h.iterations = h.iterations[1:]
		}

		events, maxTime, err = h.blockReader.EventsByIdFromSnapshot(fromId, to, limit)
		if err != nil {
			return nil, err
		}
	}

	return events, nil
}

func (h *HeimdallSimulator) FetchCheckpoint(ctx context.Context, number int64) (*heimdall.Checkpoint, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FetchCheckpoint not implemented")
}

func (h *HeimdallSimulator) FetchCheckpointCount(ctx context.Context) (int64, error) {
	return 0, status.Errorf(codes.Unimplemented, "method FetchCheckpointCount not implemented")
}

func (h *HeimdallSimulator) FetchMilestone(ctx context.Context, number int64) (*heimdall.Milestone, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FetchMilestone not implemented")
}

func (h *HeimdallSimulator) FetchMilestoneCount(ctx context.Context) (int64, error) {
	return 0, status.Errorf(codes.Unimplemented, "method FetchMilestoneCount not implemented")
}

func (h *HeimdallSimulator) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
	return status.Errorf(codes.Unimplemented, "method FetchNoAckMilestone not implemented")
}

func (h *HeimdallSimulator) FetchLastNoAckMilestone(ctx context.Context) (string, error) {
	return "", status.Errorf(codes.Unimplemented, "method FetchLastNoAckMilestone not implemented")
}

func (h *HeimdallSimulator) FetchMilestoneID(ctx context.Context, milestoneID string) error {
	return status.Errorf(codes.Unimplemented, "method FetchMilestoneID not implemented")
}

func (h *HeimdallSimulator) Close() {
	_ = h.downloader.Close()

	h.activeBorSnapshots.Close()
	h.knownBorSnapshots.Close()
}

func (h *HeimdallSimulator) downloadData(ctx context.Context, spans *freezeblocks.Segment, sType snaptype.Type) error {
	fileName := snaptype.SegmentFileName(1, spans.From(), spans.To(), sType.Enum())
	session := sync.NewTorrentSession(h.downloader, h.chain)
	info, _, _ := snaptype.ParseFileName(session.LocalFsRoot(), fileName)

	h.logger.Info(fmt.Sprintf("Downloading %s", fileName))

	err := session.Download(ctx, fileName)
	if err != nil {
		return fmt.Errorf("can't download %s: %w", fileName, err)
	}

	h.logger.Info(fmt.Sprintf("Indexing %s", fileName))

	err = sType.BuildIndexes(ctx, info, nil, session.LocalFsRoot(), nil, log.LvlDebug, h.logger)

	if err != nil {
		return fmt.Errorf("can't download %s: %w", fileName, err)
	}

	return h.activeBorSnapshots.ReopenSegments([]snaptype.Type{sType}, true)
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

		blockNum := heimdall.SpanEndBlockNum(heimdall.SpanId(spanId))

		if seg, ok := view.SpansSegment(blockNum); ok {
			if err := h.downloadData(ctx, seg, borsnaptype.BorSpans); err != nil {
				return heimdall.Span{}, err
			}
		}

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
