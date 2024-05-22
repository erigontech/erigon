package simulator

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
)

type HeimdallSimulator struct {
	ctx         context.Context
	snapshots   *freezeblocks.BorRoSnapshots
	blockReader *freezeblocks.BlockReader
	logger      log.Logger

	iterations               []uint64 // list of final block numbers for an iteration
	lastAvailableBlockNumber uint64
}

type IndexFnType func(context.Context, snaptype.FileInfo, uint32, string, *background.Progress, log.Lvl, log.Logger) error

func NewHeimdall(ctx context.Context, snapDir string, logger log.Logger, iterations []uint64) (HeimdallSimulator, error) {
	snapshots := freezeblocks.NewBorRoSnapshots(ethconfig.Defaults.Snapshot, snapDir, 0, logger)

	// index local files
	localFiles, err := os.ReadDir(snapDir)
	if err != nil {
		return HeimdallSimulator{}, err
	}

	for _, file := range localFiles {
		info, _, _ := snaptype.ParseFileName(snapDir, file.Name())
		if info.Ext == ".seg" {
			err = info.Type.BuildIndexes(ctx, info, nil, snapDir, nil, log.LvlWarn, logger)
			if err != nil {
				return HeimdallSimulator{}, err
			}
		}
	}

	if err = snapshots.ReopenFolder(); err != nil {
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
		ctx:         ctx,
		snapshots:   snapshots,
		blockReader: freezeblocks.NewBlockReader(nil, snapshots),
		logger:      logger,

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
	events, _, err := h.blockReader.EventsByIdFromSnapshot(fromId, to, limit)
	return events, err
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
	h.snapshots.Close()
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

	return heimdall.Span{}, err
}
