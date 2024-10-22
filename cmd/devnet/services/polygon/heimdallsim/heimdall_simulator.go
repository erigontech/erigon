// Copyright 2024 The Erigon Authors
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

package heimdallsim

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

type HeimdallSimulator struct {
	snapshots   *freezeblocks.BorRoSnapshots
	blockReader *freezeblocks.BlockReader

	iterations               []uint64 // list of final block numbers for an iteration
	lastAvailableBlockNumber uint64

	logger log.Logger
}

var _ heimdall.HeimdallClient = (*HeimdallSimulator)(nil)

func NewHeimdallSimulator(ctx context.Context, snapDir string, logger log.Logger, iterations []uint64) (*HeimdallSimulator, error) {
	snapshots := freezeblocks.NewBorRoSnapshots(ethconfig.Defaults.Snapshot, snapDir, 0, logger)

	// index local files
	localFiles, err := os.ReadDir(snapDir)
	if err != nil {
		return nil, err
	}

	for _, file := range localFiles {
		info, _, _ := snaptype.ParseFileName(snapDir, file.Name())
		if info.Ext == ".seg" {
			err = info.Type.BuildIndexes(ctx, info, nil, snapDir, nil, log.LvlWarn, logger)
			if err != nil {
				return nil, err
			}
		}
	}

	if err = snapshots.OpenFolder(); err != nil {
		return nil, err
	}

	h := HeimdallSimulator{
		snapshots:   snapshots,
		blockReader: freezeblocks.NewBlockReader(nil, snapshots),

		iterations: iterations,

		logger: logger,
	}

	h.Next()

	return &h, nil
}

func (h *HeimdallSimulator) Close() {
	h.snapshots.Close()
}

// Next moves to the next iteration
func (h *HeimdallSimulator) Next() {
	if len(h.iterations) == 0 {
		h.lastAvailableBlockNumber++
	} else {
		h.lastAvailableBlockNumber = h.iterations[0]
		h.iterations = h.iterations[1:]
	}
}

func (h *HeimdallSimulator) FetchLatestSpan(ctx context.Context) (*heimdall.Span, error) {
	latestSpan := uint64(heimdall.SpanIdAt(h.lastAvailableBlockNumber))

	span, err := h.getSpan(ctx, latestSpan)
	if err != nil {
		return nil, err
	}

	return &span, nil
}

func (h *HeimdallSimulator) FetchSpan(ctx context.Context, spanID uint64) (*heimdall.Span, error) {
	if spanID > uint64(heimdall.SpanIdAt(h.lastAvailableBlockNumber)) {
		return nil, errors.New("span not found")
	}

	span, err := h.getSpan(ctx, spanID)
	if err != nil {
		return nil, err
	}

	return &span, err
}

func (h *HeimdallSimulator) FetchSpans(ctx context.Context, page uint64, limit uint64) ([]*heimdall.Span, error) {
	return nil, errors.New("method FetchSpans is not implemented")
}

func (h *HeimdallSimulator) FetchStateSyncEvents(_ context.Context, fromId uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, error) {
	events, _, err := h.blockReader.EventsByIdFromSnapshot(fromId, to, limit)
	return events, err
}

func (h *HeimdallSimulator) FetchStateSyncEvent(ctx context.Context, id uint64) (*heimdall.EventRecordWithTime, error) {
	return nil, errors.New("method FetchStateSyncEvent not implemented")
}

func (h *HeimdallSimulator) FetchCheckpoint(ctx context.Context, number int64) (*heimdall.Checkpoint, error) {
	return nil, errors.New("method FetchCheckpoint not implemented")
}

func (h *HeimdallSimulator) FetchCheckpointCount(ctx context.Context) (int64, error) {
	return 0, errors.New("method FetchCheckpointCount not implemented")
}

func (h *HeimdallSimulator) FetchCheckpoints(ctx context.Context, page uint64, limit uint64) ([]*heimdall.Checkpoint, error) {
	return nil, errors.New("method FetchCheckpoints not implemented")
}

func (h *HeimdallSimulator) FetchMilestone(ctx context.Context, number int64) (*heimdall.Milestone, error) {
	return nil, errors.New("method FetchMilestone not implemented")
}

func (h *HeimdallSimulator) FetchMilestoneCount(ctx context.Context) (int64, error) {
	return 0, errors.New("method FetchMilestoneCount not implemented")
}

func (h *HeimdallSimulator) FetchFirstMilestoneNum(ctx context.Context) (int64, error) {
	return 0, errors.New("method FetchFirstMilestoneNum not implemented")
}

func (h *HeimdallSimulator) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
	return errors.New("method FetchNoAckMilestone not implemented")
}

func (h *HeimdallSimulator) FetchLastNoAckMilestone(ctx context.Context) (string, error) {
	return "", errors.New("method FetchLastNoAckMilestone not implemented")
}

func (h *HeimdallSimulator) FetchMilestoneID(ctx context.Context, milestoneID string) error {
	return errors.New("method FetchMilestoneID not implemented")
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
