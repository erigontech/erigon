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
	"errors"
	"os"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

type HeimdallSimulator struct {
	snapshots                *heimdall.RoSnapshots
	blockReader              *freezeblocks.BlockReader
	bridgeStore              bridge.Store
	iterations               []uint64 // list of final block numbers for an iteration
	lastAvailableBlockNumber uint64

	logger log.Logger
}

var _ heimdall.Client = (*HeimdallSimulator)(nil)

type sprintLengthCalculator struct{}

func (sprintLengthCalculator) CalculateSprintLength(number uint64) uint64 {
	return 16
}

type noopBridgeStore struct{}

func (noopBridgeStore) Prepare(ctx context.Context) error {
	return nil
}

func (noopBridgeStore) Close() {}

func (noopBridgeStore) LastEventId(ctx context.Context) (uint64, error) {
	return 0, errors.New("noop")
}
func (noopBridgeStore) LastEventIdWithinWindow(ctx context.Context, fromID uint64, toTime time.Time) (uint64, error) {
	return 0, errors.New("noop")
}
func (noopBridgeStore) LastProcessedEventId(ctx context.Context) (uint64, error) {
	return 0, errors.New("noop")
}
func (noopBridgeStore) LastProcessedBlockInfo(ctx context.Context) (bridge.ProcessedBlockInfo, bool, error) {
	return bridge.ProcessedBlockInfo{}, false, errors.New("noop")
}
func (noopBridgeStore) LastFrozenEventId() uint64 {
	return 0
}
func (noopBridgeStore) LastFrozenEventBlockNum() uint64 {
	return 0
}
func (noopBridgeStore) EventTxnToBlockNum(ctx context.Context, borTxHash common.Hash) (uint64, bool, error) {
	return 0, false, errors.New("noop")
}
func (noopBridgeStore) EventsByTimeframe(ctx context.Context, timeFrom, timeTo uint64) ([][]byte, []uint64, error) {
	return nil, nil, errors.New("noop")
}
func (noopBridgeStore) Events(ctx context.Context, start, end uint64) ([][]byte, error) {
	return nil, errors.New("noop")
}
func (noopBridgeStore) BlockEventIdsRange(ctx context.Context, blockNum uint64) (start uint64, end uint64, ok bool, err error) {
	return 0, 0, false, errors.New("noop")
}
func (noopBridgeStore) PutEventTxnToBlockNum(ctx context.Context, eventTxnToBlockNum map[common.Hash]uint64) error {
	return nil
}
func (noopBridgeStore) PutEvents(ctx context.Context, events []*bridge.EventRecordWithTime) error {
	return nil
}
func (noopBridgeStore) PutBlockNumToEventId(ctx context.Context, blockNumToEventId map[uint64]uint64) error {
	return nil
}
func (noopBridgeStore) PutProcessedBlockInfo(ctx context.Context, info []bridge.ProcessedBlockInfo) error {
	return nil
}
func (noopBridgeStore) Unwind(ctx context.Context, blockNum uint64) error {
	return nil
}
func (noopBridgeStore) BorStartEventId(ctx context.Context, hash common.Hash, blockHeight uint64) (uint64, error) {
	return 0, errors.New("noop")
}
func (noopBridgeStore) EventsByBlock(ctx context.Context, hash common.Hash, blockNum uint64) ([]rlp.RawValue, error) {
	return nil, errors.New("noop")
}
func (noopBridgeStore) EventsByIdFromSnapshot(from uint64, to time.Time, limit int) ([]*bridge.EventRecordWithTime, bool, error) {
	return nil, false, errors.New("noop")
}
func (noopBridgeStore) PruneEvents(ctx context.Context, blocksTo uint64, blocksDeleteLimit int) (deleted int, err error) {
	return 0, nil
}

type heimdallStore struct {
	spans heimdall.EntityStore[*heimdall.Span]
}

func (heimdallStore) Checkpoints() heimdall.EntityStore[*heimdall.Checkpoint] {
	return nil
}
func (heimdallStore) Milestones() heimdall.EntityStore[*heimdall.Milestone] {
	return nil
}
func (hs heimdallStore) Spans() heimdall.EntityStore[*heimdall.Span] {
	return hs.spans
}
func (heimdallStore) SpanBlockProducerSelections() heimdall.EntityStore[*heimdall.SpanBlockProducerSelection] {
	return nil
}
func (heimdallStore) Prepare(ctx context.Context) error {
	return nil
}
func (heimdallStore) Close() {
}

func NewHeimdallSimulator(ctx context.Context, snapDir string, logger log.Logger, iterations []uint64) (*HeimdallSimulator, error) {
	snapshots := heimdall.NewRoSnapshots(ethconfig.Defaults.Snapshot, snapDir, 0, logger)

	// index local files
	localFiles, err := os.ReadDir(snapDir)
	if err != nil {
		return nil, err
	}

	for _, file := range localFiles {
		info, _, _ := snaptype.ParseFileName(snapDir, file.Name())
		if info.Ext == ".seg" {
			err = info.Type.BuildIndexes(ctx, info, nil, nil, snapDir, nil, log.LvlWarn, logger)
			if err != nil {
				return nil, err
			}
		}
	}

	if err = snapshots.OpenFolder(); err != nil {
		return nil, err
	}

	h := HeimdallSimulator{
		snapshots: snapshots,
		blockReader: freezeblocks.NewBlockReader(nil, snapshots,
			heimdallStore{
				spans: heimdall.NewSpanSnapshotStore(heimdall.NoopEntityStore[*heimdall.Span]{Type: heimdall.Spans}, snapshots),
			}),
		bridgeStore: bridge.NewSnapshotStore(noopBridgeStore{}, snapshots, sprintLengthCalculator{}),

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

	span, _, err := h.getSpan(ctx, latestSpan)
	if err != nil {
		return nil, err
	}

	return span, nil
}

func (h *HeimdallSimulator) FetchSpan(ctx context.Context, spanID uint64) (*heimdall.Span, error) {
	if spanID > uint64(heimdall.SpanIdAt(h.lastAvailableBlockNumber)) {
		return nil, errors.New("span not found")
	}

	span, _, err := h.getSpan(ctx, spanID)
	if err != nil {
		return nil, err
	}

	return span, err
}

func (h *HeimdallSimulator) FetchSpans(ctx context.Context, page uint64, limit uint64) ([]*heimdall.Span, error) {
	return nil, errors.New("method FetchSpans is not implemented")
}

func (h *HeimdallSimulator) FetchStateSyncEvents(_ context.Context, fromId uint64, to time.Time, limit int) ([]*bridge.EventRecordWithTime, error) {
	events, _, err := h.bridgeStore.EventsByIdFromSnapshot(fromId, to, limit)
	return events, err
}

func (h *HeimdallSimulator) FetchStateSyncEvent(ctx context.Context, id uint64) (*bridge.EventRecordWithTime, error) {
	return nil, errors.New("method FetchStateSyncEvent not implemented")
}

func (h *HeimdallSimulator) FetchStatus(ctx context.Context) (*heimdall.Status, error) {
	return nil, errors.New("method FetchStatus not implemented")
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

func (h *HeimdallSimulator) getSpan(ctx context.Context, spanId uint64) (*heimdall.Span, bool, error) {
	return h.blockReader.Span(ctx, nil, spanId)
}
