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

package services

import (
	"context"
	"errors"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

const timelineSlot = 100

var timelineSlotStart = time.Unix(1_700_000_000, 0)

func timelineClock(t *testing.T, currentSlot uint64) *eth_clock.MockEthereumClock {
	t.Helper()
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(currentSlot).AnyTimes()
	clock.EXPECT().GetSlotTime(gomock.Any()).DoAndReturn(func(slot uint64) time.Time {
		return timelineSlotStart.Add(time.Duration(int64(slot)-timelineSlot) * 12 * time.Second)
	}).AnyTimes()
	return clock
}

func timelineLogger(lvl log.Lvl) (log.Logger, chan *log.Record) {
	records := make(chan *log.Record, 16)
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(lvl, log.ChannelHandler(records)))
	return logger, records
}

func timelineBlock(slot uint64, blobs int) *cltypes.SignedBeaconBlock {
	return timelineBlockByProposer(slot, 0, blobs)
}

func timelineBlockByProposer(slot, proposer uint64, blobs int) *cltypes.SignedBeaconBlock {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	block.Block.Slot = slot
	block.Block.ProposerIndex = proposer
	block.Block.Body.ExecutionPayload.BlockNumber = 26_000_000 + slot
	for range blobs {
		block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	}
	return block
}

func at(offset time.Duration) time.Time { return timelineSlotStart.Add(offset) }

func recordFields(t *testing.T, r *log.Record) map[string]any {
	t.Helper()
	fields := map[string]any{}
	for i := 0; i+1 < len(r.Ctx); i += 2 {
		fields[r.Ctx[i].(string)] = r.Ctx[i+1]
	}
	return fields
}

func TestBlockTimelineLogsTheStagesOfAnImportedBlock(t *testing.T) {
	logger, records := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
	go timelines.run(t.Context())
	block, root := timelineBlock(timelineSlot, 18), common.Hash{0x0a}

	timelines.seen(block, at(1500*time.Millisecond))
	timelines.importAttempt(block, root, at(1600*time.Millisecond), at(1700*time.Millisecond), forkchoice.ErrEIP7594ColumnDataNotAvailable)
	timelines.importAttempt(block, root, at(3900*time.Millisecond), at(4300*time.Millisecond), nil)

	select {
	case r := <-records:
		require.Equal(t, log.LvlDebug, r.Lvl)
		require.Equal(t, map[string]any{
			"slot":             uint64(timelineSlot),
			"root":             root,
			"number":           uint64(26_000_000 + timelineSlot),
			"blobs":            18,
			"seenMs":           int64(1500),
			"firstImportMs":    int64(1600),
			"importStartMs":    int64(3900),
			"importedMs":       int64(4300),
			"attempts":         2,
			"waitedForColumns": true,
		}, recordFields(t, r))
	case <-time.After(5 * time.Second):
		t.Fatal("no timeline line")
	}
}

func TestBlockTimelineMarksAStageThatWasNotSeen(t *testing.T) {
	logger, records := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
	go timelines.run(t.Context())
	block, root := timelineBlock(timelineSlot, 0), common.Hash{0x0b}

	timelines.importAttempt(block, root, at(time.Second), at(1200*time.Millisecond), nil)

	r := <-records
	fields := recordFields(t, r)
	require.Equal(t, "-", fields["seenMs"], "a block that did not come through gossip has no arrival time")
	require.Equal(t, int64(1000), fields["firstImportMs"])
	require.Equal(t, 1, fields["attempts"])
	require.Equal(t, false, fields["waitedForColumns"])
}

func TestBlockTimelineSkipsBlocksOfOldSlots(t *testing.T) {
	logger, records := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot+blockTimelineSlots+1), logger)
	go timelines.run(t.Context())
	block, root := timelineBlock(timelineSlot, 0), common.Hash{0x0c}

	timelines.seen(block, at(time.Second))
	timelines.importAttempt(block, root, at(time.Second), at(2*time.Second), nil)

	require.Never(t, func() bool { return len(records) > 0 }, 100*time.Millisecond, 10*time.Millisecond)
	require.Empty(t, timelines.pending)
}

func TestBlockTimelineIsOffWithoutDebugLogging(t *testing.T) {
	logger, records := timelineLogger(log.LvlInfo)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
	go timelines.run(t.Context())
	block, root := timelineBlock(timelineSlot, 0), common.Hash{0x0d}

	timelines.seen(block, at(time.Second))
	timelines.importAttempt(block, root, at(time.Second), at(2*time.Second), nil)

	require.Empty(t, timelines.pending)
	require.Empty(t, timelines.lines)
	require.Empty(t, records)
}

func TestBlockTimelineDropsLinesInsteadOfBlockingAnImport(t *testing.T) {
	logger, _ := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 2 * blockTimelineBuffer {
			timelines.importAttempt(timelineBlockByProposer(timelineSlot, uint64(i), 0), common.Hash{byte(i), 1}, at(time.Second), at(2*time.Second), nil)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("an import waited for the timeline writer")
	}
	require.Len(t, timelines.lines, blockTimelineBuffer)
}

func TestBlockTimelineForgetsABlockThatNeverImports(t *testing.T) {
	logger, _ := timelineLogger(log.LvlDebug)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	currentSlot := uint64(timelineSlot)
	clock.EXPECT().GetCurrentSlot().DoAndReturn(func() uint64 { return currentSlot }).AnyTimes()
	timelines := newBlockTimelines(clock, logger)

	stuck := timelineBlock(timelineSlot, 0)
	timelines.seen(stuck, at(time.Second))
	timelines.importAttempt(stuck, common.Hash{0x0e}, at(time.Second), at(2*time.Second), errors.New("parent unknown"))
	require.Len(t, timelines.pending, 1)

	currentSlot = timelineSlot + blockTimelineSlots + 1
	next := timelineBlock(currentSlot, 0)
	timelines.seen(next, at(time.Second))
	require.Len(t, timelines.pending, 1)
	require.Contains(t, timelines.pending, blockGossipKey(next))
}

func TestBlockTimelineKeepsPendingBlocksBounded(t *testing.T) {
	logger, _ := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)

	for i := range 2 * maxPendingBlockTimelines {
		timelines.seen(timelineBlockByProposer(timelineSlot, uint64(i), 0), at(time.Second))
	}
	require.Len(t, timelines.pending, maxPendingBlockTimelines)
}

func TestGossipBlockThatWaitsForColumnsLogsItsTimeline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		service, block, processing, peerDas := newFuluColumnGossipFixture(t)
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		logger, records := timelineLogger(log.LvlDebug)
		service.timelines = newBlockTimelines(timelineClock(t, block.Block.Slot), logger)
		storage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), service.beaconCfg, service.emitter)
		peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).DoAndReturn(func(slot uint64, root common.Hash) (bool, error) {
			return storage.ColumnSidecarExists(t.Context(), slot, root, 0)
		}).Times(2)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go service.timelines.run(ctx)
		go service.loop(ctx)
		synctest.Wait()

		require.NoError(t, service.ProcessMessage(t.Context(), nil, block))
		synctest.Wait()
		processing.err = nil
		sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.FuluVersion)
		sidecar.SignedBlockHeader = block.SignedBeaconBlockHeader()
		require.NoError(t, storage.WriteColumnSidecars(t.Context(), root, 0, sidecar))
		synctest.Wait()

		require.Len(t, records, 1)
		fields := recordFields(t, <-records)
		require.Equal(t, common.Hash(root), fields["root"])
		require.Equal(t, 2, fields["attempts"])
		require.Equal(t, true, fields["waitedForColumns"])
		require.IsType(t, int64(0), fields["seenMs"], "a gossip block has an arrival time")
	})
}

func TestBlockTimelineTakesTheLineFieldsFromTheImportedBlock(t *testing.T) {
	logger, records := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
	go timelines.run(t.Context())
	first, imported := timelineBlock(timelineSlot, 2), timelineBlock(timelineSlot, 5)
	imported.Block.Body.ExecutionPayload.BlockNumber++

	timelines.seen(first, at(time.Second))
	timelines.importAttempt(imported, common.Hash{0x10}, at(2*time.Second), at(3*time.Second), nil)

	fields := recordFields(t, <-records)
	require.Equal(t, imported.Block.Body.ExecutionPayload.BlockNumber, fields["number"])
	require.Equal(t, 5, fields["blobs"])
	require.Equal(t, common.Hash{0x10}, fields["root"])
}

func TestBlockTimelineCountsTheBlobsOfAGloasBid(t *testing.T) {
	logger, records := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
	go timelines.run(t.Context())
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	block.Block.Slot = timelineSlot
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	require.NotNil(t, bid)
	for range 3 {
		bid.Message.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	}

	timelines.importAttempt(block, common.Hash{0x11}, at(time.Second), at(2*time.Second), nil)

	require.Equal(t, 3, recordFields(t, <-records)["blobs"])
}

func TestBlockTimelineSkipsBlocksOfFutureSlots(t *testing.T) {
	logger, _ := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)

	timelines.seen(timelineBlock(timelineSlot+1, 0), at(time.Second))
	timelines.seen(timelineBlockByProposer(timelineSlot+2, 1, 0), at(time.Second))
	timelines.seen(timelineBlockByProposer(math.MaxUint64, 2, 0), at(time.Second))

	require.Len(t, timelines.pending, 1, "only a block of the next slot can arrive early")
}

func TestGossipBlockWithAnInvalidSignatureLeavesNoTimeline(t *testing.T) {
	service, block, _, _ := newFuluColumnGossipFixture(t)
	logger, _ := timelineLogger(log.LvlDebug)
	service.timelines = newBlockTimelines(timelineClock(t, block.Block.Slot), logger)
	block.Signature[0] ^= 0xff

	require.Error(t, service.ProcessMessage(t.Context(), nil, block))
	require.Empty(t, service.timelines.pending)
}

func TestGossipBlockQueuedForItsParentKeepsItsArrival(t *testing.T) {
	service, block, processing, _ := newFuluColumnGossipFixture(t)
	logger, _ := timelineLogger(log.LvlDebug)
	service.timelines = newBlockTimelines(timelineClock(t, block.Block.Slot), logger)
	delete(processing.ForkChoiceStorage.(*mock_services.ForkChoiceStorageMock).Headers, block.Block.ParentRoot)

	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	line := service.timelines.pending[blockGossipKey(block)]
	require.NotNil(t, line, "a block queued for its parent passed the signature check")
	require.False(t, line.seen.IsZero())
}

func TestBlockTimelineOfAGloasBlockHasNoFakeNumber(t *testing.T) {
	logger, records := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
	go timelines.run(t.Context())
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	block.Block.Slot = timelineSlot

	timelines.importAttempt(block, common.Hash{0x11}, at(time.Second), at(2*time.Second), nil)

	require.Equal(t, "-", recordFields(t, <-records)["number"], "a Gloas block carries no execution block number")
}

// notAcceptedStore returns no error from OnBlock but does not take the block, as fork choice does for a block at the
// finalized horizon.
type notAcceptedStore struct {
	forkchoice.ForkChoiceStorage
}

func (notAcceptedStore) OnBlock(context.Context, *cltypes.SignedBeaconBlock, bool, bool, bool) error {
	return nil
}

func (notAcceptedStore) GetHeader(common.Hash) (*cltypes.BeaconBlockHeader, bool) { return nil, false }

func TestBlockNotAcceptedByForkChoiceLogsNoTimeline(t *testing.T) {
	service, block, processing, _ := newFuluColumnGossipFixture(t)
	logger, _ := timelineLogger(log.LvlDebug)
	service.timelines = newBlockTimelines(timelineClock(t, block.Block.Slot), logger)
	service.forkchoiceStore = notAcceptedStore{ForkChoiceStorage: processing}

	accepted, err := service.processAndStoreBlock(t.Context(), block, false)
	require.NoError(t, err)
	require.False(t, accepted)
	require.Empty(t, service.timelines.lines, "an import is complete only once fork choice holds the block")
}

func TestBlockTimelineLogsABlockImportedByTwoPathsOnce(t *testing.T) {
	logger, _ := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
	block, root := timelineBlock(timelineSlot, 0), common.Hash{0x10}

	timelines.seen(block, at(time.Second))
	timelines.importAttempt(block, root, at(time.Second), at(1200*time.Millisecond), nil)
	timelines.importAttempt(block, root, at(1100*time.Millisecond), at(1300*time.Millisecond), nil)

	require.Len(t, timelines.lines, 1)
	line := <-timelines.lines
	require.Equal(t, 1, line.attempts)
	require.Equal(t, at(time.Second), line.seen)
}
