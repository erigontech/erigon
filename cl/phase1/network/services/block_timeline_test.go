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
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
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
	binary.BigEndian.PutUint64(block.Signature[:], slot)
	binary.BigEndian.PutUint64(block.Signature[8:], proposer)
	block.Signature[16] = byte(blobs)
	return block
}

func importOnce(timelines *blockTimelines, block *cltypes.SignedBeaconBlock, root common.Hash, start, end time.Time, err error) {
	timelines.importStarted(block, start)
	timelines.importEnded(block, root, start, end, err == nil, err)
}

func nextRecord(t *testing.T, records chan *log.Record) *log.Record {
	t.Helper()
	select {
	case r := <-records:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("no timeline line")
		return nil
	}
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
	importOnce(timelines, block, root, at(1600*time.Millisecond), at(1700*time.Millisecond), forkchoice.ErrEIP7594ColumnDataNotAvailable)
	importOnce(timelines, block, root, at(3900*time.Millisecond), at(4300*time.Millisecond), nil)

	r := nextRecord(t, records)
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
}

func TestBlockTimelineMarksAStageThatWasNotSeen(t *testing.T) {
	logger, records := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
	go timelines.run(t.Context())
	block, root := timelineBlock(timelineSlot, 0), common.Hash{0x0b}

	importOnce(timelines, block, root, at(time.Second), at(1200*time.Millisecond), nil)

	fields := recordFields(t, nextRecord(t, records))
	require.Equal(t, "-", fields["seenMs"], "a block that did not come through gossip has no arrival time")
	require.Equal(t, int64(1000), fields["firstImportMs"])
	require.Equal(t, 1, fields["attempts"])
	require.Equal(t, false, fields["waitedForColumns"])
}

func TestBlockTimelineTracksBlocksUpToTwoSlotsOld(t *testing.T) {
	for _, tc := range []struct {
		age   uint64
		lines int
	}{{age: 2, lines: 1}, {age: 3, lines: 0}} {
		logger, _ := timelineLogger(log.LvlDebug)
		timelines := newBlockTimelines(timelineClock(t, timelineSlot+tc.age), logger)
		block := timelineBlock(timelineSlot, 0)

		timelines.seen(block, at(time.Second))
		importOnce(timelines, block, common.Hash{0x0c}, at(time.Second), at(2*time.Second), nil)

		require.Len(t, timelines.lines, tc.lines, "block %d slots old", tc.age)
		require.Len(t, timelines.pending, tc.lines, "block %d slots old", tc.age)
	}
}

func TestBlockTimelineIsOffWithoutDebugLogging(t *testing.T) {
	logger, records := timelineLogger(log.LvlInfo)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
	go timelines.run(t.Context())
	block, root := timelineBlock(timelineSlot, 0), common.Hash{0x0d}

	timelines.seen(block, at(time.Second))
	importOnce(timelines, block, root, at(time.Second), at(2*time.Second), nil)

	require.Empty(t, timelines.pending)
	require.Empty(t, timelines.lines)
	require.Empty(t, records)
}

func TestBlockTimelineDropsLinesInsteadOfBlockingAnImport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logger, _ := timelineLogger(log.LvlDebug)
		timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
		for range blockTimelineBuffer {
			timelines.lines <- blockTimeline{}
		}
		block := timelineBlock(timelineSlot, 0)

		// A blocking send deadlocks the bubble and fails the test.
		importOnce(timelines, block, common.Hash{0x11}, at(time.Second), at(2*time.Second), nil)

		require.Len(t, timelines.lines, blockTimelineBuffer)
		require.True(t, timelines.pending[block.Signature].logged, "the import reached the send")
	})
}

func TestBlockTimelineForgetsABlockThatNeverImports(t *testing.T) {
	logger, _ := timelineLogger(log.LvlDebug)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	currentSlot := uint64(timelineSlot)
	clock.EXPECT().GetCurrentSlot().DoAndReturn(func() uint64 { return currentSlot }).AnyTimes()
	timelines := newBlockTimelines(clock, logger)

	stuck := timelineBlock(timelineSlot, 0)
	timelines.seen(stuck, at(time.Second))
	importOnce(timelines, stuck, common.Hash{0x0e}, at(time.Second), at(2*time.Second), errors.New("parent unknown"))
	require.Len(t, timelines.pending, 1)

	currentSlot = timelineSlot + blockTimelineSlots + 1
	next := timelineBlock(currentSlot, 0)
	timelines.seen(next, at(time.Second))
	require.Len(t, timelines.pending, 1)
	require.Contains(t, timelines.pending, next.Signature)
}

func TestBlockTimelineKeepsPendingBlocksBounded(t *testing.T) {
	logger, _ := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)

	for i := range 2 * maxPendingBlockTimelines {
		timelines.seen(timelineBlockByProposer(timelineSlot, uint64(i), 0), at(time.Second))
	}
	require.Len(t, timelines.pending, maxPendingBlockTimelines)
}

func TestBlockTimelineGivesEachBlockOfOneProposerAndSlotItsOwnLine(t *testing.T) {
	logger, _ := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
	gossiped, other := timelineBlock(timelineSlot, 2), timelineBlock(timelineSlot, 5)
	other.Block.Body.ExecutionPayload.BlockNumber++

	timelines.seen(gossiped, at(time.Second))
	importOnce(timelines, gossiped, common.Hash{0x0f}, at(2*time.Second), at(3*time.Second), nil)
	importOnce(timelines, other, common.Hash{0x10}, at(4*time.Second), at(5*time.Second), nil)

	require.Len(t, timelines.lines, 2)
	first, second := <-timelines.lines, <-timelines.lines
	require.Equal(t, common.Hash{0x0f}, first.root)
	require.Equal(t, 2, first.blobs)
	require.Equal(t, at(time.Second), first.seen)
	require.Equal(t, common.Hash{0x10}, second.root)
	require.Equal(t, 5, second.blobs)
	require.Equal(t, other.Block.Body.ExecutionPayload.BlockNumber, second.number)
	require.True(t, second.seen.IsZero(), "the other block did not arrive on gossip")
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

	importOnce(timelines, block, common.Hash{0x11}, at(time.Second), at(2*time.Second), nil)

	require.Equal(t, 3, recordFields(t, nextRecord(t, records))["blobs"])
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
	line := service.timelines.pending[block.Signature]
	require.NotNil(t, line, "a block queued for its parent passed the signature check")
	require.False(t, line.seen.IsZero())
}

func TestBlockTimelineOfABlockWithoutExecutionPayloadHasNoFakeNumber(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.Phase0Version, clparams.AltairVersion, clparams.GloasVersion} {
		logger, _ := timelineLogger(log.LvlDebug)
		timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, version)
		block.Block.Slot = timelineSlot

		importOnce(timelines, block, common.Hash{0x11}, at(time.Second), at(2*time.Second), nil)

		require.Len(t, timelines.lines, 1)
		require.Equal(t, "-", (<-timelines.lines).number, "%s block", version)
	}
}

func TestBlockNotAcceptedByForkChoiceLogsNoTimeline(t *testing.T) {
	service, block, processing, _ := newFuluColumnGossipFixture(t)
	logger, _ := timelineLogger(log.LvlDebug)
	service.timelines = newBlockTimelines(timelineClock(t, block.Block.Slot), logger)
	// The fork choice mock returns no error from OnBlock but does not take the block, as fork choice does for a block
	// at the finalized horizon.
	service.forkchoiceStore = processing.ForkChoiceStorage

	accepted, err := service.processAndStoreBlock(t.Context(), block, false)
	require.NoError(t, err)
	require.False(t, accepted)
	require.Empty(t, service.timelines.lines, "an import is complete only once fork choice holds the block")
	require.Zero(t, service.timelines.pending[block.Signature].inFlight, "the attempt has ended")
}

func TestBlockTimelineOfOverlappingImportsCountsBothAndLogsOnce(t *testing.T) {
	logger, _ := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
	block, root := timelineBlock(timelineSlot, 0), common.Hash{0x10}

	timelines.seen(block, at(900*time.Millisecond))
	timelines.importStarted(block, at(time.Second))
	timelines.importStarted(block, at(1100*time.Millisecond))
	timelines.importEnded(block, root, at(1100*time.Millisecond), at(1200*time.Millisecond), true, nil)
	timelines.importEnded(block, root, at(time.Second), at(1300*time.Millisecond), true, nil)

	require.Len(t, timelines.lines, 1)
	line := <-timelines.lines
	require.Equal(t, 2, line.attempts)
	require.Equal(t, at(time.Second), line.firstImport)
	require.Equal(t, at(1100*time.Millisecond), line.importStart)
	require.Equal(t, at(1200*time.Millisecond), line.imported)
	require.Equal(t, at(900*time.Millisecond), line.seen)
}

func TestBlockTimelineKeepsTheEarliestArrivalAndStart(t *testing.T) {
	logger, _ := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
	block, root := timelineBlock(timelineSlot, 0), common.Hash{0x12}

	timelines.seen(block, at(1100*time.Millisecond))
	timelines.seen(block, at(time.Second))
	timelines.importStarted(block, at(1300*time.Millisecond))
	timelines.importStarted(block, at(1200*time.Millisecond))
	timelines.importEnded(block, root, at(1200*time.Millisecond), at(1400*time.Millisecond), true, nil)

	require.Len(t, timelines.lines, 1)
	line := <-timelines.lines
	require.Equal(t, at(time.Second), line.seen)
	require.Equal(t, at(1200*time.Millisecond), line.firstImport)
	require.Equal(t, at(1200*time.Millisecond), line.importStart)
}

func TestBlockTimelineOfABlockImportedByAnotherPathHasNoImportTimes(t *testing.T) {
	service, block, processing, _ := newFuluColumnGossipFixture(t)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	logger, _ := timelineLogger(log.LvlDebug)
	service.timelines = newBlockTimelines(timelineClock(t, block.Block.Slot), logger)

	require.NoError(t, service.ProcessMessage(t.Context(), nil, block))
	require.Empty(t, service.timelines.lines, "the block waits for its columns")
	processing.headers.Store(common.Hash(root), block.SignedBeaconBlockHeader().Header)
	accepted, err := service.processAndStoreBlock(t.Context(), block, false)
	require.NoError(t, err)
	require.True(t, accepted)

	require.Len(t, service.timelines.lines, 1)
	line := <-service.timelines.lines
	require.Equal(t, common.Hash(root), line.root)
	require.False(t, line.seen.IsZero())
	require.Equal(t, 1, line.attempts)
	require.True(t, line.waitedForColumns)
	require.True(t, line.importStart.IsZero())
	require.True(t, line.imported.IsZero())
}

func TestBlockTimelineOfAnUntrackedBlockImportedByAnotherPathIsNotLogged(t *testing.T) {
	logger, _ := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)

	timelines.importedElsewhere(timelineBlock(timelineSlot, 0), common.Hash{0x13})

	require.Empty(t, timelines.pending)
	require.Empty(t, timelines.lines)
}

func TestBlockTimelineImportReplacesAnArrivalWhenFull(t *testing.T) {
	logger, _ := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
	for i := range maxPendingBlockTimelines {
		timelines.seen(timelineBlockByProposer(timelineSlot, uint64(i), 0), at(time.Second))
	}
	block := timelineBlockByProposer(timelineSlot, maxPendingBlockTimelines, 0)

	importOnce(timelines, block, common.Hash{0x14}, at(time.Second), at(2*time.Second), nil)

	require.Len(t, timelines.pending, maxPendingBlockTimelines)
	require.Len(t, timelines.lines, 1)
}

func TestBlockTimelineImportKeepsEntriesWithAttemptsWhenFull(t *testing.T) {
	logger, _ := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
	for i := range maxPendingBlockTimelines {
		timelines.importStarted(timelineBlockByProposer(timelineSlot, uint64(i), 0), at(time.Second))
	}
	block := timelineBlockByProposer(timelineSlot, maxPendingBlockTimelines, 0)

	importOnce(timelines, block, common.Hash{0x15}, at(time.Second), at(2*time.Second), nil)

	require.Len(t, timelines.pending, maxPendingBlockTimelines)
	require.NotContains(t, timelines.pending, block.Signature)
	require.Empty(t, timelines.lines)
}

func TestBlockTimelineWaitsForAnImportInFlightBeforeTheFallback(t *testing.T) {
	logger, _ := timelineLogger(log.LvlDebug)
	timelines := newBlockTimelines(timelineClock(t, timelineSlot), logger)
	block, root := timelineBlock(timelineSlot, 0), common.Hash{0x16}

	timelines.importStarted(block, at(time.Second))
	timelines.importedElsewhere(block, root)
	require.Empty(t, timelines.lines, "the import in flight logs the line")
	timelines.importEnded(block, root, at(time.Second), at(2*time.Second), true, nil)

	require.Len(t, timelines.lines, 1)
	line := <-timelines.lines
	require.Equal(t, at(time.Second), line.importStart)
	require.Equal(t, at(2*time.Second), line.imported)
}

// pausingStore publishes the block in fork choice and then waits, as OnBlock does while it finishes an import.
type pausingStore struct {
	*onBlockErrorStore
	published, release chan struct{}
}

func (s pausingStore) OnBlock(ctx context.Context, block *cltypes.SignedBeaconBlock, newPayload, fullValidation, checkDataAvailability bool) error {
	err := s.onBlockErrorStore.OnBlock(ctx, block, newPayload, fullValidation, checkDataAvailability)
	close(s.published)
	<-s.release
	return err
}

func TestBlockSeenInForkChoiceDuringAnImportGetsTheTimedLine(t *testing.T) {
	service, block, processing, _ := newFuluColumnGossipFixture(t)
	logger, _ := timelineLogger(log.LvlDebug)
	service.timelines = newBlockTimelines(timelineClock(t, block.Block.Slot), logger)
	processing.err = nil
	store := pausingStore{onBlockErrorStore: processing, published: make(chan struct{}), release: make(chan struct{})}
	service.forkchoiceStore = store

	imported := make(chan error, 1)
	go func() {
		_, err := service.processAndStoreBlock(t.Context(), block, false)
		imported <- err
	}()
	<-store.published
	accepted, err := service.processAndStoreBlock(t.Context(), block, false)
	require.NoError(t, err)
	require.True(t, accepted)
	close(store.release)
	require.NoError(t, <-imported)

	require.Len(t, service.timelines.lines, 1)
	line := <-service.timelines.lines
	require.False(t, line.importStart.IsZero(), "the line has the times of the import")
	require.False(t, line.imported.IsZero())
}
