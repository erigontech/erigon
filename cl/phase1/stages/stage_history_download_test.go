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

package stages

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

type historyDownloaderStub struct {
	finished       bool
	progress       uint64
	requestErr     error
	requestMore    func() error
	skipped        []network.SkippedFullBlock
	recoverySource bool
}

func (d *historyDownloaderStub) SetSlotToDownload(uint64)             {}
func (d *historyDownloaderStub) SetExpectedRoot(common.Hash)          {}
func (d *historyDownloaderStub) SetBlockChecker(network.BlockChecker) {}
func (d *historyDownloaderStub) SetOnNewBlock(network.OnNewBlock)     {}
func (d *historyDownloaderStub) Finished() bool                       { return d.finished }
func (d *historyDownloaderStub) Progress() uint64                     { return d.progress }
func (d *historyDownloaderStub) RequestMore(context.Context) error {
	if d.requestMore != nil {
		return d.requestMore()
	}
	return d.requestErr
}
func (d *historyDownloaderStub) SkippedFullBlocks() []network.SkippedFullBlock {
	return d.skipped
}
func (d *historyDownloaderStub) HasEnvelopeRecoverySource() bool { return d.recoverySource }
func (d *historyDownloaderStub) RecoverSkippedEnvelopes(context.Context, []network.SkippedFullBlock, map[common.Hash]*cltypes.SignedBeaconBlock) network.EnvelopeRecoveryResult {
	return network.EnvelopeRecoveryResult{}
}
func (d *historyDownloaderStub) SetThrottle(time.Duration) {}
func (d *historyDownloaderStub) SetNeverSkip(bool)         {}

func TestSpawnStageHistoryDownloadReturnsDownloaderFailure(t *testing.T) {
	wantErr := errors.New("terminal downloader failure")
	downloader := &historyDownloaderStub{progress: math.MaxUint64, requestErr: wantErr}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
		beaconCfg:  &clparams.MainnetBeaconConfig,
		downloader: downloader,
	}, ctx, log.New())
	if !errors.Is(err, wantErr) {
		t.Fatalf("SpawnStageHistoryDownload() error = %v, want %v", err, wantErr)
	}
}

func TestSpawnStageHistoryDownloadReturnsFailureWhenRequestCrossesELFloor(t *testing.T) {
	wantErr := errors.New("terminal downloader failure at EL floor")
	destinationSlot := clparams.MainnetBeaconConfig.BellatrixForkEpoch * clparams.MainnetBeaconConfig.SlotsPerEpoch
	downloader := &historyDownloaderStub{progress: destinationSlot + 1}
	downloader.requestMore = func() error {
		downloader.progress = destinationSlot
		return wantErr
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err := SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
		beaconCfg:  &clparams.MainnetBeaconConfig,
		downloader: downloader,
		engine:     &testExecutionEngine{supportInsertion: true},
	}, ctx, log.New())
	if !errors.Is(err, wantErr) {
		t.Fatalf("SpawnStageHistoryDownload() error = %v, want %v", err, wantErr)
	}
}

func TestSpawnStageHistoryDownloadReturnsEnvelopeRecoveryFailure(t *testing.T) {
	downloader := &historyDownloaderStub{
		finished: true,
		skipped:  []network.SkippedFullBlock{{Slot: 1}},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := SpawnStageHistoryDownload(StageHistoryReconstructionCfg{
		beaconCfg:  &clparams.MainnetBeaconConfig,
		downloader: downloader,
	}, ctx, log.New())
	if err == nil {
		t.Fatal("SpawnStageHistoryDownload() returned nil after envelope recovery failed")
	}
}

func TestUnresolvedSkippedEnvelopesRetriesEveryMissingEnvelope(t *testing.T) {
	first := network.SkippedFullBlock{Slot: 1, Root: [32]byte{1}}
	second := network.SkippedFullBlock{Slot: 2, Root: [32]byte{2}}
	result := network.EnvelopeRecoveryResult{}

	remaining := unresolvedSkippedEnvelopes([]network.SkippedFullBlock{first, second}, result, func(network.SkippedFullBlock, *cltypes.SignedExecutionPayloadEnvelope) bool {
		t.Fatal("missing envelopes must not be persisted")
		return false
	})

	if len(remaining) != 2 || remaining[0] != first || remaining[1] != second {
		t.Fatalf("remaining = %v, want both missing envelopes", remaining)
	}
}

func TestRecoverSkippedEnvelopeBatchesDoesNotStarveLaterBatches(t *testing.T) {
	skipped := []network.SkippedFullBlock{{Slot: 1}, {Slot: 2}, {Slot: 3}, {Slot: 4}, {Slot: 5}, {Slot: 6}, {Slot: 7}, {Slot: 8}}
	attempted := make([]uint64, 0, len(skipped))
	recoverBatch := func(ctx, _ context.Context, batch []network.SkippedFullBlock) []network.SkippedFullBlock {
		attempted = append(attempted, batch[0].Slot)
		if batch[0].Slot < 7 {
			<-ctx.Done()
			return batch
		}
		return nil
	}

	pending := recoverSkippedEnvelopeBatches(context.Background(), skipped, 2, time.Millisecond, recoverBatch)
	if len(attempted) != 4 || attempted[3] != 7 {
		t.Fatalf("attempted batch starts = %v, want [1 3 5 7]", attempted)
	}
	for _, item := range pending {
		if item.Slot >= 7 {
			t.Fatalf("later recoverable item %d remained pending", item.Slot)
		}
	}
}

func TestRecoverSkippedEnvelopeBatchesKeepsPartialSuccess(t *testing.T) {
	skipped := []network.SkippedFullBlock{{Slot: 1}, {Slot: 2}}
	recoverBatch := func(fetchCtx, persistCtx context.Context, batch []network.SkippedFullBlock) []network.SkippedFullBlock {
		<-fetchCtx.Done()
		if persistCtx.Err() != nil {
			t.Fatalf("persist context expired with fetch context: %v", persistCtx.Err())
		}
		return batch[1:]
	}

	pending := recoverSkippedEnvelopeBatches(context.Background(), skipped, 2, time.Millisecond, recoverBatch)
	if len(pending) != 1 || pending[0].Slot != 2 {
		t.Fatalf("pending = %v, want only slot 2", pending)
	}
}

func TestRecoverSkippedEnvelopesWithoutSourcesDoesNotCompleteBackfill(t *testing.T) {
	cfg := StageHistoryReconstructionCfg{downloader: &network.BackwardBeaconDownloader{}}
	attempts := 0
	recoverAttempt := func(_ context.Context, pending []network.SkippedFullBlock) []network.SkippedFullBlock {
		attempts++
		return pending
	}

	if recoverSkippedEnvelopesWithRetryPolicy(context.Background(), cfg, []network.SkippedFullBlock{{Slot: 1}}, recoverAttempt, 0) {
		t.Fatal("recovery without an HTTP or P2P source must not report completion")
	}
	if attempts != 0 {
		t.Fatalf("attempts = %d, want no recovery attempt without a source", attempts)
	}
}

func TestRecoverSkippedEnvelopesRetriesBeyondThreeAttemptCapacity(t *testing.T) {
	const itemsPerAttempt = int(skippedEnvelopeRecoveryAttemptTimeout/skippedEnvelopeRecoveryBatchTimeout) * skippedEnvelopeRecoveryBatchSize
	downloader := &network.BackwardBeaconDownloader{}
	downloader.SetHTTPFallbackURL("http://recovery.test")
	cfg := StageHistoryReconstructionCfg{downloader: downloader}
	skipped := make([]network.SkippedFullBlock, itemsPerAttempt*3+1)
	for i := range skipped {
		skipped[i].Slot = uint64(i + 1)
	}

	attemptStarts := make([]uint64, 0, 4)
	recoverAttempt := func(_ context.Context, pending []network.SkippedFullBlock) []network.SkippedFullBlock {
		attemptStarts = append(attemptStarts, pending[0].Slot)
		return pending[min(itemsPerAttempt, len(pending)):]
	}

	if !recoverSkippedEnvelopesWithRetryPolicy(context.Background(), cfg, skipped, recoverAttempt, 0) {
		t.Fatal("configured recovery stopped before all pending envelopes were recovered")
	}
	if len(attemptStarts) != 4 || attemptStarts[3] != uint64(itemsPerAttempt*3+1) {
		t.Fatalf("attempt starts = %v, want a fourth attempt starting at slot %d", attemptStarts, itemsPerAttempt*3+1)
	}
}

func TestRecoverSkippedEnvelopesStopsWhenParentContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	downloader := &network.BackwardBeaconDownloader{}
	downloader.SetHTTPFallbackURL("http://recovery.test")
	cfg := StageHistoryReconstructionCfg{downloader: downloader}
	attempts := 0
	recoverAttempt := func(_ context.Context, pending []network.SkippedFullBlock) []network.SkippedFullBlock {
		attempts++
		cancel()
		return pending
	}

	if recoverSkippedEnvelopesWithRetryPolicy(ctx, cfg, []network.SkippedFullBlock{{Slot: 1}}, recoverAttempt, time.Hour) {
		t.Fatal("recovery reported completion with a pending envelope after parent cancellation")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

// clampProgress must never report a total below processed nor underflow, even
// when the floor and current counters drift past the frozen highestBlockSeen.
// The last case mirrors the field report where the live EL head advanced past
// the frozen top and previously underflowed the denominator to ~2^64.
func TestClampProgress(t *testing.T) {
	cases := []struct {
		name                     string
		highest, floor, current  uint64
		wantProcessed, wantTotal uint64
	}{
		{"normal", 100, 20, 60, 40, 80},
		{"floor above top", 100, 150, 60, 40, 40},
		{"current above top", 100, 20, 200, 0, 80},
		{"current below floor grows total", 100, 20, 5, 95, 95},
		{"el head past frozen tip", 23_000_000, 23_123_953, 22_983_559, 16_441, 16_441},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			processed, total := clampProgress(tc.highest, tc.floor, tc.current)
			if processed != tc.wantProcessed || total != tc.wantTotal {
				t.Fatalf("clampProgress(%d,%d,%d) = (%d,%d), want (%d,%d)",
					tc.highest, tc.floor, tc.current, processed, total, tc.wantProcessed, tc.wantTotal)
			}
			if processed > total {
				t.Fatalf("processed (%d) exceeds total (%d)", processed, total)
			}
		})
	}
}

// Post-merge the EL block number exceeds the beacon slot, so a snapshot-gap
// floor must be compared against EL block progress, not the slot.
func TestELBackfillFinished_GapUsesBlockNotSlot(t *testing.T) {
	const (
		bellatrixSlot = uint64(4_636_672) // a real beacon-slot floor
		frozenBlock   = uint64(25_073_000)
		headSlot      = uint64(14_460_640)
		headBlock     = uint64(25_224_522)
	)
	destBlock := frozenBlock - 1

	if elBackfillFinished(headSlot, headBlock, bellatrixSlot, destBlock) {
		t.Fatalf("backfill reported finished at the tip (slot=%d block=%d) while gap down to block %d is unfilled",
			headSlot, headBlock, destBlock)
	}

	// Once EL block progress has descended to the frozen tip, it is finished.
	if !elBackfillFinished(headSlot-150_000, destBlock, bellatrixSlot, destBlock) {
		t.Fatalf("backfill should be finished once EL block progress reaches the frozen tip (block %d)", destBlock)
	}
}

// Without a snapshot gap, the EL block floor is unset and completion is driven
// purely by the beacon-slot floor (normal Deneb backfill toward the merge).
func TestELBackfillFinished_NoGapUsesSlotFloor(t *testing.T) {
	const bellatrixSlot = uint64(4_636_672)
	noBlockFloor := uint64(math.MaxUint64)

	if elBackfillFinished(bellatrixSlot+1, 20_000_000, bellatrixSlot, noBlockFloor) {
		t.Fatal("backfill must continue while still above the beacon-slot floor")
	}
	if !elBackfillFinished(bellatrixSlot, 20_000_000, bellatrixSlot, noBlockFloor) {
		t.Fatal("backfill must finish once the beacon-slot floor is reached")
	}
}
