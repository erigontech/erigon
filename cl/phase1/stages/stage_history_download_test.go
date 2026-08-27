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
	"math"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/common"
)

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

func TestCompleteHistoryBackfillNotifiesAfterMoreThanThreeRecoveryAttempts(t *testing.T) {
	skipped := []network.SkippedFullBlock{{Root: [32]byte{1}}}
	attempts := 0
	notified := false

	completed := completeHistoryBackfill(
		context.Background(), skipped, 0,
		func(context.Context, []network.SkippedFullBlock) []network.SkippedFullBlock {
			attempts++
			if attempts == 5 {
				return nil
			}
			return skipped
		},
		func() { notified = true },
	)

	require.True(t, completed)
	require.Equal(t, 5, attempts)
	require.True(t, notified)
}

func TestRecoverSkippedEnvelopeRejectsBidCommitmentMismatchBeforeSideEffects(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	block.Block.Slot = 9
	block.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	block.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = common.HexToHash("0x01")
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	envelope.Message.BeaconBlockRoot = root
	envelope.Message.Payload.SlotNumber = block.Block.Slot
	envelope.Message.Payload.BlockHash = common.HexToHash("0x02")

	require.False(t, recoverSkippedEnvelope(
		context.Background(),
		StageHistoryReconstructionCfg{beaconCfg: &clparams.MainnetBeaconConfig},
		network.SkippedFullBlock{Block: block, Root: root},
		envelope,
	))
}

func TestCompleteHistoryBackfillCancellationWithholdsNotification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	skipped := []network.SkippedFullBlock{{Root: [32]byte{1}}}
	attempts := 0
	notified := false

	completed := completeHistoryBackfill(
		ctx, skipped, 0,
		func(context.Context, []network.SkippedFullBlock) []network.SkippedFullBlock {
			attempts++
			cancel()
			return skipped
		},
		func() { notified = true },
	)

	require.False(t, completed)
	require.Equal(t, 1, attempts)
	require.False(t, notified)
}

func TestCompleteHistoryBackfillNotifiesAfterPartialEnvelopeRecoveryCompletes(t *testing.T) {
	first := network.SkippedFullBlock{Root: [32]byte{1}}
	second := network.SkippedFullBlock{Root: [32]byte{2}}
	inputs := make([][][32]byte, 0, 2)
	notified := false

	completed := completeHistoryBackfill(
		context.Background(), []network.SkippedFullBlock{first, second}, 0,
		func(_ context.Context, pending []network.SkippedFullBlock) []network.SkippedFullBlock {
			roots := make([][32]byte, len(pending))
			for i := range pending {
				roots[i] = pending[i].Root
			}
			inputs = append(inputs, roots)
			if len(inputs) == 1 {
				return []network.SkippedFullBlock{second}
			}
			return nil
		},
		func() { notified = true },
	)

	require.True(t, completed)
	require.Equal(t, [][][32]byte{{first.Root, second.Root}, {second.Root}}, inputs)
	require.True(t, notified)
}

func TestCompleteHistoryBackfillBoundsRecoveryAndRotatesFailures(t *testing.T) {
	skipped := make([]network.SkippedFullBlock, 10)
	for i := range skipped {
		skipped[i].Root[0] = byte(i + 1)
	}
	inputs := make([][]byte, 0, 3)
	notified := false

	completed := completeHistoryBackfill(
		context.Background(), skipped, 0,
		func(_ context.Context, pending []network.SkippedFullBlock) []network.SkippedFullBlock {
			roots := make([]byte, len(pending))
			for i := range pending {
				roots[i] = pending[i].Root[0]
			}
			inputs = append(inputs, roots)
			if len(inputs) == 1 {
				return pending[1:]
			}
			return nil
		},
		func() { notified = true },
	)

	require.True(t, completed)
	require.Equal(t, [][]byte{
		{1, 2, 3, 4, 5, 6, 7, 8},
		{9, 10, 2, 3, 4, 5, 6, 7},
		{8},
	}, inputs)
	require.True(t, notified)
}

func TestCompleteHistoryBackfillPacesOnlyZeroProgress(t *testing.T) {
	t.Run("progress continues immediately", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		skipped := []network.SkippedFullBlock{{Root: [32]byte{1}}, {Root: [32]byte{2}}}
		attempts := 0

		completed := completeHistoryBackfill(
			ctx, skipped, time.Hour,
			func(_ context.Context, pending []network.SkippedFullBlock) []network.SkippedFullBlock {
				attempts++
				if attempts == 1 {
					return pending[1:]
				}
				return nil
			},
			func() {},
		)

		require.True(t, completed)
		require.Equal(t, 2, attempts)
	})

	t.Run("zero progress waits", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		skipped := []network.SkippedFullBlock{{Root: [32]byte{1}}}
		attempted := make(chan struct{}, 2)
		done := make(chan bool, 1)

		go func() {
			done <- completeHistoryBackfill(
				ctx, skipped, time.Hour,
				func(context.Context, []network.SkippedFullBlock) []network.SkippedFullBlock {
					attempted <- struct{}{}
					return skipped
				},
				func() {},
			)
		}()

		select {
		case <-attempted:
		case <-time.After(time.Second):
			t.Fatal("recovery was not attempted")
		}
		select {
		case <-attempted:
			t.Fatal("zero-progress recovery retried without pacing")
		case <-time.After(50 * time.Millisecond):
		}
		cancel()
		select {
		case completed := <-done:
			require.False(t, completed)
		case <-time.After(time.Second):
			t.Fatal("recovery did not stop after cancellation")
		}
	})
}

type skippedFullBlockTrackerStub struct {
	pending []network.SkippedFullBlock
}

func (s *skippedFullBlockTrackerStub) SkippedFullBlocks() []network.SkippedFullBlock {
	return slices.Clone(s.pending)
}

func (s *skippedFullBlockTrackerStub) MarkSkippedFullBlocksRecovered(roots []common.Hash) {
	recovered := make(map[common.Hash]struct{}, len(roots))
	for _, root := range roots {
		recovered[root] = struct{}{}
	}
	retained := s.pending[:0]
	for _, pending := range s.pending {
		if _, ok := recovered[common.Hash(pending.Root)]; !ok {
			retained = append(retained, pending)
		}
	}
	s.pending = retained
}

func (s *skippedFullBlockTrackerStub) SkippedFullBlocksAtCapacity() bool {
	return len(s.pending) >= 64
}

func TestTrackedSkippedFullBlockCapacityDrainResumesAfterPartialRecovery(t *testing.T) {
	tracker := &skippedFullBlockTrackerStub{pending: make([]network.SkippedFullBlock, 64)}
	for i := range tracker.pending {
		tracker.pending[i].Root[0] = byte(i + 1)
	}
	attempts := 0

	result := relieveTrackedHistoryBackfill(context.Background(), tracker,
		func(_ context.Context, pending []network.SkippedFullBlock) []network.SkippedFullBlock {
			attempts++
			return pending[:1]
		})

	require.Equal(t, trackedHistoryRelieved, result)
	require.Equal(t, 1, attempts)
	require.Len(t, tracker.pending, 57)
	require.Equal(t, byte(1), tracker.pending[0].Root[0])
}

func TestTrackedSkippedFullBlockCapacityDrainChecksPastUnavailableBatch(t *testing.T) {
	tracker := &skippedFullBlockTrackerStub{pending: make([]network.SkippedFullBlock, 64)}
	for i := range tracker.pending {
		tracker.pending[i].Root[0] = byte(i + 1)
	}
	attempts := 0

	result := relieveTrackedHistoryBackfill(context.Background(), tracker,
		func(_ context.Context, pending []network.SkippedFullBlock) []network.SkippedFullBlock {
			attempts++
			if pending[0].Root[0] == 1 {
				return pending
			}
			return nil
		})

	require.Equal(t, trackedHistoryRelieved, result)
	require.Equal(t, 2, attempts)
	require.Len(t, tracker.pending, 56)
	require.Equal(t, byte(1), tracker.pending[0].Root[0])
}

func TestTrackedSkippedFullBlockCapacityReliefReturnsStalledWithoutLosingEntries(t *testing.T) {
	tracker := &skippedFullBlockTrackerStub{pending: make([]network.SkippedFullBlock, 64)}
	for i := range tracker.pending {
		tracker.pending[i].Root[0] = byte(i + 1)
	}
	want := slices.Clone(tracker.pending)
	attempts := 0

	result := relieveTrackedHistoryBackfill(context.Background(), tracker,
		func(_ context.Context, pending []network.SkippedFullBlock) []network.SkippedFullBlock {
			attempts++
			return pending
		})

	require.Equal(t, trackedHistoryStalled, result)
	require.Equal(t, 8, attempts)
	require.Equal(t, want, tracker.pending)
}

func TestWaitForHistoryDownloadReadyReturnsOnStalledCapacity(t *testing.T) {
	stalled := make(chan struct{}, 1)
	stalled <- struct{}{}
	checks := 0

	err := waitForHistoryDownloadReady(context.Background(), stalled, func() bool {
		checks++
		return false
	})

	require.NoError(t, err)
	require.Equal(t, 1, checks)
}

func TestWaitForHistoryDownloadReadyPreservesReadyFastPath(t *testing.T) {
	require.NoError(t, waitForHistoryDownloadReady(context.Background(), nil, func() bool { return true }))
}

func TestWaitForHistoryDownloadReadyStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := waitForHistoryDownloadReady(ctx, nil, func() bool { return false })

	require.ErrorIs(t, err, context.Canceled)
}

func TestBlobBackfillNotificationWaitsForFinishedHistory(t *testing.T) {
	var historyFinished atomic.Bool
	var notifications atomic.Int32
	notify := notifyBlobBackfilledWhenHistoryReady(historyFinished.Load, func() {
		notifications.Add(1)
	})

	notify()
	notify()
	require.Zero(t, notifications.Load())

	historyFinished.Store(true)
	notify()
	require.Zero(t, notifications.Load())

	notify()
	notify()
	require.Equal(t, int32(1), notifications.Load())
}

func TestTrackedSkippedFullBlockRecoveryStreamsLongHistoryWithinBound(t *testing.T) {
	tracker := &skippedFullBlockTrackerStub{}
	for admitted := 0; admitted < 256; {
		for len(tracker.pending) < 64 && admitted < 256 {
			tracker.pending = append(tracker.pending, network.SkippedFullBlock{Root: [32]byte{byte(admitted), byte(admitted >> 8)}})
			admitted++
		}
		require.LessOrEqual(t, len(tracker.pending), 64)
		require.True(t, completeTrackedHistoryBackfill(context.Background(), tracker, 0,
			func(context.Context, []network.SkippedFullBlock) []network.SkippedFullBlock { return nil }, func() {}))
		require.Empty(t, tracker.pending)
	}
}

func TestTrackedSkippedFullBlockRecoveryRetainsPermanentlyUnavailableBatch(t *testing.T) {
	tracker := &skippedFullBlockTrackerStub{pending: make([]network.SkippedFullBlock, 64)}
	ctx, cancel := context.WithCancel(context.Background())
	notified := false
	completed := completeTrackedHistoryBackfill(ctx, tracker, 0,
		func(_ context.Context, pending []network.SkippedFullBlock) []network.SkippedFullBlock {
			cancel()
			return pending
		}, func() { notified = true })

	require.False(t, completed)
	require.Len(t, tracker.pending, 64)
	require.False(t, notified)
}

func TestWaitForHistoryCompletion(t *testing.T) {
	t.Run("asynchronous caller returns immediately", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		require.NoError(t, waitForHistoryCompletion(ctx, make(chan struct{}), false))
	})

	t.Run("synchronous caller waits for completion", func(t *testing.T) {
		finishCh := make(chan struct{})
		close(finishCh)

		require.NoError(t, waitForHistoryCompletion(context.Background(), finishCh, true))
	})

	t.Run("synchronous caller remains owned until cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- waitForHistoryCompletion(ctx, make(chan struct{}), true)
		}()
		cancel()

		require.ErrorIs(t, <-done, context.Canceled)
	})

	t.Run("cancellation wins when completion is also ready", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		finishCh := make(chan struct{})
		close(finishCh)

		require.ErrorIs(t, waitForHistoryCompletion(ctx, finishCh, true), context.Canceled)
	})
}
