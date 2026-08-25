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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/phase1/network"
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

func TestCompleteHistoryBackfillWithholdsNotificationWhenEnvelopeRecoveryExhausts(t *testing.T) {
	skipped := []network.SkippedFullBlock{{Root: [32]byte{1}}}
	attempts := 0
	notified := false

	completed := completeHistoryBackfill(
		context.Background(), skipped, 3, 0,
		func(context.Context, []network.SkippedFullBlock) []network.SkippedFullBlock {
			attempts++
			return skipped
		},
		func() { notified = true },
	)

	require.False(t, completed)
	require.Equal(t, 3, attempts)
	require.False(t, notified)
}

func TestCompleteHistoryBackfillNotifiesAfterPartialEnvelopeRecoveryCompletes(t *testing.T) {
	first := network.SkippedFullBlock{Root: [32]byte{1}}
	second := network.SkippedFullBlock{Root: [32]byte{2}}
	inputs := make([][][32]byte, 0, 2)
	notified := false

	completed := completeHistoryBackfill(
		context.Background(), []network.SkippedFullBlock{first, second}, 3, 0,
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
