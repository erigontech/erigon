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

package forkchoice

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/common"
)

func BenchmarkFastConfirmationMillionValidators(b *testing.B) {
	const count = 1_000_000
	balances := &fcrBalances{balances: make([]uint64, count), active: make([]bool, count), slashed: make([]bool, count), total: count * 32_000_000_000}
	roots := make([]common.Hash, 64)
	for i := range roots {
		binary.LittleEndian.PutUint64(roots[i][:], uint64(i+1))
	}
	justified := solid.Checkpoint{Root: roots[32], Epoch: 1}
	view := &fcrView{slot: 64, slotsPerEpoch: 32, balanceIncrement: 1_000_000_000, proposerBoost: 40, head: roots[63], headBalances: balances, finalized: solid.Checkpoint{Root: roots[0]}, unrealized: justified, blocks: make(map[common.Hash]fcrBlock), committees: make(map[uint64][]uint64), votes: make([]fcrVote, count)}
	for i := range roots {
		block := fcrBlock{slot: uint64(i), valid: true, unrealized: justified, votingSource: justified}
		if i > 0 {
			block.parent = roots[i-1]
		}
		view.blocks[roots[i]] = block
	}
	for i := range count {
		balances.balances[i] = 32_000_000_000
		balances.active[i] = true
		view.votes[i] = fcrVote{root: roots[63], epoch: 1}
		slot := uint64(i%32) + 32
		view.committees[slot] = append(view.committees[slot], uint64(i))
	}
	rule := newFastConfirmation(view.finalized)
	rule.confirmedRoot = roots[32]
	rule.currentObserved = justified
	rule.previousSlotHead = roots[63]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := rule.latestConfirmed(view, balances, balances); got != roots[63] {
			b.Fatalf("confirmed %x", got)
		}
	}
}

type fcrBenchmarkGraph struct {
	fork_graph.ForkGraph
	startWriter func()
}

func (g *fcrBenchmarkGraph) GetHeader(root common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	if g.startWriter != nil {
		start := g.startWriter
		g.startWriter = nil
		start()
	}
	return g.ForkGraph.GetHeader(root)
}

func BenchmarkFastConfirmationSnapshotMillionValidators(b *testing.B) {
	store, _ := newFastConfirmationBenchmarkStore(b, true)
	graph := &fcrBenchmarkGraph{ForkGraph: store.forkGraph}
	store.forkGraph = graph
	var writerWait time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		started, done := make(chan struct{}), make(chan time.Duration, 1)
		graph.startWriter = func() {
			go func() {
				start := time.Now()
				close(started)
				store.mu.Lock()
				done <- time.Since(start)
				store.mu.Unlock()
			}()
			<-started
		}
		view, err := store.fastConfirmationView(store.Slot())
		require.NoError(b, err)
		require.Len(b, view.votes, 1_000_000)
		writerWait += <-done
	}
	b.ReportMetric(float64(writerWait.Nanoseconds())/float64(b.N), "writer-wait-ns/op")
}

func BenchmarkOnFastConfirmationMillionValidators(b *testing.B) {
	for _, epochStart := range []bool{false, true} {
		name := "within_epoch"
		if epochStart {
			name = "epoch_start"
		}
		b.Run(name, func(b *testing.B) {
			store, now := newFastConfirmationBenchmarkStore(b, epochStart)
			tracker := store.fastConfirmation
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tracker.attempted = false
				tracker.rule = newFastConfirmation(store.FinalizedCheckpoint())
				require.NoError(b, store.OnFastConfirmation(b.Context(), now, nil))
				require.NotNil(b, tracker.result.Load())
				require.NotNil(b, tracker.balances, "FCR must finish without a deadline reset")
			}
		})
	}
}

func newFastConfirmationBenchmarkStore(b *testing.B, epochStart bool) (*ForkChoiceStore, uint64) {
	b.Helper()
	_, anchor, _ := tests.GetBellatrixRandom()
	cfg := anchor.BeaconConfig()
	for i := anchor.ValidatorLength(); i < 1_000_000; i++ {
		var pubkey [48]byte
		binary.LittleEndian.PutUint64(pubkey[:], uint64(i))
		validator := solid.NewValidatorFromParameters(pubkey, common.Hash{}, cfg.MaxEffectiveBalance, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch)
		require.NoError(b, anchor.AddValidator(validator, cfg.MaxEffectiveBalance))
		anchor.AddCurrentEpochParticipationFlags(0)
		anchor.AddPreviousEpochParticipationFlags(0)
		anchor.AddInactivityScore(0)
	}
	slot := (anchor.Slot()/cfg.SlotsPerEpoch + 3) * cfg.SlotsPerEpoch
	if !epochStart {
		slot++
	}
	require.NoError(b, anchor.SetSlot(slot-1))
	header := anchor.LatestBlockHeader()
	header.Slot, header.Root = slot-1, common.Hash{}
	anchor.SetLatestBlockHeader(&header)
	store := newOverlapTestStore(b, anchor, nil, false).store
	store.EnableFastConfirmation(store.FinalizedCheckpoint())
	root := store.AnchorRoot()
	for i := range anchor.ValidatorLength() {
		store.latestMessages.set(i, LatestMessage{Root: root, Epoch: (slot - 1) / cfg.SlotsPerEpoch, Slot: slot - 1})
	}
	now := (anchor.GenesisTime() + slot*cfg.SecondsPerSlot) * 1000
	store.OnTick(now)
	// Warm the head calculation; writers contend with the snapshot's read lock.
	_, _, err := store.GetHeadNode()
	require.NoError(b, err)
	return store, now
}
