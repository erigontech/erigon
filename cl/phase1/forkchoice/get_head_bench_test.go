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
	"math"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// Mainnet shape: ~815k active validators in a ~2.2M registry; one block carries ~1/32 of their votes.
const (
	benchRegistry = 2_200_000
	benchActive   = 815_000
	benchBalance  = 32_000_000_000
)

type headBenchVotes struct {
	f      *ForkChoiceStore
	rng    *rand.Rand
	active []int
	recent []common.Hash
	cs     *checkpointState
}

func newHeadBenchVotes(f *ForkChoiceStore, recent []common.Hash, message func(root common.Hash) LatestMessage) *headBenchVotes {
	v := &headBenchVotes{f: f, rng: rand.New(rand.NewSource(1)), recent: recent}
	v.cs = &checkpointState{
		beaconConfig:     f.beaconCfg,
		balances:         make([]uint64, benchRegistry),
		actives:          make([]byte, (benchRegistry+7)/8),
		slasheds:         make([]byte, (benchRegistry+7)/8),
		validatorSetSize: benchRegistry,
	}
	f.latestMessages = newLatestMessagesStore(benchRegistry)
	for i := range benchRegistry {
		if v.rng.Intn(benchRegistry) >= benchActive {
			continue
		}
		v.cs.balances[i] = benchBalance
		writeToBitset(v.cs.actives, i, true)
		v.active = append(v.active, i)
		f.setLatestMessage(uint64(i), message(v.recent[v.rng.Intn(len(v.recent))]))
	}
	v.cs.unslashedActiveBalance = uint64(len(v.active)) * benchBalance
	return v
}

func (v *headBenchVotes) recordBlockVotes(message func(root common.Hash) LatestMessage) {
	for range len(v.active) / 32 {
		vi := v.active[v.rng.Intn(len(v.active))]
		v.f.setLatestMessage(uint64(vi), message(v.recent[v.rng.Intn(len(v.recent))]))
	}
}

func runHeadBench(b *testing.B, getHead func(b *testing.B), recordBlock func()) {
	getHead(b)
	b.Run("record-block-votes+get_head", func(b *testing.B) {
		for b.Loop() {
			recordBlock()
			getHead(b)
		}
	})
	b.Run("get_head-after-block", func(b *testing.B) {
		for b.Loop() {
			b.StopTimer()
			recordBlock()
			b.StartTimer()
			getHead(b)
		}
	})
}

func BenchmarkGetHeadPreGloas(b *testing.B) {
	f, graph := newHeadSnapshotTestStore()
	cfg := *f.beaconCfg
	cfg.GloasForkEpoch = math.MaxUint64
	cfg.InitializeForkSchedule()
	f.beaconCfg = &cfg
	f.headSet = map[common.Hash]struct{}{}
	justified := f.JustifiedCheckpoint()
	justified.Epoch = 10
	graph.headers[justified.Root].Slot = 320
	f.justifiedCheckpoint.Store(justified)
	f.unrealizedJustifications.Store(justified.Root, justified)
	parent := justified.Root
	var roots []common.Hash
	for i := range 96 {
		root := common.Hash{0x01, byte(i)}
		graph.headers[root] = &cltypes.BeaconBlockHeader{Slot: uint64(321 + i), ParentRoot: parent}
		f.updateChildren(graph.headers[parent].Slot, parent, root)
		f.unrealizedJustifications.Store(root, justified)
		roots = append(roots, root)
		parent = root
	}
	f.headSet[parent] = struct{}{}
	f.timeMs.Store((graph.headers[parent].Slot + 1) * cfg.SecondsPerSlot * 1000)
	f.proposerBoostRoot.Store(parent)

	epoch := uint64(11)
	message := func(root common.Hash) LatestMessage { return LatestMessage{Epoch: epoch, Root: root} }
	votes := newHeadBenchVotes(f, roots[len(roots)-32:], message)
	f.checkpointStates.Store(justified, votes.cs)
	runHeadBench(b, func(b *testing.B) {
		f.headHash = common.Hash{}
		if _, _, err := f.getHead(nil, false); err != nil {
			b.Fatal(err)
		}
	}, func() {
		epoch++
		votes.recordBlockVotes(message)
	})
}

func BenchmarkGetHeadGloas(b *testing.B) {
	v := newViabilityTree(b)
	f := v.store
	parent := v.justified.Root
	var roots []common.Hash
	for i := range 96 {
		root := common.Hash{0x03, byte(i)}
		v.add(root, parent, uint64(33+i), true, true)
		roots = append(roots, root)
		parent = root
	}
	slot := uint64(33 + 95)
	f.timeMs.Store((slot + 1) * f.beaconCfg.SecondsPerSlot * 1000)

	message := func(root common.Hash) LatestMessage { return LatestMessage{Slot: slot, Root: root} }
	votes := newHeadBenchVotes(f, roots[len(roots)-32:], message)
	f.checkpointStates.Store(v.justified, votes.cs)
	runHeadBench(b, func(b *testing.B) {
		f.headHash = common.Hash{}
		if _, _, err := f.getHeadGloas(false); err != nil {
			b.Fatal(err)
		}
	}, func() {
		slot++
		votes.recordBlockVotes(message)
	})
}
