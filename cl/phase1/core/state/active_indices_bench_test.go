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

package state_test

import (
	"encoding/binary"
	"slices"
	"sync"
	"testing"
	"unsafe"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/shuffling"
	"github.com/erigontech/erigon/common"
)

const (
	benchRegistrySize = 2_413_613
	benchActiveCount  = 766_000
	benchEpoch        = 10
)

var (
	benchStateOnce sync.Once
	benchState     *state.CachingBeaconState
	benchSink      any
	benchIntSink   int
)

func mainnetSizeState(tb testing.TB) *state.CachingBeaconState {
	benchStateOnce.Do(func() {
		s := state.New(&clparams.MainnetBeaconConfig)
		for i := range benchRegistrySize {
			var pk [48]byte
			binary.BigEndian.PutUint64(pk[:], uint64(i))
			v := solid.NewValidator()
			v.SetPublicKey(pk)
			v.SetEffectiveBalance(32_000_000_000)
			v.SetExitEpoch(^uint64(0))
			v.SetActivationEpoch(^uint64(0))
			if uint64(i)*benchActiveCount/benchRegistrySize != uint64(i+1)*benchActiveCount/benchRegistrySize {
				v.SetActivationEpoch(0)
			}
			if err := s.AddValidator(v, 32_000_000_000); err != nil {
				tb.Fatal(err)
			}
		}
		benchState = s
	})
	return benchState
}

func BenchmarkActiveIndicesScan(b *testing.B) {
	s := mainnetSizeState(b)
	epoch := uint64(benchEpoch)
	var list any
	var retained int
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		epoch++
		active := s.GetActiveValidatorsIndices(epoch)
		list = active
		retained = cap(active) * int(unsafe.Sizeof(active[0]))
	}
	benchSink = list
	b.ReportMetric(float64(retained), "retainedB/list")
}

func BenchmarkShuffleActiveSet(b *testing.B) {
	s := mainnetSizeState(b)
	active := s.GetActiveValidatorsIndices(benchEpoch)
	out := slices.Clone(active)
	cfg := &clparams.MainnetBeaconConfig
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out = shuffling.ComputeShuffledIndicies(cfg, common.Hash{}, out, active, benchEpoch*cfg.SlotsPerEpoch)
	}
	benchIntSink = len(out)
	b.ReportMetric(float64(len(out))*float64(unsafe.Sizeof(out[0])), "retainedB/list")
}

func BenchmarkBeaconCommitteesWarm(b *testing.B) {
	s := mainnetSizeState(b)
	cfg := s.BeaconConfig()
	firstSlot := uint64(benchEpoch) * cfg.SlotsPerEpoch
	committeesPerSlot := s.CommitteeCount(benchEpoch)
	sumEpoch := func() uint64 {
		var sum uint64
		for slot := firstSlot; slot < firstSlot+cfg.SlotsPerEpoch; slot++ {
			for index := range committeesPerSlot {
				committee, err := s.GetBeaconCommitee(slot, index)
				if err != nil {
					b.Fatal(err)
				}
				for _, member := range committee {
					sum += uint64(member)
				}
			}
		}
		return sum
	}
	sumEpoch()
	var total uint64
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		total += sumEpoch()
	}
	benchIntSink = int(total)
}

func BenchmarkBeaconProposerIndices(b *testing.B) {
	s := mainnetSizeState(b)
	for _, version := range []struct {
		name string
		v    clparams.StateVersion
	}{
		{"electra", clparams.ElectraVersion},
		{"gloas", clparams.GloasVersion},
	} {
		b.Run(version.name, func(b *testing.B) {
			s.SetVersion(version.v)
			if _, err := s.GetBeaconProposerIndices(benchEpoch); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				indices, err := s.GetBeaconProposerIndices(benchEpoch)
				if err != nil {
					b.Fatal(err)
				}
				benchSink = indices
			}
		})
	}
}

func BenchmarkNextSyncCommitteeIndices(b *testing.B) {
	s := mainnetSizeState(b)
	s.SetVersion(clparams.ElectraVersion)
	if _, err := state.GetNextSyncCommitteeIndices(s); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		indices, err := state.GetNextSyncCommitteeIndices(s)
		if err != nil {
			b.Fatal(err)
		}
		benchSink = indices
	}
}
