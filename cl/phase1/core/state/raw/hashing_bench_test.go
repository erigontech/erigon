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

package raw

import (
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

func newStateWithQueues(version clparams.StateVersion) *BeaconState {
	s := newTestState(version)
	addQueues(s, version)
	return s
}

func addQueues(s *BeaconState, version clparams.StateVersion) {
	for i := range 7600 {
		s.AppendPendingDeposit(&solid.PendingDeposit{Amount: uint64(i), Slot: uint64(i)})
	}
	for i := range 100 {
		s.AppendPendingPartialWithdrawal(&solid.PendingPartialWithdrawal{ValidatorIndex: uint64(i), Amount: uint64(i)})
	}
	for i := range 50 {
		s.AppendPendingConsolidation(&solid.PendingConsolidation{SourceIndex: uint64(i), TargetIndex: uint64(i + 1)})
	}
	if version >= clparams.GloasVersion {
		for i := range 64 {
			s.builders.Append(&cltypes.Builder{Balance: uint64(i)})
			s.builderPendingWithdrawals.Append(&cltypes.BuilderPendingWithdrawal{BuilderIndex: uint64(i), Amount: uint64(i)})
		}
		for i := range 16 {
			s.payloadExpectedWithdrawals.Append(&cltypes.Withdrawal{Index: uint64(i), Amount: uint64(i)})
		}
	}
}

func BenchmarkHashSSZAfterCopyInto(b *testing.B) {
	for _, tc := range []struct {
		name       string
		version    clparams.StateVersion
		validators int
	}{
		{"fulu", clparams.FuluVersion, 0},
		{"gloas", clparams.GloasVersion, 0},
		{"fulu_2400k", clparams.FuluVersion, 2_400_000},
		{"gloas_2400k", clparams.GloasVersion, 2_400_000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var src *BeaconState
			if tc.validators == 0 {
				src = newStateWithQueues(tc.version)
			} else {
				src = newMainnetSizedState(b, tc.validators, tc.version)
				addQueues(src, tc.version)
			}
			if _, err := src.HashSSZ(); err != nil {
				b.Fatal(err)
			}
			dst := New(src.BeaconConfig())
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := src.CopyInto(dst); err != nil {
					b.Fatal(err)
				}
				if _, err := dst.HashSSZ(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func newMainnetSizedState(b *testing.B, validators int, version clparams.StateVersion) *BeaconState {
	b.Helper()
	s := newTestState(version)
	for i := range validators {
		var publicKey [48]byte
		binary.LittleEndian.PutUint32(publicKey[:], uint32(i))
		validator := solid.NewValidatorFromParameters(publicKey, [32]byte{1}, 32e9, false, 0, 0, math.MaxUint64, math.MaxUint64)
		if err := s.AddValidator(validator, 32e9+uint64(i)); err != nil {
			b.Fatal(err)
		}
		s.AddCurrentEpochParticipationFlags(0)
		s.AddPreviousEpochParticipationFlags(7)
		s.AddInactivityScore(0)
	}
	if _, err := s.HashSSZ(); err != nil {
		b.Fatal(err)
	}
	return s
}

// BenchmarkHashSSZAfterBlock hashes a mainnet-sized state after the field updates of one normal block.
func BenchmarkHashSSZAfterBlock(b *testing.B) {
	if testing.Short() {
		b.Skip("resource-heavy benchmark: builds a 2.4M-validator state")
	}
	const validators, attestersPerBlock, withdrawalsPerBlock = 2_400_000, 25_000, 16
	for _, tc := range []struct {
		name               string
		version            clparams.StateVersion
		previousEpochShare int // out of 4 attestations
	}{
		{"current_epoch_attestations", clparams.FuluVersion, 0},
		{"half_previous_epoch_attestations", clparams.FuluVersion, 2},
		{"gloas/current_epoch_attestations", clparams.GloasVersion, 0},
		{"gloas/half_previous_epoch_attestations", clparams.GloasVersion, 2},
	} {
		b.Run(tc.name, func(b *testing.B) {
			s := newMainnetSizedState(b, validators, tc.version)
			rng := rand.New(rand.NewSource(1))
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				slot := s.Slot() + 1
				if err := s.SetSlot(slot); err != nil {
					b.Fatal(err)
				}
				s.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: slot})
				if err := s.SetRandaoMixAt(int(slot%s.BeaconConfig().EpochsPerHistoricalVector), common.Hash{byte(slot)}); err != nil {
					b.Fatal(err)
				}
				for range withdrawalsPerBlock + 1 {
					if err := s.SetValidatorBalance(rng.Intn(validators), slot); err != nil {
						b.Fatal(err)
					}
				}
				for i := range attestersPerBlock {
					s.SetEpochParticipationForValidatorIndex(i%4 >= tc.previousEpochShare, rng.Intn(validators), 3)
				}
				b.StartTimer()
				if _, err := s.HashSSZ(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
