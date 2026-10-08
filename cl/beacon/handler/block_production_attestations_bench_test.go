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

package handler

import (
	"testing"

	"github.com/erigontech/erigon/cl/cltypes/solid"
)

// mainnetLikeCandidates models one slot of mainnet votes: 64 committees of 425
// validators with mainnet-sized indices, split into overlapping layers of on-chain aggregates.
func mainnetLikeCandidates(layers int) []attestationCandidate {
	const committees, committeeSize = 64, 425
	candidates := make([]attestationCandidate, 0, layers)
	for layer := range layers {
		candidate := attestationCandidate{attestation: &solid.Attestation{Data: &solid.AttestationData{Slot: uint64(layer)}}, currentEpoch: true}
		for c := range committees {
			for i := range committeeSize {
				if (i+layer*37)%(layer+2) == 0 && layer > 0 {
					continue
				}
				candidate.attesters = append(candidate.attesters, uint64(2_000_000+c*committeeSize+i))
				candidate.baseRewards = append(candidate.baseRewards, 1_000_000)
				candidate.newFlags = append(candidate.newFlags, 0b111)
			}
		}
		candidates = append(candidates, candidate)
	}
	return candidates
}

func BenchmarkSelectAttestations(b *testing.B) {
	candidates := mainnetLikeCandidates(32)
	weights := []uint64{14, 26, 14}
	b.ResetTimer()
	for b.Loop() {
		selectAttestations(candidates, weights, 1, 8)
	}
}

// BenchmarkSelectAttestationsManyDisjoint models a pre-Electra pool: many single-committee
// candidates without overlap and a limit of 128.
func BenchmarkSelectAttestationsManyDisjoint(b *testing.B) {
	const candidatesCount, committeeSize = 2048, 450
	candidates := make([]attestationCandidate, 0, candidatesCount)
	for c := range candidatesCount {
		candidate := attestationCandidate{attestation: &solid.Attestation{Data: &solid.AttestationData{Slot: uint64(c)}}, currentEpoch: true}
		for i := range committeeSize {
			candidate.attesters = append(candidate.attesters, uint64(2_000_000+c*committeeSize+i))
			candidate.baseRewards = append(candidate.baseRewards, uint64(1_000_000+c))
			candidate.newFlags = append(candidate.newFlags, 0b111)
		}
		candidates = append(candidates, candidate)
	}
	weights := []uint64{14, 26, 14}
	b.ResetTimer()
	for b.Loop() {
		selectAttestations(candidates, weights, 1, 128)
	}
}
