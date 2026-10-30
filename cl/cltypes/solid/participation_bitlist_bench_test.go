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

package solid

import (
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
)

// BenchmarkParticipationBitListHash rehashes a mainnet-sized participation list after the flag updates of one block.
func BenchmarkParticipationBitListHash(b *testing.B) {
	const validators, attestersPerBlock = 2_100_001, 25_000
	bitList := NewParticipationBitList(validators, int(clparams.MainnetBeaconConfig.ValidatorRegistryLimit))
	rng := rand.New(rand.NewSource(1))
	if _, err := bitList.HashSSZ(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		for range attestersPerBlock {
			bitList.Set(rng.Intn(validators), byte(rng.Intn(8)))
		}
		b.StartTimer()
		if _, err := bitList.HashSSZ(); err != nil {
			b.Fatal(err)
		}
	}
}
