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

package cltypes

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
)

func BenchmarkGloasBodyHashSSZ(b *testing.B) {
	body := gloasBodyHashBenchmarkBody(b)

	b.Run("miss", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			body.rootCache.Store(nil)
			if _, err := body.HashSSZ(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("cached", func(b *testing.B) {
		if _, err := body.HashSSZ(); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			if _, err := body.HashSSZ(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func gloasBodyHashBenchmarkBody(tb testing.TB) *BeaconBody {
	tb.Helper()
	body := NewBeaconBody(&clparams.MainnetBeaconConfig, clparams.GloasVersion)

	aggregationBits := bytes.Repeat([]byte{0xff}, 35_600/8)
	aggregationBits = append(aggregationBits, 0x01)
	for i := range 8 {
		body.Attestations.Append(gloasBodyRootCacheTestAttestation(tb, byte(i+1), aggregationBits))
	}

	for i := range 4 {
		body.PayloadAttestations.Append(gloasBodyRootCacheTestPayloadAttestation(tb, byte(i+1), true))
	}
	return body
}
