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

package netutil

import (
	"net/netip"
	"testing"
	"time"

	"github.com/erigontech/erigon/common/mclock"
)

func BenchmarkIPTrackerPredict(b *testing.B) {
	var (
		clock mclock.Simulated
		it    = NewIPTracker(5*time.Minute, 10*time.Minute, 10)
	)
	it.clock = &clock
	for i := range 100000 {
		host := netip.AddrFrom16([16]byte{0x20, 0x01, 0x0d, 0xb8, byte(i >> 16), byte(i >> 8), byte(i), 1})
		it.AddStatement(host, netip.MustParseAddrPort("[2001:db9::1]:30303"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it.PredictEndpoint()
	}
}
