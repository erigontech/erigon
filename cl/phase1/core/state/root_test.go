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

package state_test

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

// Curr: 4358340
func BenchmarkStateRootNonCached(b *testing.B) {
	for i := 0; i < b.N; i++ {
		base := state.New(&clparams.MainnetBeaconConfig)
		base.HashSSZ()
	}
}

// Prev: 1400
// Curr: 139.4
func BenchmarkStateRootCached(b *testing.B) {
	// Re-use same fields
	base := state.New(&clparams.MainnetBeaconConfig)
	for i := 0; i < b.N; i++ {
		base.HashSSZ()
	}
}
