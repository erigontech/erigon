// Copyright 2023 The go-ethereum Authors
// (original work)
// Copyright 2025 The Erigon Authors
// (modifications)
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

package vm

import (
	"maps"
	"slices"
	"testing"
)

func FuzzPrecompiledContracts(f *testing.F) {
	// Create list of addresses
	addrs := slices.Collect(maps.Keys(allPrecompiles))
	f.Fuzz(func(t *testing.T, addr uint8, input []byte) {
		a := addrs[int(addr)%len(addrs)]
		p := allPrecompiles[a]
		gas := p.RequiredGas(input)
		if gas > 10_000_000 {
			return
		}
		inWant := string(input)
		RunPrecompiledContract(p, input, gas, nil)
		if inHave := string(input); inWant != inHave {
			t.Errorf("Precompiled %v modified input data", a)
		}
	})
}
