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

package vm

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/chain"
)

func BenchmarkActivePrecompilesParallel(b *testing.B) {
	mainnet := &chain.Rules{ChainID: uint256.NewInt(1), IsOsaka: true}
	resolve := func(b *testing.B, rules *chain.Rules) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = ActivePrecompiles(rules)
			}
		})
	}

	b.Run("none_registered", func(b *testing.B) { resolve(b, mainnet) })

	chainID := uint256.NewInt(900904)
	RegisterPrecompiles(chainID, PrecompiledContracts{})
	defer unregisterPrecompiles(chainID)

	b.Run("registered_chain", func(b *testing.B) {
		resolve(b, &chain.Rules{ChainID: chainID, IsOsaka: true})
	})
}
