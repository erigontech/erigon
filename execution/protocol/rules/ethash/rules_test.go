// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
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

package ethash

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/chain"
)

func TestCalcDifficultyAppliesMinimumAfterBomb(t *testing.T) {
	zero := uint64(0)
	for _, test := range []struct {
		name         string
		config       *chain.Config
		currentBlock uint64
	}{
		{"Frontier", &chain.Config{}, 900_000},
		{"Homestead", &chain.Config{HomesteadBlock: &zero}, 900_000},
		{"Byzantium", &chain.Config{HomesteadBlock: &zero, ByzantiumBlock: &zero}, 4_370_000},
	} {
		t.Run(test.name, func(t *testing.T) {
			difficulty := CalcDifficulty(
				test.config,
				1,
				1,
				*uint256.NewInt(1_000),
				test.currentBlock-1,
				empty.UncleHash,
			)
			if !difficulty.Eq(uint256.NewInt(minimumDifficulty)) {
				t.Fatalf("got %s, want %d", &difficulty, minimumDifficulty)
			}
		})
	}
}
