// Copyright 2020 The erigon Authors
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

package core

import (
	"sort"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
)

// SkipAnalysis function tells us whether we can skip performing jumpdest analysis
// for the historical blocks (on mainnet now but perhaps on the testsnets
// in the future), because we have verified that there were only a few blocks
// where codeBitmap was useful. Invalid jumps either did not occur, or were
// prevented simply by checking whether the jump destination has JUMPDEST opcode
// Mainnet transactions that use jumpdest analysis are:
// 0x3666640316df11865abd1352f4c0b4c5126f8ac1d858ef2a0c6e744a4865bca2 (block 5800596)
// 0x88a1f2a9f048a21fd944b28ad9962f533ab5d3c40e17b1bc3f99ae999a4021b2 (block 6426432)
// 0x86e55d1818b5355424975de9633a57c40789ca08552297b726333a9433949c92 (block 6426298)
// 0xcdb5bf0b4b51093e1c994f471921f88623c9d3e1b6aa2782049f53a0048f2b32 (block 11079912)
// 0x21ab7bf7245a87eae265124aaf180d91133377e47db2b1a4866493ec4b371150 (block 13119520)

var analysisBlocks = map[string][]uint64{
	networkname.Mainnet:    {5_800_596, 6_426_298, 6_426_432, 11_079_912, 13_119_520, 15_081_051},
	networkname.BorMainnet: {29_447_463},
}

func SkipAnalysis(config *chain.Config, blockNumber uint64) bool {
	blockNums, ok := analysisBlocks[config.ChainName]
	if !ok {
		return false
	}
	// blockNums is ordered, and the last element is the first block number which has not been checked
	p := sort.Search(len(blockNums), func(i int) bool {
		return blockNums[i] >= blockNumber
	})
	if p == len(blockNums) {
		// blockNum is beyond the last element, no optimisation
		return false
	}
	// If the blockNumber is in the list, no optimisation
	return blockNumber != blockNums[p]
}
