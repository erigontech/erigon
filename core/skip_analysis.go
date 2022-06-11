// Copyright 2020 The erigon Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"github.com/ledgerwatch/erigon/params"
)

// MainnetNotCheckedFrom is the first block number not yet checked for invalid jumps
const MainnetNotCheckedFrom uint64 = 14_909_200

// MainnetNotCheckedFrom is the first block number not yet checked for invalid jumps
const BSCNotCheckedFrom uint64 = 18_589_376

const BorMainnetNotCheckedFrom uint64 = 24_673_536

const RopstenNotCheckedFrom uint64 = 12_331_664

// SkipAnalysis function tells us whether we can skip performing jumpdest analysis
// for the historical blocks (on mainnet now but perhaps on the testsnets
// in the future), because we have verified that there were only a few blocks
// where codeBitmap was useful. Invalid jumps either did not occur, or were
// prevented simply by checking whether the jump destination has JUMPDEST opcode
// Mainnet transactions that use jumpdest analysis are:
// 0x88a1f2a9f048a21fd944b28ad9962f533ab5d3c40e17b1bc3f99ae999a4021b2 (block 6426432)
// 0x86e55d1818b5355424975de9633a57c40789ca08552297b726333a9433949c92 (block 6426298)
// 0x3666640316df11865abd1352f4c0b4c5126f8ac1d858ef2a0c6e744a4865bca2 (block 5800596)
// 0xcdb5bf0b4b51093e1c994f471921f88623c9d3e1b6aa2782049f53a0048f2b32 (block 11079912)
// 0x21ab7bf7245a87eae265124aaf180d91133377e47db2b1a4866493ec4b371150 (block 13119520)

func SkipAnalysis(config *params.ChainConfig, blockNumber uint64) bool {
	if config == params.MainnetChainConfig {
		if blockNumber >= MainnetNotCheckedFrom { // We have not checked beyond that block
			return false
		}
		if blockNumber == 6426298 || blockNumber == 6426432 || blockNumber == 5800596 || blockNumber == 11079912 || blockNumber == 13119520 {
			return false
		}
		return true
	} else if config == params.BSCChainConfig {
		return blockNumber < BSCNotCheckedFrom
	} else if config == params.BorMainnetChainConfig {
		return blockNumber < BorMainnetNotCheckedFrom
	} else if config == params.RopstenChainConfig {
		if blockNumber >= RopstenNotCheckedFrom {
			return false
		}
		if blockNumber == 2534105 || blockNumber == 2534116 || blockNumber == 3028887 || blockNumber == 3028940 || blockNumber == 3028956 ||
			blockNumber == 3450102 || blockNumber == 5294626 || blockNumber == 5752787 || blockNumber == 10801303 || blockNumber == 10925062 ||
			blockNumber == 11440683 || blockNumber == 11897655 || blockNumber == 11898288 || blockNumber == 12291199 {
			return false
		}
		return true
	}
	return false
}
