// Copyright 2020 The turbo-geth Authors
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
	"github.com/ledgerwatch/turbo-geth/params"
)

// This function tells us whether we can skip performing jumpdest analysis
// for the historical blocks (on mainnet now but perhaps on the testsnets
// in the future), because we have verified that there were only a few blocks
// where codeBitmap was useful. Invalid jumps either did not occur, or were
// prevented simply by checking whether the jump destination has JUMPDEST opcode
// Mainnet transactions that use jumpdest analysis are:
// 0x88a1f2a9f048a21fd944b28ad9962f533ab5d3c40e17b1bc3f99ae999a4021b2 (block 6426432)
// 0x86e55d1818b5355424975de9633a57c40789ca08552297b726333a9433949c92 (block 6426298)
// 0x3666640316df11865abd1352f4c0b4c5126f8ac1d858ef2a0c6e744a4865bca2 (block 5800596)

const MainnetNotCheckedFrom uint64 = 10920183

func SkipAnalysis(config *params.ChainConfig, blockNumber uint64) bool {
	if config != params.MainnetChainConfig {
		return false
	}
	if blockNumber >= MainnetNotCheckedFrom { // We have not checked beyond that block
		return false
	}
	if blockNumber == 6426298 || blockNumber == 6426432 || blockNumber == 5800596 {
		return false
	}
	return true
}
