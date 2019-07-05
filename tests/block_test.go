// Copyright 2015 The go-ethereum Authors
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

package tests

import (
	"testing"
)

func TestBlockchain(t *testing.T) {
	t.Parallel()

	bt := new(testMatcher)
	// General state tests are 'exported' as blockchain tests, but we can run them natively.
	bt.skipLoad(`^GeneralStateTests/`)
	// Skip random failures due to selfish mining test.
	bt.skipLoad(`^bcForgedTest/bcForkUncle\.json`)
	bt.skipLoad(`^bcMultiChainTest/(ChainAtoChainB_blockorder|CallContractFromNotBestBlock)`)
	bt.skipLoad(`^bcTotalDifficultyTest/(lotsOfLeafs|lotsOfBranches|sideChainWithMoreTransactions)`)
	// Slow tests
	bt.slow(`^bcExploitTest/DelegateCallSpam.json`)
	bt.slow(`^bcExploitTest/ShanghaiLove.json`)
	bt.slow(`^bcExploitTest/SuicideIssue.json`)
	bt.slow(`^bcForkStressTest/`)
	bt.slow(`^bcGasPricerTest/RPC_API_Test.json`)
	bt.slow(`^bcWalletTest/`)

	// Work in progress
	bt.fails(`^bcStateTests/suicideStorageCheckVCreate2.json/suicideStorageCheckVCreate2_Constantinople`, "Work in progress")
	bt.fails(`^bcStateTests/suicideStorageCheck.json/suicideStorageCheck_Byzantium`, "Work in progress")
	bt.fails(`^bcStateTests/suicideStorageCheck.json/suicideStorageCheck_Constantinople`, "Work in progress")
	bt.fails(`^bcStateTests/suicideStorageCheckVCreate.json/suicideStorageCheckVCreate_Byzantium`, "Work in progress")
	bt.fails(`^bcStateTests/suicideStorageCheckVCreate.json/suicideStorageCheckVCreate_Constantinople`, "Work in progress")
	bt.fails(`^bcBlockGasLimitTest/BlockGasLimit2p63m1.json/BlockGasLimit2p63m1_Byzantium`, "Work in progress")
	bt.fails(`^bcBlockGasLimitTest/BlockGasLimit2p63m1.json/BlockGasLimit2p63m1_Constantinople`, "Work in progress")
	bt.fails(`^bcBlockGasLimitTest/BlockGasLimit2p63m1.json/BlockGasLimit2p63m1_EIP158`, "Work in progress")
	bt.fails(`^TransitionTests/bcFrontierToHomestead/blockChainFrontierWithLargerTDvsHomesteadBlockchain2.json`, "Work in progress")
	bt.fails(`^TransitionTests/bcFrontierToHomestead/blockChainFrontierWithLargerTDvsHomesteadBlockchain.json`, "Work in progress")
	bt.fails(`^bcBlockGasLimitTest/TransactionGasHigherThanLimit2p63m1_2.json/TransactionGasHigherThanLimit2p63m1_2_Byzantium`, "Work in progress")
	bt.fails(`^bcBlockGasLimitTest/TransactionGasHigherThanLimit2p63m1_2.json/TransactionGasHigherThanLimit2p63m1_2_Constantinople`, "Work in progress")
	bt.fails(`^bcBlockGasLimitTest/TransactionGasHigherThanLimit2p63m1_2.json/TransactionGasHigherThanLimit2p63m1_2_EIP158`, "Work in progress")
	bt.fails(`^bcBlockGasLimitTest/TransactionGasHigherThanLimit2p63m1.json/TransactionGasHigherThanLimit2p63m1_Byzantium`, "Work in progress")
	bt.fails(`^bcBlockGasLimitTest/TransactionGasHigherThanLimit2p63m1.json/TransactionGasHigherThanLimit2p63m1_Constantinople`, "Work in progress")
	bt.fails(`^bcBlockGasLimitTest/TransactionGasHigherThanLimit2p63m1.json/TransactionGasHigherThanLimit2p63m1_EIP158`, "Work in progress")

	bt.walk(t, blockTestDir, func(t *testing.T, name string, test *BlockTest) {
		if err := bt.checkFailure(t, name, test.Run()); err != nil {
			t.Error(err)
		}
	})
}
