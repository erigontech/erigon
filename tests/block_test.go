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

//go:build integration

package tests

import (
	"runtime"
	"testing"

	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/log/v3"
)

func TestBlockchain(t *testing.T) {
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please") // after remove ChainReader from consensus engine - this test can be changed to create less databases, then can enable on win. now timeout after 20min
	}

	bt := new(testMatcher)
	// General state tests are 'exported' as blockchain tests, but we can run them natively.
	// For speedier CI-runs those are skipped.
	bt.skipLoad(`^GeneralStateTests/`)

	// Skipping due to https://github.com/ethereum/tests/issues/1133
	bt.skipLoad(`^EIPTests/bc4895-withdrawals/`)

	// Currently it fails because SpawnStageHeaders doesn't accept any PoW blocks after PoS transition
	// TODO(yperbasis): make it work
	bt.skipLoad(`^TransitionTests/bcArrowGlacierToMerge/powToPosBlockRejection\.json`)
	if ethconfig.EnableHistoryV3InTest {
		// HistoryV3: doesn't produce receipts on execution by design
		bt.skipLoad(`^InvalidBlocks/bcInvalidHeaderTest/log1_wrongBloom\.json`)
		bt.skipLoad(`^InvalidBlocks/bcInvalidHeaderTest/wrongReceiptTrie\.json`)
		bt.skipLoad(`^InvalidBlocks/bcInvalidHeaderTest/wrongGasUsed\.json`)
	}

	bt.walk(t, blockTestDir, func(t *testing.T, name string, test *BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.checkFailure(t, test.Run(t, false)); err != nil {
			t.Error(err)
		}
	})
}
