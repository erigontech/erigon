// Copyright 2015 The go-ethereum Authors
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

//go:build integration

package tests

import (
	"runtime"
	"testing"

	"github.com/erigontech/erigon/erigon-lib/log/v3"
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

	// Currently it fails because SpawnStageHeaders doesn't accept any PoW blocks after PoS transition
	// TODO(yperbasis): make it work
	bt.skipLoad(`^TransitionTests/bcArrowGlacierToParis/powToPosBlockRejection\.json`)
	bt.skipLoad(`^TransitionTests/bcFrontierToHomestead/blockChainFrontierWithLargerTDvsHomesteadBlockchain\.json`)

	// TODO: HistoryV3: doesn't produce receipts on execution by design. But maybe we can Generate them on-the fly (on history) and enable this tests
	bt.skipLoad(`^InvalidBlocks/bcInvalidHeaderTest/log1_wrongBloom\.json`)
	bt.skipLoad(`^InvalidBlocks/bcInvalidHeaderTest/wrongReceiptTrie\.json`)
	bt.skipLoad(`^InvalidBlocks/bcInvalidHeaderTest/wrongGasUsed\.json`)

	checkStateRoot := true

	bt.walk(t, blockTestDir, func(t *testing.T, name string, test *BlockTest) {
		t.Parallel()
		// import pre accounts & construct test genesis block & state root
		if err := bt.checkFailure(t, test.Run(t, checkStateRoot)); err != nil {
			t.Error(err)
		}
	})
}
