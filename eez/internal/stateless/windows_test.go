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
package stateless

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/types"
)

// verifyWindow verifies every block of a window and requires each to start
// from the state its predecessor ended in.
func verifyWindow(t *testing.T, w *window) []*Result {
	t.Helper()
	var results []*Result
	for i, block := range w.blocks {
		res, err := Verify(w.config, w.engine, block, w.witness(i))
		require.NoError(t, err)
		if i > 0 {
			require.Equal(t, w.blocks[i-1].Hash(), block.ParentHash())
			require.Equal(t, results[i-1].StateRoot, w.blocks[i-1].Root())
		}
		results = append(results, res)
	}
	return results
}

func oracleHash(t *testing.T, w *window, key string) common.Hash {
	t.Helper()
	h, err := w.OracleHash(key)
	require.NoError(t, err)
	return h
}

// TestCheckpoint2175 is T-WIT-2: the state root after each transaction
// equals the root the composer recorded for it.
func TestCheckpoint2175(t *testing.T) {
	w := loadWindow(t, "stateless-checkpoint-2175")
	block := w.blocks[0]
	require.Equal(t, oracleHash(t, w, "block_hash"), block.Hash())

	var want []common.Hash
	require.NoError(t, json.Unmarshal(w.Oracle["transaction_state_roots"], &want))
	require.Len(t, want, len(block.Transactions()))

	res, err := Verify(w.config, w.engine, block, w.witness(0), WithTxStateRoots())
	require.NoError(t, err)
	require.Equal(t, want, res.TxStateRoots)
	require.Equal(t, block.Root(), res.StateRoot)
}

// TestOutbound630 is T-WIT-3.
func TestOutbound630(t *testing.T) {
	w := loadWindow(t, "nonzero-outbound-630")
	results := verifyWindow(t, w)
	require.Equal(t, oracleHash(t, w, "final_state_root"), results[len(results)-1].StateRoot)

	// The recording's EEZL2 emits the five-argument
	// CrossChainCallExecuted(bytes32,address,address,bytes,uint256), which
	// must not pass for the six-argument one.
	fiveArg := common.HexToHash("0xad4275800b3b34f59dea39a4bd58ae171a852e3555a2ed1a6f9bad83eb6fb598")
	require.NotEqual(t, protocol.L2CrossChainCallExecutedTopic, fiveArg)
	var seen int
	for _, r := range results[len(results)-1].Receipts {
		for _, l := range r.Logs {
			if len(l.Topics) == 0 || l.Topics[0] != fiveArg {
				continue
			}
			seen++
			_, err := protocol.ParseL2CrossChainCallExecuted(l)
			require.Error(t, err)
		}
	}
	require.NotZero(t, seen)
}

// TestDevnetWindows is T-WIT-4 on the 855fe06 devnet recordings.
func TestDevnetWindows(t *testing.T) {
	for _, name := range []string{"captured-devnet-window-84", "captured-devnet-window-468", "captured-devnet-anchor-402"} {
		t.Run(name, func(t *testing.T) {
			w := loadWindow(t, name)
			verifyWindow(t, w)
			require.Equal(t, oracleHash(t, w, "window_pre_block_hash"), w.blocks[0].ParentHash())
			require.Equal(t, oracleHash(t, w, "window_post_block_hash"), w.blocks[len(w.blocks)-1].Hash())
		})
	}
}

// TestSystemTransactions468 checks the settling block of window 468: three
// type-0x76 transactions succeed fee-free under a fee collector.
func TestSystemTransactions468(t *testing.T) {
	w := loadWindow(t, "captured-devnet-window-468")
	require.NotNil(t, w.config.FeeCollector)
	results := verifyWindow(t, w)

	block, res := w.blocks[len(w.blocks)-1], results[len(results)-1]
	var system int
	for i, txn := range block.Transactions() {
		receipt := res.Receipts[i]
		require.Equal(t, txn.Type(), receipt.Type)
		require.Equal(t, types.ReceiptStatusSuccessful, receipt.Status, "transaction %d", i)
		if txn.Type() == protocol.SystemTxType {
			system++
		}
	}
	require.Equal(t, 3, system)
}
