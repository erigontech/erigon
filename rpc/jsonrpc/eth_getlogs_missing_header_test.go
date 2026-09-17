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

package jsonrpc

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/filters"
)

const (
	// missingHeaderChainLen keeps the block with no header below the tip, which the range
	// gate resolves before the query runs.
	missingHeaderChainLen = 3
	missingHeaderBlock    = 2
	// The txNum iterator reports a block change only on the first txNum of a block, so a
	// single-transaction block hides a skip that covers one txNum instead of the block.
	missingHeaderLogsPerBlock = 2
)

// setupMissingHeaderChain builds a chain where every transaction emits one log, then drops
// the canonical marker of missingHeaderBlock so HeaderByNumber answers nil for it.
func setupMissingHeaderChain(t *testing.T) *execmoduletester.ExecModuleTester {
	t.Helper()

	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config: chain.TestChainBerlinConfig,
			Alloc:  types.GenesisAlloc{testAddr: {Balance: big.NewInt(1_000_000_000)}},
		}),
		execmoduletester.WithKey(testKey),
	)
	signer := types.LatestSignerForChainID(nil)
	topic := common.Hash{0x11}
	c, err := m.GenerateChain(missingHeaderChainLen, func(i int, block *blockgen.BlockGen) {
		for range missingHeaderLogsPerBlock {
			logOnCreate := append(append([]byte{0x7f}, topic[:]...), 0x60, 0x00, 0x60, 0x00, 0xa1, 0x00) // PUSH32 topic PUSH1 0 PUSH1 0 LOG1 STOP
			txn, err := types.SignTx(types.NewContractCreation(block.TxNonce(testAddr), uint256.NewInt(0), 100_000, uint256.NewInt(1), logOnCreate), *signer, testKey)
			require.NoError(t, err)
			block.AddTx(txn)
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(c))

	require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		return tx.Delete(kv.HeaderCanonical, hexutil.EncodeTs(missingHeaderBlock))
	}))

	return m
}

func fullRangeCriteria() filters.FilterCriteria {
	return filters.FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(missingHeaderChainLen)}
}

// TestGetLogsFailsOnAMissingHeader pins that a block whose header cannot be read stops the
// query. Skipping only the txNum that detected it leaves the rest of the block to run
// against the nil the failed load stored.
func TestGetLogsFailsOnAMissingHeader(t *testing.T) {
	t.Parallel()

	m := setupMissingHeaderChain(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	_, err := api.GetLogs(m.Ctx, fullRangeCriteria())
	require.Error(t, err)
	require.Contains(t, err.Error(), "header not found")
}

// TestGetLatestLogsFailsOnAMissingHeader pins the same for erigon_getLatestLogs, where the
// block hash, the timestamp and the executor's block context keep the previous block's
// values instead of failing.
func TestGetLatestLogsFailsOnAMissingHeader(t *testing.T) {
	t.Parallel()

	m := setupMissingHeaderChain(t)
	api := NewErigonAPI(newBaseApiForTest(m), m.DB, nil)

	_, err := api.GetLatestLogs(m.Ctx, fullRangeCriteria(), filters.LogFilterOptions{BlockCount: missingHeaderChainLen})
	require.Error(t, err)
	require.Contains(t, err.Error(), "header not found")
}
