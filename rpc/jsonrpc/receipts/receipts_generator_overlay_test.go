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

package receipts_test

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/ethutils"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// TestGetReceiptLogIndexThroughOverlay pins the wiring that lets GetReceipt see a
// block whose commit is in flight: the log index must be resolved through
// Filters.WithTemporalOverlay, not read from the committed tx. The record the
// second transaction reads is seeded in the overlay with a value the committed
// tx does not hold, so only a routed read can produce it.
func TestGetReceiptLogIndexThroughOverlay(t *testing.T) {
	signer := types.LatestSignerForChainID(nil)
	m := mockWithGenerator(t, 2, func(i int, block *blockgen.BlockGen) {
		for range 2 {
			txn, err := types.SignTx(
				types.NewTransaction(block.TxNonce(testAddr), testAddr, uint256.NewInt(1), params.TxGas, nil, nil),
				*signer, testKey,
			)
			require.NoError(t, err)
			block.AddTx(txn)
		}
	})

	tx, err := m.DB.BeginTemporalRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const blockNum = uint64(2)
	block, err := m.BlockReader.BlockByNumber(m.Ctx, tx, blockNum)
	require.NoError(t, err)
	require.Len(t, block.Transactions(), 2)

	minTxNum, err := m.BlockReader.TxnumReader().Min(m.Ctx, tx, blockNum)
	require.NoError(t, err)
	firstTxNum := minTxNum + 1 // txIndex 0, past the block's system tx
	txNum := firstTxNum + 1    // txIndex 1, the transaction under test

	const overlayLogIdx = uint32(41)

	sd, err := execctx.NewSharedDomains(m.Ctx, tx, m.Log)
	require.NoError(t, err)
	defer sd.Close()
	require.NoError(t, sd.InitBlockOverlay(tx))
	require.NoError(t, rawtemporaldb.AppendReceiptMetadata(sd.AsPutDel(tx), overlayLogIdx, 0, 0, firstTxNum))

	events := shards.NewEvents()
	events.PublishOverlay(sd)
	ff := rpchelper.New(m.Ctx, rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, m.Log, events)

	gen := receipts.NewGenerator(m.Dirs, m.BlockReader, m.Engine, nil, time.Minute, ff)
	receipt, err := gen.GetReceipt(m.Ctx, m.ChainConfig, tx, block.HeaderNoCopy(), block.Transactions()[1], 1, txNum, nil)
	require.NoError(t, err)
	require.Equal(t, overlayLogIdx, receipt.FirstLogIndexWithinBlock,
		"GetReceipt must resolve the log index through the block overlay")
}

// TestGetReceiptSkipsBloomOfPersistedReceipt pins that a receipt served from the persistent cache
// keeps an empty Bloom: eth_getLogs reads only the logs, and ethutils.MarshalReceipt derives the
// bloom for the callers that return it.
func TestGetReceiptSkipsBloomOfPersistedReceipt(t *testing.T) {
	defer func(prev bool) { dbg.AssertEnabled = prev }(dbg.AssertEnabled)
	dbg.AssertEnabled = false // assertions re-execute instead of serving the persistent cache

	signer := types.LatestSignerForChainID(nil)
	logOnCreate := []byte{0x60, 0x00, 0x60, 0x00, 0xa0, 0x00} // PUSH1 0 PUSH1 0 LOG0 STOP
	m := mockWithGenerator(t, 1, func(i int, block *blockgen.BlockGen) {
		txn, err := types.SignTx(
			types.NewContractCreation(block.TxNonce(testAddr), uint256.NewInt(0), 100_000, uint256.NewInt(1), logOnCreate),
			*signer, testKey,
		)
		require.NoError(t, err)
		block.AddTx(txn)
	}, execmoduletester.WithEnableDomain(kv.RCacheDomain))

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const blockNum = uint64(1)
	block, err := m.BlockReader.BlockByNumber(m.Ctx, tx, blockNum)
	require.NoError(t, err)
	minTxNum, err := m.BlockReader.TxnumReader().Min(m.Ctx, tx, blockNum)
	require.NoError(t, err)
	txNum := minTxNum + 1 // txIndex 0, past the block's system tx
	header, txn := block.HeaderNoCopy(), block.Transactions()[0]

	gen := receipts.NewGenerator(m.Dirs, m.BlockReader, m.Engine, nil, time.Minute)
	receipt, err := gen.GetReceipt(m.Ctx, m.ChainConfig, tx, header, txn, 0, txNum, nil)
	require.NoError(t, err)
	require.Len(t, receipt.Logs, 1)
	require.True(t, receipt.Bloom.IsEmpty(), "a receipt served from the persistent cache must not derive its bloom")

	served := ethutils.MarshalReceipt(receipt, txn, m.ChainConfig, header, true, true)
	bloom := types.CreateBloom(types.Receipts{receipt})
	require.Equal(t, &bloom, served.LogsBloom)
}

// TestGetReceiptReplayOfStorageWriteAndLog pins a replayed receipt of a contract call that writes
// storage and emits a log. A fresh generator and no persistent receipt cache force the replay.
func TestGetReceiptReplayOfStorageWriteAndLog(t *testing.T) {
	signer := types.LatestSignerForChainID(nil)
	runtime := []byte{0x60, 0x01, 0x60, 0x00, 0x55, 0x60, 0x00, 0x60, 0x00, 0xa0, 0x00} // SSTORE(0, 1) LOG0 STOP
	initCode := append([]byte{0x60, byte(len(runtime)), 0x60, 0x0c, 0x60, 0x00, 0x39, 0x60, byte(len(runtime)), 0x60, 0x00, 0xf3}, runtime...)
	contract := types.CreateAddress(testAddr, 0)
	m := mockWithGenerator(t, 2, func(i int, block *blockgen.BlockGen) {
		var txn types.Transaction
		if i == 0 {
			txn = types.NewContractCreation(block.TxNonce(testAddr), uint256.NewInt(0), 100_000, uint256.NewInt(1), initCode)
		} else {
			txn = types.NewTransaction(block.TxNonce(testAddr), contract, uint256.NewInt(0), 100_000, uint256.NewInt(1), nil)
		}
		signed, err := types.SignTx(txn, *signer, testKey)
		require.NoError(t, err)
		block.AddTx(signed)
	})

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const blockNum = uint64(2)
	block, err := m.BlockReader.BlockByNumber(m.Ctx, tx, blockNum)
	require.NoError(t, err)
	minTxNum, err := m.BlockReader.TxnumReader().Min(m.Ctx, tx, blockNum)
	require.NoError(t, err)
	txNum := minTxNum + 1 // txIndex 0, past the block's system tx
	header, txn := block.HeaderNoCopy(), block.Transactions()[0]

	gen := receipts.NewGenerator(m.Dirs, m.BlockReader, m.Engine, nil, time.Minute)
	receipt, err := gen.GetReceipt(m.Ctx, m.ChainConfig, tx, header, txn, 0, txNum, nil)
	require.NoError(t, err)
	require.Equal(t, types.ReceiptStatusSuccessful, receipt.Status)
	require.Len(t, receipt.Logs, 1)
	require.Equal(t, contract, receipt.Logs[0].Address)
}

// TestPreByzantiumPostStateFromCommitmentHistory pins the per-tx root that the
// commitment-history replay writes into a pre-Byzantium receipt against the root
// rebuilt from state history. Later txs of the block read the earlier txs' writes, and
// one destroys a contract whose storage predates the block, so each root depends on the
// replay folding every key the block has changed so far.
func TestPreByzantiumPostStateFromCommitmentHistory(t *testing.T) {
	for _, v3 := range []bool{false, true} {
		t.Run(fmt.Sprintf("v3=%t", v3), func(t *testing.T) {
			t.Cleanup(statecfg.SetCommitmentV3(v3))
			testPreByzantiumPostStateFromCommitmentHistory(t)
		})
	}
}

func testPreByzantiumPostStateFromCommitmentHistory(t *testing.T) {
	statecfg.EnableHistoricalCommitment()
	cfg := chain.TestChainBerlinConfig.Copy()
	cfg.ByzantiumBlock, cfg.ConstantinopleBlock, cfg.PetersburgBlock, cfg.IstanbulBlock, cfg.MuirGlacierBlock, cfg.BerlinBlock = nil, nil, nil, nil, nil, nil

	signer := types.LatestSignerForChainID(nil)
	// no calldata: SSTORE(0, 1) LOG0 STOP; with calldata: SELFDESTRUCT(CALLER)
	runtime := []byte{0x36, 0x60, 0x0f, 0x57, 0x60, 0x01, 0x60, 0x00, 0x55, 0x60, 0x00, 0x60, 0x00, 0xa0, 0x00, 0x5b, 0x33, 0xff}
	initCode := append([]byte{0x60, byte(len(runtime)), 0x60, 0x0c, 0x60, 0x00, 0x39, 0x60, byte(len(runtime)), 0x60, 0x00, 0xf3}, runtime...)
	preBlock, inBlock := types.CreateAddress(testAddr, 0), types.CreateAddress(testAddr, 3)
	m := mockWithGenerator(t, 2, func(i int, block *blockgen.BlockGen) {
		nonce := block.TxNonce(testAddr)
		txns := []types.Transaction{
			types.NewContractCreation(nonce, uint256.NewInt(0), 100_000, uint256.NewInt(1), initCode),
			types.NewTransaction(nonce+1, preBlock, uint256.NewInt(0), 100_000, uint256.NewInt(1), nil),
		}
		if i == 1 {
			txns = []types.Transaction{
				types.NewTransaction(nonce, common.Address{0x01}, uint256.NewInt(1), 21_000, uint256.NewInt(1), nil),
				types.NewContractCreation(nonce+1, uint256.NewInt(0), 100_000, uint256.NewInt(1), initCode),
				types.NewTransaction(nonce+2, inBlock, uint256.NewInt(0), 100_000, uint256.NewInt(1), nil),
				types.NewTransaction(nonce+3, preBlock, uint256.NewInt(0), 100_000, uint256.NewInt(1), []byte{0x01}),
				types.NewTransaction(nonce+4, common.Address{0x02}, uint256.NewInt(1), 21_000, uint256.NewInt(1), nil),
				types.NewTransaction(nonce+5, preBlock, uint256.NewInt(1), 21_000, uint256.NewInt(1), nil), // funds the destroyed contract again
			}
		}
		for _, txn := range txns {
			signed, err := types.SignTx(txn, *signer, testKey)
			require.NoError(t, err)
			block.AddTx(signed)
		}
	}, execmoduletester.WithGenesisSpec(&types.Genesis{Config: cfg, Alloc: types.GenesisAlloc{testAddr: {Balance: big.NewInt(1000000)}}}))

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const blockNum = uint64(2)
	block, err := m.BlockReader.BlockByNumber(m.Ctx, tx, blockNum)
	require.NoError(t, err)
	txns := block.Transactions()
	require.Len(t, txns, 6)
	minTxNum, err := m.BlockReader.TxnumReader().Min(m.Ctx, tx, blockNum)
	require.NoError(t, err)

	receipt := func(index int, commitmentHistory bool) *types.Receipt {
		gen := receipts.NewGenerator(m.Dirs, m.BlockReader, m.Engine, nil, time.Minute)
		r, err := gen.GetReceipt(m.Ctx, cfg, tx, block.HeaderNoCopy(), txns[index], index, minTxNum+1+uint64(index), &receipts.PostStateInfo{Txns: txns, CommitmentHistory: commitmentHistory})
		require.NoError(t, err)
		return r
	}
	gen := receipts.NewGenerator(m.Dirs, m.BlockReader, m.Engine, nil, time.Minute)
	blockReceipts, err := gen.GetReceipts(m.Ctx, cfg, tx, block, eth.ReceiptsOpts{CommitmentHistoryEnabled: true})
	require.NoError(t, err)
	for i := range txns {
		want := receipt(i, false)
		require.Equal(t, types.ReceiptStatusSuccessful, want.Status, "tx %d", i)
		require.Len(t, want.PostState, 32, "tx %d", i)
		require.Equal(t, want.PostState, receipt(i, true).PostState, "GetReceipt tx %d", i)
		require.Equal(t, want.PostState, blockReceipts[i].PostState, "GetReceipts tx %d", i)
	}
	require.Len(t, blockReceipts[2].Logs, 1)
	require.Empty(t, blockReceipts[3].Logs, "tx 3 takes the SELFDESTRUCT branch")
}
