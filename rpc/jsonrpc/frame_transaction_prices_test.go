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
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/ethutils"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func framePriceTestChain(t *testing.T) (*execmoduletester.ExecModuleTester, *types.Block, types.Receipts) {
	t.Helper()
	cfg := chain.AllProtocolChanges.Copy()
	cfg.BogotaTime = common.NewUint64(0)
	sender, target := common.HexToAddress("0x1000"), common.HexToAddress("0x2000")
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(10),
		Alloc: types.GenesisAlloc{
			sender: {
				Balance: big.NewInt(1_000_000_000),
				Code:    program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(),
			},
			target: {Code: []byte{byte(vm.STOP)}},
		},
	}))
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		for i, tip := range []uint64{1, 3} {
			txn := &types.FrameTransaction{
				ChainID: *cfg.ChainID, Nonce: uint64(i), SenderAddress: sender,
				Frames: []types.Frame{
					{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
					{Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 100_000}},
				},
				Fees: types.FrameFees{TipCap: *uint256.NewInt(tip)},
			}
			txn.Fees.MaxFee.Mul(uint256.NewInt(txn.GetGasLimit()), b.GetHeader().BaseFee)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	return m, pack.Blocks[0], pack.Receipts[0]
}

func TestFrameTransactionRPCSettledGasPrice(t *testing.T) {
	m, block, receipts := framePriceTestChain(t)
	base := newBaseApiForTest(m)
	api := newEthApiForTest(base, m.DB, nil, nil)
	ots := NewOtterscanAPI(base, m.DB, 25)
	erigon := NewErigonAPI(base, m.DB, nil)

	check := func(t *testing.T, got *ethapi.RPCTransaction, i int) {
		t.Helper()
		txn := block.Transactions()[i].(*types.FrameTransaction)
		receipt := ethutils.MarshalReceipt(receipts[i], txn, m.ChainConfig, block.HeaderNoCopy(), false)
		want := new(uint256.Int).Add(block.BaseFee(), &txn.Fees.TipCap)
		require.Equal(t, want, (*uint256.Int)(receipt.EffectiveGasPrice))
		require.NotNil(t, got)
		require.Equal(t, txn.Hash(), got.Hash)
		require.Equal(t, want, (*uint256.Int)(got.GasPrice))
	}
	for name, lookup := range map[string]func(int) (*ethapi.RPCTransaction, error){
		"transaction hash": func(i int) (*ethapi.RPCTransaction, error) {
			return api.GetTransactionByHash(m.Ctx, block.Transactions()[i].Hash())
		},
		"block number and index": func(i int) (*ethapi.RPCTransaction, error) {
			return api.GetTransactionByBlockNumberAndIndex(m.Ctx, 1, hexutil.Uint(i))
		},
		"block hash and index": func(i int) (*ethapi.RPCTransaction, error) {
			return api.GetTransactionByBlockHashAndIndex(m.Ctx, block.Hash(), hexutil.Uint64(i))
		},
	} {
		t.Run(name, func(t *testing.T) {
			for i := range block.Transactions() {
				got, err := lookup(i)
				require.NoError(t, err)
				check(t, got, i)
			}
		})
	}
	for name, lookup := range map[string]func() (*ethapi.RPCBlock, error){
		"block number": func() (*ethapi.RPCBlock, error) {
			return api.GetBlockByNumber(m.Ctx, 1, true)
		},
		"block hash": func() (*ethapi.RPCBlock, error) {
			return api.GetBlockByHash(m.Ctx, rpc.BlockNumberOrHashWithHash(block.Hash(), true), true)
		},
		"block timestamp": func() (*ethapi.RPCBlock, error) {
			return erigon.GetBlockByTimestamp(m.Ctx, rpc.Timestamp(block.Time()), true)
		},
		"otterscan": func() (*ethapi.RPCBlock, error) {
			got, err := ots.GetBlockTransactions(m.Ctx, 1, 0, 25)
			if err != nil {
				return nil, err
			}
			return got["fullblock"].(*ethapi.RPCBlock), nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := lookup()
			require.NoError(t, err)
			require.NotNil(t, got)
			txns := got.Transactions.([]*ethapi.RPCTransaction)
			require.Len(t, txns, len(block.Transactions()))
			for i, txn := range txns {
				check(t, txn, i)
			}
		})
	}
}

func TestPendingBlockUsesEstimatedFrameGasPrice(t *testing.T) {
	m, block, _ := framePriceTestChain(t)
	// The pending-block cache can still hold a payload after it becomes canonical.
	filters := rpchelper.New(m.Ctx, rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, m.Log, nil)
	encoded, err := rlp.EncodeToBytes(block)
	require.NoError(t, err)
	filters.HandlePendingBlock(&txpoolproto.OnPendingBlockReply{RplBlock: encoded})
	require.NotNil(t, filters.LastPendingBlock())

	api := newEthApiForTest(newBaseApiWithFiltersForTest(filters, m.StateCache, m), m.DB, nil, nil)
	got, err := api.GetBlockByNumber(m.Ctx, rpc.PendingBlockNumber, true)
	require.NoError(t, err)
	require.NotNil(t, got)
	txns := got.Transactions.([]*ethapi.RPCTransaction)
	require.Len(t, txns, len(block.Transactions()))
	for _, txn := range txns {
		require.Equal(t, block.BaseFee(), (*uint256.Int)(txn.GasPrice))
	}
}

func TestFrameTransactionRPCNonCanonicalGasPrice(t *testing.T) {
	m, block, _ := framePriceTestChain(t)
	header := block.Header()
	header.Coinbase = common.Address{42}
	side := types.NewBlockFromStorage(header.Hash(), header, block.Transactions(), nil, nil, nil)
	require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		return rawdb.WriteBlock(tx, side)
	}))
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	got, err := api.GetBlockByHash(m.Ctx, rpc.BlockNumberOrHashWithHash(side.Hash(), false), true)
	require.NoError(t, err)
	require.NotNil(t, got)
	txns := got.Transactions.([]*ethapi.RPCTransaction)
	require.Len(t, txns, len(side.Transactions()))
	for _, txn := range txns {
		require.Equal(t, block.BaseFee(), (*uint256.Int)(txn.GasPrice), "an unexecuted fork must not use canonical receipts")
	}
}

func TestFrameTransactionRPCUnexecutedGasPrice(t *testing.T) {
	m, block, _ := framePriceTestChain(t)
	header := block.Header()
	header.ParentHash = block.Hash()
	header.Number.AddUint64(&header.Number, 1)
	block = types.NewBlockFromStorage(header.Hash(), header, block.Transactions(), nil, nil, nil)
	require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		if err := rawdb.WriteBlock(tx, block); err != nil {
			return err
		}
		rawdb.WriteForkchoiceHead(tx, block.Hash())
		return rawdb.WriteCanonicalHash(tx, block.Hash(), block.NumberU64())
	}))
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	t.Run("block", func(t *testing.T) {
		got, err := api.GetBlockByHash(m.Ctx, rpc.BlockNumberOrHashWithHash(block.Hash(), true), true)
		require.NoError(t, err)
		require.NotNil(t, got)
		txns := got.Transactions.([]*ethapi.RPCTransaction)
		require.Len(t, txns, len(block.Transactions()))
		for _, txn := range txns {
			require.Equal(t, block.BaseFee(), (*uint256.Int)(txn.GasPrice), "a canonical header does not imply execution has finished")
		}
	})
	t.Run("transaction", func(t *testing.T) {
		for i := range block.Transactions() {
			got, err := api.GetTransactionByBlockHashAndIndex(m.Ctx, block.Hash(), hexutil.Uint64(i))
			require.NoError(t, err)
			require.NotNil(t, got)
			require.Equal(t, block.BaseFee(), (*uint256.Int)(got.GasPrice))
		}
	})
	t.Run("fee history", func(t *testing.T) {
		headers, err := api.FeeHistory(m.Ctx, 2, rpc.LatestBlockNumber, nil)
		require.NoError(t, err)
		require.Len(t, headers.GasUsedRatio, 2)

		withRewards, err := api.FeeHistory(m.Ctx, 2, rpc.LatestBlockNumber, []float64{50})
		require.NoError(t, err)
		require.Equal(t, uint64(1), withRewards.OldestBlock.ToInt().Uint64())
		require.Len(t, withRewards.Reward, 1, "only the executed parent has receipts")
	})
}

func TestFrameTransactionRPCPrunedGasPrice(t *testing.T) {
	m, block, _ := framePriceTestChain(t)
	require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		if err := tx.ClearTable(kv.TblReceiptHistoryVals); err != nil {
			return err
		}
		return tx.ClearTable(kv.TblRCacheHistoryVals)
	}))
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	got, err := api.GetBlockByHash(m.Ctx, rpc.BlockNumberOrHashWithHash(block.Hash(), true), true)
	require.NoError(t, err)
	for i, txn := range got.Transactions.([]*ethapi.RPCTransaction) {
		require.Equal(t, block.BaseFee(), (*uint256.Int)(txn.GasPrice))
		single, err := api.GetTransactionByHash(m.Ctx, block.Transactions()[i].Hash())
		require.NoError(t, err)
		require.Equal(t, txn.GasPrice, single.GasPrice)
	}
}

func TestBlockRPCFrameBlobFee(t *testing.T) {
	m, block, _ := framePriceTestChain(t)
	base := newBaseApiForTest(m)
	eth := newEthApiForTest(base, m.DB, nil, nil)
	debug := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})
	original := block.Transactions()[0].(*types.FrameTransaction)
	frame := &types.FrameTransaction{
		ChainID: original.ChainID, SenderAddress: original.SenderAddress, Frames: original.Frames, Fees: original.Fees,
		BlobVersionedHashes: []common.Hash{{1}},
	}
	// At one wei per blob gas, the budget covers both base fees exactly at the
	// declared gas limit. The estimated tip is zero. Using less gas can leave
	// room for a tip at settlement.
	frame.Fees.MaxFee.AddUint64(&frame.Fees.MaxFee, frame.GetBlobGas())
	writeBlock := func(t *testing.T, excessBlobGas uint64) common.Hash {
		t.Helper()
		header := block.Header()
		header.ExcessBlobGas = &excessBlobGas
		header.BlobGasUsed = common.NewUint64(frame.GetBlobGas())
		side := types.NewBlock(header, types.Transactions{frame}, nil, nil, block.Withdrawals(), nil)
		require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error { return rawdb.WriteBlock(tx, side) }))
		return side.Hash()
	}
	t.Run("full block with priced blob frame", func(t *testing.T) {
		hash := writeBlock(t, 0)
		got, err := eth.GetBlockByHash(m.Ctx, rpc.BlockNumberOrHashWithHash(hash, false), true)
		require.NoError(t, err)
		require.NotNil(t, got)
		txns := got.Transactions.([]*ethapi.RPCTransaction)
		require.Len(t, txns, 1)
		require.Equal(t, frame.Hash(), txns[0].Hash)
		require.Equal(t, block.BaseFee(), (*uint256.Int)(txns[0].GasPrice))
	})
	t.Run("pending frame with sidecar", func(t *testing.T) {
		tx, err := m.DB.BeginTemporalRo(m.Ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		fee, err := base.frameBlobBaseFee(m.Ctx, tx, block.HeaderNoCopy(), &types.FrameTxWrapper{FrameTransaction: frame})
		require.NoError(t, err)
		require.Equal(t, uint256.NewInt(1), fee)
	})
	hash := writeBlock(t, 1<<62)
	ref := rpc.BlockNumberOrHashWithHash(hash, false)
	t.Run("hashes", func(t *testing.T) {
		got, err := eth.GetBlockByHash(m.Ctx, ref, false)
		require.NoError(t, err)
		require.Equal(t, []common.Hash{frame.Hash()}, got.Transactions)
	})
	t.Run("full transactions", func(t *testing.T) {
		got, err := eth.GetBlockByHash(m.Ctx, ref, true)
		require.NoError(t, err)
		txn := got.Transactions.([]*ethapi.RPCTransaction)[0]
		require.Equal(t, frame.Hash(), txn.Hash)
		data, err := jsonstream.Marshal(txn)
		require.NoError(t, err)
		require.NotContains(t, string(data), `"gasPrice"`)
	})
	t.Run("bad blocks", func(t *testing.T) {
		require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
			if err := rawdb.WriteCanonicalHash(tx, hash, block.NumberU64()); err != nil {
				return err
			}
			return rawdb.TruncateCanonicalHash(tx, block.NumberU64(), true)
		}))
		require.NoError(t, m.DB.View(m.Ctx, func(tx kv.Tx) error { return rawdb.ResetBadBlockCache(tx, 100) }))
		got, err := debug.GetBadBlocks(m.Ctx)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, hash, got[0]["hash"])
		require.NotEmpty(t, got[0]["rlp"])
	})
}
