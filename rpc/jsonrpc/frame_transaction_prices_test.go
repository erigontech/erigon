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
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/ethutils"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
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
		require.Equal(t, want, (*uint256.Int)(&got.GasPrice))
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

func TestFrameTransactionRPCNonCanonicalGasPrice(t *testing.T) {
	m, block, _ := framePriceTestChain(t)
	header := block.Header()
	header.Coinbase = common.Address{42}
	side := types.NewBlockFromStorage(header.Hash(), header, block.Transactions(), nil, nil, nil)
	require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		return rawdb.WriteHeader(tx, header)
	}))
	base := newBaseApiForTest(m)
	base.blocksLRU.Add(side.Hash(), side)
	api := newEthApiForTest(base, m.DB, nil, nil)
	got, err := api.GetBlockByHash(m.Ctx, rpc.BlockNumberOrHashWithHash(side.Hash(), false), true)
	require.NoError(t, err)
	require.NotNil(t, got)
	for _, txn := range got.Transactions.([]*ethapi.RPCTransaction) {
		require.Equal(t, block.BaseFee(), (*uint256.Int)(&txn.GasPrice), "an unexecuted fork must not use canonical receipts")
	}
}

func TestFrameTransactionRPCUnexecutedGasPrice(t *testing.T) {
	m, block, _ := framePriceTestChain(t)
	header := block.Header()
	header.Number.AddUint64(&header.Number, 1)
	block = types.NewBlockFromStorage(header.Hash(), header, block.Transactions(), nil, nil, nil)
	require.NoError(t, m.DB.Update(m.Ctx, func(tx kv.RwTx) error {
		if err := rawdb.WriteHeader(tx, header); err != nil {
			return err
		}
		return rawdb.WriteCanonicalHash(tx, block.Hash(), block.NumberU64())
	}))
	base := newBaseApiForTest(m)
	base.blocksLRU.Add(block.Hash(), block)
	api := newEthApiForTest(base, m.DB, nil, nil)
	t.Run("block", func(t *testing.T) {
		got, err := api.GetBlockByHash(m.Ctx, rpc.BlockNumberOrHashWithHash(block.Hash(), true), true)
		require.NoError(t, err)
		require.NotNil(t, got)
		for _, txn := range got.Transactions.([]*ethapi.RPCTransaction) {
			require.Equal(t, block.BaseFee(), (*uint256.Int)(&txn.GasPrice), "a canonical header does not imply execution has finished")
		}
	})
	t.Run("transaction", func(t *testing.T) {
		for i := range block.Transactions() {
			got, err := api.GetTransactionByBlockHashAndIndex(m.Ctx, block.Hash(), hexutil.Uint64(i))
			require.NoError(t, err)
			require.NotNil(t, got)
			require.Equal(t, block.BaseFee(), (*uint256.Int)(&got.GasPrice))
		}
	})
}
