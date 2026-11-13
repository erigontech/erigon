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
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

func TestPostTxReplayPreservesFeeAccounting(t *testing.T) {
	cfg := chain.AllProtocolChanges.Copy()
	cfg.BogotaTime = common.NewUint64(0)
	sender, body, assertion := common.HexToAddress("0x1000"), common.HexToAddress("0x2000"), common.HexToAddress("0x3000")
	// With no value transfers, the payer's balance delta must equal the pre-charge.
	code := program.New().Push0().Push(0x14).Op(vm.TXTRACE).
		Push0().Push(sender).Push(3).Op(vm.TXDIFF).
		Push0().Push(sender).Push(2).Op(vm.TXDIFF, vm.SUB, vm.EQ)
	destination := code.Size() + 7
	code.Op(vm.PUSH2).Append([]byte{byte(destination >> 8), byte(destination)}).Op(vm.JUMPI).
		Push0().Push0().Op(vm.REVERT, vm.JUMPDEST, vm.STOP)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
		Alloc: types.GenesisAlloc{
			sender: {
				Balance: big.NewInt(1_000_000_000_000_000_000),
				Code:    program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(),
			},
			body:      {Code: program.New().Push0().Push0().Op(vm.LOG0, vm.STOP).Bytes()},
			assertion: {Code: code.Bytes()},
		},
	}))
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		for nonce := range uint64(2) {
			b.AddTx(&types.FrameTransaction{
				ChainID: *cfg.ChainID, NonceKeys: []uint256.Int{{}}, Nonce: nonce, SenderAddress: sender,
				Fees: types.FrameFees{FeeCap: uint256.Int{1}},
				Frames: []types.Frame{
					{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
					{Mode: types.FrameSender, Target: &body, Limits: mdgas.MdGas{Execution: 10_000}},
					{Mode: types.FramePostTx, Target: &assertion, Limits: mdgas.MdGas{Execution: 10_000}},
				},
			})
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	for _, receipt := range pack.Receipts[0] {
		require.EqualValues(t, types.ReceiptStatusSuccessful, receipt.FrameReceipts[2].Status)
		require.Len(t, receipt.Logs, 1)
	}

	api := newTraceApiForTest(m)
	number := rpc.BlockNumber(1)
	hash := pack.Blocks[0].Transactions()[0].Hash()
	for _, tc := range []struct {
		name string
		call func(bool) (any, error)
	}{
		{"transaction", func(bailout bool) (any, error) {
			return api.Transaction(m.Ctx, hash, &bailout, nil)
		}},
		{"block", func(bailout bool) (any, error) {
			return api.Block(m.Ctx, number, &bailout, nil)
		}},
		{"replay transaction", func(bailout bool) (any, error) {
			return api.ReplayTransaction(m.Ctx, hash, []string{TraceTypeTrace, TraceTypeStateDiff}, &bailout, nil)
		}},
		{"replay block", func(bailout bool) (any, error) {
			return api.ReplayBlockTransactions(m.Ctx, rpc.BlockNumberOrHashWithNumber(number), []string{TraceTypeTrace, TraceTypeStateDiff}, &bailout, nil)
		}},
		{"filter", func(bailout bool) (any, error) {
			stream := jsonstream.New(nil)
			err := api.Filter(m.Ctx, TraceFilterRequest{FromBlock: &number, ToBlock: &number}, &bailout, nil, stream)
			return string(stream.Buffer()), err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, err := tc.call(false)
			require.NoError(t, err)
			got, err := tc.call(true)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}

	t.Run("overlay with unrelated override", func(t *testing.T) {
		base := newBaseApiForTest(m)
		overlay := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: 1_000_000}, NewOtterscanAPI(base, m.DB, 25))
		balance := (*hexutil.U256)(uint256.NewInt(1))
		overrides := ethapi.StateOverrides{accounts.InternAddress(common.HexToAddress("0x9999")): {Balance: &balance}}
		logs, err := overlay.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(1)}, &overrides, nil)
		require.NoError(t, err)
		require.Len(t, logs, 2)
		for _, log := range logs {
			require.Equal(t, body, log.Address)
		}
	})
}
