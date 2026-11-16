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
	"encoding/json"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
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

func newMixedFrameReplayChain(t *testing.T, ordinaryReverts bool) (*execmoduletester.ExecModuleTester, *blockgen.ChainPack) {
	t.Helper()
	cfg := chain.AllProtocolChanges.Copy()
	cfg.BogotaTime = common.NewUint64(0)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	ordinaryTarget, frameTarget, assertion := common.HexToAddress("0x2000"), common.HexToAddress("0x3000"), common.HexToAddress("0x4000")
	ordinaryCode := []byte{byte(vm.STOP)}
	if ordinaryReverts {
		ordinaryCode = program.New().Push0().Push0().Op(vm.REVERT).Bytes()
	}
	balanceLog := program.New().Push(sender).Op(vm.BALANCE).Push0().Op(vm.MSTORE).
		Push(32).Push0().Op(vm.LOG0).Bytes()
	// Enough to fund the original gas budgets, but not an enlarged RPC allowance.
	const initialBalance = 200_000
	// Revert if the preceding transaction's fee debit is missing from the prestate.
	assertionCode := program.New().Push0().Push(sender).Push(2).Op(vm.TXDIFF).
		Push(initialBalance).Op(vm.EQ)
	revertJump := assertionCode.Size()
	assertionCode.Push(0).Op(vm.JUMPI, vm.STOP)
	_, revertPC := assertionCode.Jumpdest()
	require.Less(t, revertPC, uint64(256))
	assertionCode.Bytes()[revertJump+1] = byte(revertPC)
	assertionCode.Push0().Push0().Op(vm.REVERT)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
		Alloc: types.GenesisAlloc{
			sender:         {Balance: big.NewInt(initialBalance)},
			ordinaryTarget: {Code: ordinaryCode},
			frameTarget:    {Code: balanceLog},
			assertion:      {Code: assertionCode.Bytes()},
		},
	}), execmoduletester.WithKey(key))
	ordinaryTx, err := types.SignTx(types.NewTransaction(
		0, ordinaryTarget, new(uint256.Int), 40_000, uint256.NewInt(2), nil,
	), *types.LatestSigner(cfg), key)
	require.NoError(t, err)
	frameTx := &types.FrameTransaction{
		ChainID: *cfg.ChainID, Nonce: 1, NonceKeys: []uint256.Int{{}}, SenderAddress: sender,
		Fees: types.FrameFees{FeeCap: uint256.Int{1}},
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
			{Mode: types.FrameSender, Target: &frameTarget, Limits: mdgas.MdGas{Execution: 10_000}},
			{Mode: types.FramePostTx, Target: &assertion, Limits: mdgas.MdGas{Execution: 10_000}},
		},
		Signatures: []types.FrameSignature{{Scheme: types.FrameSignatureSecp256k1}},
	}
	hash := frameTx.SigningHash(nil)
	sig, err := crypto.Sign(hash[:], key)
	require.NoError(t, err)
	frameTx.Signatures[0].Signature = append([]byte{sig[64]}, sig[:64]...)
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		if ordinaryReverts {
			b.AddFailedTx(ordinaryTx)
		} else {
			b.AddTx(ordinaryTx)
		}
		b.AddTx(frameTx)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	require.EqualValues(t, types.ReceiptStatusSuccessful, pack.Receipts[0][1].FrameReceipts[2].Status)
	require.Len(t, pack.Receipts[0][1].Logs, 1)
	return m, pack
}

func TestReplayBlockPreservesOrdinaryFeesBeforeFrame(t *testing.T) {
	for _, tc := range []struct {
		name   string
		failed bool
	}{
		{name: "successful"},
		{name: "reverted", failed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, _ := newMixedFrameReplayChain(t, tc.failed)
			api := newTraceApiForTest(m)
			block := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(1))
			for _, traceTypes := range [][]string{{TraceTypeTrace}, {TraceTypeTrace, TraceTypeStateDiff}, {TraceTypeTrace, TraceTypeVmTrace}} {
				t.Run(traceTypes[len(traceTypes)-1], func(t *testing.T) {
					got, err := api.ReplayBlockTransactions(m.Ctx, block, traceTypes, nil)
					require.NoError(t, err)
					require.Len(t, got, 2)
					require.NotEmpty(t, got[1].Trace)
					for _, trace := range got[1].Trace {
						require.Empty(t, trace.Error)
					}
					last := got[1].Trace[len(got[1].Trace)-1]
					require.Equal(t, common.HexToAddress("0x4000"), last.Action.(*CallTraceAction).To)
				})
			}
		})
	}
}

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
		call func() (any, error)
	}{
		{"transaction", func() (any, error) {
			return api.Transaction(m.Ctx, hash, nil)
		}},
		{"block", func() (any, error) {
			return api.Block(m.Ctx, number, nil)
		}},
		{"replay transaction", func() (any, error) {
			return api.ReplayTransaction(m.Ctx, hash, []string{TraceTypeTrace, TraceTypeStateDiff}, nil)
		}},
		{"replay block", func() (any, error) {
			return api.ReplayBlockTransactions(m.Ctx, rpc.BlockNumberOrHashWithNumber(number), []string{TraceTypeTrace, TraceTypeStateDiff}, nil)
		}},
		{"filter", func() (any, error) {
			stream := jsonstream.New(nil)
			err := api.Filter(m.Ctx, TraceFilterRequest{FromBlock: &number, ToBlock: &number}, nil, stream)
			return json.RawMessage(stream.Buffer()), err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.call()
			require.NoError(t, err)
			encoded, err := json.Marshal(got)
			require.NoError(t, err)
			require.Contains(t, string(encoded), assertion.Hex())
			require.NotContains(t, string(encoded), `"error":`)
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
