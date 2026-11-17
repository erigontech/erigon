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
	"github.com/erigontech/erigon/execution/vm/testutil"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// Enough to fund the original gas budgets, but not an enlarged RPC allowance.
const mixedFrameReplayBalance = 200_000

func newMixedFrameReplayChain(t *testing.T, ordinaryReverts bool) (*execmoduletester.ExecModuleTester, *blockgen.ChainPack) {
	t.Helper()
	cfg := chain.AllProtocolChanges.Copy()
	cfg.BogotaTime = common.NewUint64(0)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	ordinaryTarget, frameTarget, assertion := common.HexToAddress("0x2000"), common.HexToAddress("0x3000"), common.HexToAddress("0x4000")
	ordinaryCode := program.New().Push(sender).Op(vm.BALANCE).Push0().Op(vm.MSTORE).
		Push(32).Push0().Op(vm.RETURN).Bytes()
	if ordinaryReverts {
		ordinaryCode = program.New().Push0().Push0().Op(vm.REVERT).Bytes()
	}
	balanceLog := program.New().Push(sender).Op(vm.BALANCE).Push0().Op(vm.MSTORE).
		Push(32).Push0().Op(vm.LOG0).Bytes()
	// Revert if the preceding transaction's fee debit is missing from the prestate.
	assertionCode := program.New().Push0().Push(5).Op(vm.TXTRACE).
		Push(mixedFrameReplayBalance).Op(vm.EQ)
	testutil.AssertStackWord(assertionCode, 0).Op(vm.STOP)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
		Alloc: types.GenesisAlloc{
			sender:         {Balance: big.NewInt(mixedFrameReplayBalance)},
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
		ChainID: *cfg.ChainID, Nonce: 1, NonceKeys: nil, SenderAddress: sender,
		Fees: types.FrameFees{MaxFee: uint256.Int{1}},
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
			{Mode: types.FrameSender, Target: &frameTarget, Limits: mdgas.MdGas{Execution: 10_000}},
			{Mode: types.FrameAssert, Target: &assertion, Limits: mdgas.MdGas{Execution: 10_000}},
		},
		Signatures: []types.FrameSignature{{Scheme: types.FrameSignatureSecp256k1, Signature: make([]byte, 65)}},
	}
	frameTx.Fees.MaxFee.Mul(&frameTx.Fees.MaxFee, uint256.NewInt(frameTx.GetGasLimit()))
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

func TestTraceReplayChargesOrdinaryFees(t *testing.T) {
	m, pack := newMixedFrameReplayChain(t, false)
	api := newTraceApiForTest(m)
	txn := pack.Blocks[0].Transactions()[0]
	upfront := new(uint256.Int).Mul(uint256.NewInt(txn.GetGasLimit()), txn.GetFeeCap())
	expected := new(uint256.Int).Sub(uint256.NewInt(mixedFrameReplayBalance), upfront).Bytes32()

	t.Run("single transaction", func(t *testing.T) {
		result, err := api.ReplayTransaction(m.Ctx, txn.Hash(), []string{TraceTypeTrace, TraceTypeStateDiff}, nil)
		require.NoError(t, err)
		require.Equal(t, expected[:], []byte(result.Output))
	})
	t.Run("parallel block", func(t *testing.T) {
		results, err := api.ReplayBlockTransactions(m.Ctx, rpc.BlockNumberOrHashWithNumber(1), []string{TraceTypeTrace}, nil)
		require.NoError(t, err)
		require.Len(t, results, 2)
		require.Equal(t, expected[:], []byte(results[0].Output))
	})
	t.Run("filter", func(t *testing.T) {
		number := rpc.BlockNumber(1)
		stream := jsonstream.New(nil)
		require.NoError(t, api.Filter(m.Ctx, TraceFilterRequest{FromBlock: &number, ToBlock: &number}, nil, stream))
		var traces []struct {
			TransactionHash common.Hash `json:"transactionHash"`
			Result          TraceResult `json:"result"`
		}
		require.NoError(t, json.Unmarshal(stream.Buffer(), &traces))
		require.NotEmpty(t, traces)
		require.Equal(t, txn.Hash(), traces[0].TransactionHash)
		require.Equal(t, expected[:], []byte(traces[0].Result.Output))
	})
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

func TestAssertReplayPreservesFeeAccounting(t *testing.T) {
	cfg := chain.AllProtocolChanges.Copy()
	cfg.BogotaTime = common.NewUint64(0)
	sender, body, assertion := common.HexToAddress("0x1000"), common.HexToAddress("0x2000"), common.HexToAddress("0x3000")
	// With no value transfers, the payer's balance delta must equal the pre-charge.
	code := program.New().Push0().Push(0x19).Op(vm.TXTRACE).
		Push0().Push(6).Op(vm.TXTRACE).
		Push0().Push(5).Op(vm.TXTRACE, vm.SUB, vm.EQ)
	testutil.AssertStackWord(code, 1).Op(vm.STOP)
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
			tx := &types.FrameTransaction{
				ChainID: *cfg.ChainID, NonceKeys: nil, Nonce: nonce, SenderAddress: sender,
				Fees: types.FrameFees{MaxFee: uint256.Int{1}},
				Frames: []types.Frame{
					{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
					{Mode: types.FrameSender, Target: &body, Limits: mdgas.MdGas{Execution: 10_000}},
					{Mode: types.FrameAssert, Target: &assertion, Limits: mdgas.MdGas{Execution: 10_000}},
				},
			}
			tx.Fees.MaxFee.Mul(&tx.Fees.MaxFee, uint256.NewInt(tx.GetGasLimit()))
			b.AddTx(tx)
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
