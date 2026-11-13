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
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"sync/atomic"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func TestOverlayGetBeginEnd(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := &OverlayAPIImpl{BaseAPI: newBaseApiForTest(m)}
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	latestExecuted, _, _, err := rpchelper.GetBlockNumber(m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.LatestExecutedBlockNumber), tx, api._blockReader)
	require.NoError(t, err)

	begin, end, err := getBeginEnd(m.Ctx, tx, api, filters.FilterCriteria{FromBlock: big.NewInt(2), ToBlock: big.NewInt(5)})
	require.NoError(t, err)
	require.Equal(t, uint64(2), begin)
	require.Equal(t, uint64(5), end)

	begin, end, err = getBeginEnd(m.Ctx, tx, api, filters.FilterCriteria{})
	require.NoError(t, err)
	require.Equal(t, latestExecuted, begin)
	require.Equal(t, latestExecuted, end)

	_, _, err = getBeginEnd(m.Ctx, tx, api, filters.FilterCriteria{FromBlock: big.NewInt(5), ToBlock: big.NewInt(2)})
	require.EqualError(t, err, "end (2) < begin (5)")

	block, err := api._blockReader.BlockByNumber(m.Ctx, tx, 1)
	require.NoError(t, err)
	blockHash := block.Hash()
	begin, end, err = getBeginEnd(m.Ctx, tx, api, filters.FilterCriteria{BlockHash: &blockHash})
	require.NoError(t, err)
	require.Equal(t, uint64(1), begin)
	require.Equal(t, uint64(1), end)
}

type overlayGetLogsTestSetup struct {
	m           *execmoduletester.ExecModuleTester
	api         *OverlayAPIImpl
	blockNumber *big.Int
	rules       *chain.Rules
}

// newOverlayGetLogsTestSetup builds a single-block chain where txn 0 (a call to
// failedTarget with failedTxGas) fails and txn 1 (a call to controlTarget)
// succeeds, and returns an overlay API ready to query that block.
func newOverlayGetLogsTestSetup(t *testing.T, code map[common.Address][]byte, failedTarget common.Address, failedTxGas uint64, controlTarget common.Address) overlayGetLogsTestSetup {
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)

	alloc := types.GenesisAlloc{sender: {Balance: big.NewInt(9_000_000_000_000_000_000)}}
	for addr, c := range code {
		alloc[addr] = types.GenesisAccount{Balance: big.NewInt(0), Code: c}
	}
	genesis := &types.Genesis{Config: chain.TestChainBerlinConfig, Alloc: alloc, GasLimit: 10_000_000}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))
	signer := types.LatestSigner(genesis.Config)
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		failedTx, signErr := types.SignTx(types.NewTransaction(
			0, failedTarget, uint256.NewInt(0), failedTxGas, uint256.NewInt(0), nil,
		), *signer, key)
		require.NoError(t, signErr)
		b.AddFailedTx(failedTx)

		controlTx, signErr := types.SignTx(types.NewTransaction(
			1, controlTarget, uint256.NewInt(0), 100_000, uint256.NewInt(0), nil,
		), *signer, key)
		require.NoError(t, signErr)
		b.AddTx(controlTx)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	require.Len(t, pack.Receipts[0], 2)
	require.Equal(t, uint64(types.ReceiptStatusFailed), pack.Receipts[0][0].Status)
	require.Equal(t, uint64(types.ReceiptStatusSuccessful), pack.Receipts[0][1].Status)

	base := newBaseApiForTest(m)
	api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: 1_000_000}, NewOtterscanAPI(base, m.DB, 25))
	block := pack.Blocks[0]
	blockCtx := protocol.NewEVMBlockContext(block.HeaderNoCopy(), nil, m.Engine, accounts.NilAddress, m.ChainConfig)
	return overlayGetLogsTestSetup{
		m:           m,
		api:         api,
		blockNumber: new(big.Int).SetUint64(block.NumberU64()),
		rules:       blockCtx.Rules(m.ChainConfig),
	}
}

// A transaction that failed under the original code must still be replayed
// when a state override could flip its outcome: here the target's REVERT
// code is overridden with LOG0, so the hypothetical execution emits a log.
func TestOverlayGetLogsReplaysFailedTxWithCodeOverride(t *testing.T) {
	failedTarget := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	controlTarget := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	revertCode := []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.REVERT)}
	logCode := []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.LOG0)}

	setup := newOverlayGetLogsTestSetup(t, map[common.Address][]byte{
		failedTarget:  revertCode,
		controlTarget: logCode,
	}, failedTarget, 100_000, controlTarget)

	overrideCode := hexutil.Bytes(logCode)
	overrides := ethapi.StateOverrides{
		accounts.InternAddress(failedTarget): {Code: &overrideCode},
	}
	logs, err := setup.api.GetLogs(setup.m.Ctx, filters.FilterCriteria{
		FromBlock: setup.blockNumber,
		ToBlock:   setup.blockNumber,
		Addresses: []common.Address{failedTarget, controlTarget},
	}, &overrides, setup.rules)
	require.NoError(t, err)
	require.Len(t, logs, 2)
	require.Equal(t, failedTarget, logs[0].Address, "originally failed transaction should be replayed under the code override and emit one log")
	require.Equal(t, controlTarget, logs[1].Address, "positive-control transaction should emit one log")
	require.Equal(t, []hexutil.Uint{0, 1}, []hexutil.Uint{logs[0].TxIndex, logs[1].TxIndex})
}

// The rules parameter is optional: a request with state overrides and no rules must not crash.
func TestOverlayGetLogsStateOverrideWithoutRules(t *testing.T) {
	failedTarget := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	controlTarget := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	revertCode := []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.REVERT)}
	logCode := []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.LOG0)}
	setup := newOverlayGetLogsTestSetup(t, map[common.Address][]byte{
		failedTarget:  revertCode,
		controlTarget: logCode,
	}, failedTarget, 100_000, controlTarget)

	balance := (*hexutil.U256)(uint256.NewInt(1))
	logs, err := setup.api.GetLogs(setup.m.Ctx, filters.FilterCriteria{
		FromBlock: setup.blockNumber,
		ToBlock:   setup.blockNumber,
		Addresses: []common.Address{failedTarget, controlTarget},
	}, &ethapi.StateOverrides{accounts.InternAddress(controlTarget): {Balance: &balance}}, nil)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, controlTarget, logs[0].Address)
}

// Raising an out-of-gas transaction's limit to the RPC cap would make it
// succeed and emit a log that was absent from the original execution.
func TestOverlayGetLogsPreservesFailedTxOutcome(t *testing.T) {
	target := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	logCode := []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.LOG0)}

	setup := newOverlayGetLogsTestSetup(t, map[common.Address][]byte{target: logCode}, target, 21_100, target)

	logs, err := setup.api.GetLogs(setup.m.Ctx, filters.FilterCriteria{
		FromBlock: setup.blockNumber,
		ToBlock:   setup.blockNumber,
		Addresses: []common.Address{target},
	}, nil, setup.rules)
	require.NoError(t, err, "nonce accounting after failure must let the next txn of the same sender replay")
	require.Len(t, logs, 1, "only the successful control transaction should emit a log")
	require.Equal(t, hexutil.Uint(1), logs[0].TxIndex, "the log must come from the control transaction")
}

func TestOverlayGetLogsKeepsSuccessfulFrameLogs(t *testing.T) {
	cfg := chain.AllProtocolChanges.Copy()
	cfg.BogotaTime = common.NewUint64(0)
	sender := common.HexToAddress("0x1000")
	successfulTarget, revertingTarget := common.HexToAddress("0x2000"), common.HexToAddress("0x3000")
	balanceLog := program.New().Push(sender).Op(vm.BALANCE).Push0().Op(vm.MSTORE).
		Push(32).Push0().Op(vm.LOG0).Bytes()
	genesis := &types.Genesis{
		Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
		Alloc: types.GenesisAlloc{
			sender: {
				Balance: big.NewInt(1_000_000_000_000_000_000),
				Code:    program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(),
			},
			successfulTarget: {Code: balanceLog},
			revertingTarget:  {Code: program.New().Push0().Push0().Op(vm.LOG0).Push0().Push0().Op(vm.REVERT).Bytes()},
		},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
	frameTx := &types.FrameTransaction{
		ChainID: *cfg.ChainID, NonceKeys: nil, SenderAddress: sender,
		Fees: types.FrameFees{MaxFee: uint256.Int{1}},
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
			{Mode: types.FrameSender, Target: &successfulTarget, Limits: mdgas.MdGas{Execution: 10_000, State: 5_000}},
			{Mode: types.FrameSender, Target: &revertingTarget, Limits: mdgas.MdGas{Execution: 10_000}},
		},
	}
	frameTx.Fees.MaxFee.Mul(&frameTx.Fees.MaxFee, uint256.NewInt(frameTx.GetGasLimit()))
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) { b.AddFailedTx(frameTx) })
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	receipt := pack.Receipts[0][0]
	require.Equal(t, types.ReceiptStatusFailed, receipt.Status)
	require.Len(t, receipt.Logs, 1)
	require.Equal(t, successfulTarget, receipt.Logs[0].Address)
	_, _, limits, err := frameTx.GasLimits()
	require.NoError(t, err)
	require.Greater(t, limits.Execution, limits.State)

	base := newBaseApiForTest(m)
	blockNumber := new(big.Int).SetUint64(pack.Blocks[0].NumberU64())
	overrideBalance := (*hexutil.U256)(uint256.NewInt(1))
	for _, tc := range []struct {
		name      string
		gasCap    uint64
		overrides *ethapi.StateOverrides
		wantErr   string
	}{
		{name: "within cap", gasCap: 1_000_000},
		{name: "below consumed gas", gasCap: receipt.GasUsed - 1, wantErr: "exceeds the RPC gas cap"},
		{name: "below execution budget", gasCap: limits.Execution - 1, wantErr: "exceeds the RPC gas cap"},
		{name: "at execution budget", gasCap: limits.Execution},
		{name: "unlimited", gasCap: 0},
		{name: "empty overrides", gasCap: limits.Execution, overrides: &ethapi.StateOverrides{}},
		{
			name: "underfunded balance override", gasCap: limits.Execution,
			overrides: &ethapi.StateOverrides{accounts.InternAddress(sender): {Balance: &overrideBalance}},
			wantErr:   "verification frame 0 failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: tc.gasCap}, NewOtterscanAPI(base, m.DB, 25))
			logs, err := api.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: blockNumber, ToBlock: blockNumber}, tc.overrides, nil)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Empty(t, logs)
				return
			}
			require.NoError(t, err)
			require.Len(t, logs, 1)
			require.Equal(t, successfulTarget, logs[0].Address)
			require.Equal(t, []byte(receipt.Logs[0].Data), []byte(logs[0].Data))
		})
	}
}

func TestOverlayGetLogsPreservesOrdinaryFeesBeforeFrame(t *testing.T) {
	for _, tc := range []struct {
		name   string
		failed bool
	}{
		{name: "successful"},
		{name: "reverted", failed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := chain.AllProtocolChanges.Copy()
			cfg.BogotaTime = common.NewUint64(0)
			key, err := crypto.GenerateKey()
			require.NoError(t, err)
			sender := crypto.PubkeyToAddress(key.PublicKey)
			ordinaryTarget, frameTarget := common.HexToAddress("0x2000"), common.HexToAddress("0x3000")
			ordinaryCode := []byte{byte(vm.STOP)}
			if tc.failed {
				ordinaryCode = program.New().Push0().Push0().Op(vm.REVERT).Bytes()
			}
			balanceLog := program.New().Push(sender).Op(vm.BALANCE).Push0().Op(vm.MSTORE).
				Push(32).Push0().Op(vm.LOG0).Bytes()
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
				Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
				Alloc: types.GenesisAlloc{
					sender:         {Balance: big.NewInt(1_000_000_000_000_000_000)},
					ordinaryTarget: {Code: ordinaryCode},
					frameTarget:    {Code: balanceLog},
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
				},
				Signatures: []types.FrameSignature{{Scheme: types.FrameSignatureSecp256k1, Signature: make([]byte, 65)}},
			}
			frameTx.Fees.MaxFee.Mul(&frameTx.Fees.MaxFee, uint256.NewInt(frameTx.GetGasLimit()))
			hash := frameTx.SigningHash(nil)
			sig, err := crypto.Sign(hash[:], key)
			require.NoError(t, err)
			frameTx.Signatures[0].Signature = append([]byte{sig[64]}, sig[:64]...)
			pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
				if tc.failed {
					b.AddFailedTx(ordinaryTx)
				} else {
					b.AddTx(ordinaryTx)
				}
				b.AddTx(frameTx)
			})
			require.NoError(t, err)
			require.NoError(t, m.InsertChain(pack))
			require.Len(t, pack.Receipts[0][1].Logs, 1)

			base := newBaseApiForTest(m)
			api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: 1_000_000}, NewOtterscanAPI(base, m.DB, 25))
			blockNumber := new(big.Int).SetUint64(pack.Blocks[0].NumberU64())
			logs, err := api.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: blockNumber, ToBlock: blockNumber}, nil, nil)
			require.NoError(t, err)
			require.Len(t, logs, 1)
			require.Equal(t, pack.Receipts[0][1].Logs[0].Data, logs[0].Data)
		})
	}
}

func newOverlayGetLogsSingleLogSetup(t *testing.T) (overlayGetLogsTestSetup, filters.FilterCriteria) {
	target := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	logCode := []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.LOG0)}
	setup := newOverlayGetLogsTestSetup(t, map[common.Address][]byte{target: logCode}, target, 21_100, target)
	return setup, filters.FilterCriteria{FromBlock: setup.blockNumber, ToBlock: setup.blockNumber, Addresses: []common.Address{target}}
}

func TestOverlayGetLogsOrdinaryGasCap(t *testing.T) {
	setup, crit := newOverlayGetLogsSingleLogSetup(t)
	balance := (*hexutil.U256)(uint256.NewInt(1))
	for _, tc := range []struct {
		name      string
		gasCap    uint64
		overrides *ethapi.StateOverrides
		wantLogs  int
		wantErr   bool
	}{
		{name: "below declared limit", gasCap: 99_999, wantErr: true},
		{name: "at declared limit", gasCap: 100_000, wantLogs: 1},
		{name: "unlimited", gasCap: 0, wantLogs: 1},
		{name: "empty overrides", gasCap: 99_999, overrides: &ethapi.StateOverrides{}, wantErr: true},
		{
			name: "state override", gasCap: 99_999, wantLogs: 2,
			overrides: &ethapi.StateOverrides{accounts.InternAddress(crit.Addresses[0]): {Balance: &balance}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setup.api.GasCap = tc.gasCap
			logs, err := setup.api.GetLogs(setup.m.Ctx, crit, tc.overrides, nil)
			if tc.wantErr {
				require.ErrorContains(t, err, "exceeds the RPC gas cap")
			} else {
				require.NoError(t, err)
			}
			require.Len(t, logs, tc.wantLogs)
		})
	}
}

func TestOverlayGetLogsOverrideGasCeiling(t *testing.T) {
	cfg := chain.AllProtocolChanges.Copy()
	cfg.AmsterdamTime, cfg.BogotaTime = nil, nil
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender, target := crypto.PubkeyToAddress(key.PublicKey), common.HexToAddress("0x2000")
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
		Alloc: types.GenesisAlloc{
			sender: {Balance: big.NewInt(1_000_000_000_000_000_000)},
			target: {Code: program.New().Push0().Push0().Op(vm.LOG0, vm.STOP).Bytes()},
		},
	}), execmoduletester.WithKey(key))
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		for nonce := range uint64(3) {
			txn, err := types.SignTx(types.NewTransaction(nonce, target, new(uint256.Int), 40_000, uint256.NewInt(2), nil), *types.LatestSigner(cfg), key)
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	base := newBaseApiForTest(m)
	blockNumber := new(big.Int).SetUint64(pack.Blocks[0].NumberU64())
	balance := (*hexutil.U256)(uint256.NewInt(1))
	overrides := ethapi.StateOverrides{accounts.InternAddress(target): {Balance: &balance}}
	for _, cap := range []uint64{params.MaxTxnGasLimit, params.MaxTxnGasLimit + 1, rpccfg.DefaultGasCap, 0} {
		api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: cap}, NewOtterscanAPI(base, m.DB, 25))
		logs, err := api.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: blockNumber, ToBlock: blockNumber}, &overrides, nil)
		require.NoError(t, err, "gas cap %d", cap)
		require.Len(t, logs, 3)
	}
}

func TestOverlayGetLogsOverrideExecutionGasCap(t *testing.T) {
	cfg := chain.AllProtocolChanges.Copy()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender, target := crypto.PubkeyToAddress(key.PublicKey), common.HexToAddress("0x2000")
	gasLogCode := program.New().Op(vm.GAS).Push0().Op(vm.MSTORE).Push(32).Push0().Op(vm.LOG0, vm.STOP).Bytes()
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: cfg, GasLimit: 60_000_000, BaseFee: uint256.NewInt(1),
		Alloc: types.GenesisAlloc{
			sender: {Balance: big.NewInt(1_000_000_000_000_000_000)},
			target: {Code: gasLogCode},
		},
	}), execmoduletester.WithKey(key))
	txn, err := types.SignTx(types.NewTransaction(0, target, new(uint256.Int), 50_000_000, uint256.NewInt(2), nil), *types.LatestSigner(cfg), key)
	require.NoError(t, err)
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) { b.AddTx(txn) })
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	base := newBaseApiForTest(m)
	blockNumber := new(big.Int).SetUint64(pack.Blocks[0].NumberU64())
	balance := (*hexutil.U256)(uint256.NewInt(1))
	overrides := ethapi.StateOverrides{accounts.InternAddress(target): {Balance: &balance}}
	for _, tc := range []struct {
		name              string
		gasCap            uint64
		overrides         *ethapi.StateOverrides
		aboveExecutionCap bool
	}{
		{name: "canonical replay", gasCap: 50_000_000},
		{name: "RPC budget", gasCap: 50_000_000, overrides: &overrides, aboveExecutionCap: true},
		{name: "small RPC budget", gasCap: 1_000_000, overrides: &overrides},
		{name: "unlimited", overrides: &overrides, aboveExecutionCap: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: tc.gasCap}, NewOtterscanAPI(base, m.DB, 25))
			logs, err := api.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: blockNumber, ToBlock: blockNumber}, tc.overrides, nil)
			require.NoError(t, err)
			require.Len(t, logs, 1)
			gasLeft := new(uint256.Int).SetBytes(logs[0].Data).Uint64()
			if tc.aboveExecutionCap {
				require.Greater(t, gasLeft, params.MaxTxnGasLimit)
			} else {
				require.Less(t, gasLeft, params.MaxTxnGasLimit)
			}
			if tc.gasCap != 0 {
				require.Less(t, gasLeft, tc.gasCap)
			}
		})
	}
}

func TestOverlayGetLogsReturnsBlockReplayError(t *testing.T) {
	setup, crit := newOverlayGetLogsSingleLogSetup(t)
	overrides := ethapi.StateOverrides{
		accounts.InternAddress(crit.Addresses[0]): {State: &map[common.Hash]common.Hash{}, StateDiff: &map[common.Hash]common.Hash{}},
	}

	_, err := setup.api.GetLogs(setup.m.Ctx, crit, &overrides, setup.rules)
	require.ErrorContains(t, err, "has both 'state' and 'stateDiff'")
}

type failWorkerBeginDB struct {
	kv.TemporalRoDB
	begins atomic.Int32
}

func (db *failWorkerBeginDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	if db.begins.Add(1) > 1 {
		return nil, errors.New("worker begin failed")
	}
	return db.TemporalRoDB.BeginTemporalRo(ctx) //nolint:gocritic
}

func TestOverlayGetLogsReturnsWorkerBeginError(t *testing.T) {
	setup, crit := newOverlayGetLogsSingleLogSetup(t)
	setup.api.db = &failWorkerBeginDB{TemporalRoDB: setup.m.DB}

	_, err := setup.api.GetLogs(setup.m.Ctx, crit, nil, setup.rules)
	require.ErrorContains(t, err, "worker begin failed")
}

func TestCallConstructorReturnsOverlayError(t *testing.T) {
	m, _, contractAddress, _ := chainWithDeployedContractAndConfig(t, chain.AllProtocolChanges)
	base := newBaseApiForTest(m)
	for _, tc := range []struct {
		name   string
		gasCap uint64
	}{
		{name: "limited", gasCap: 1_000_000},
		{name: "unlimited", gasCap: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: tc.gasCap}, NewOtterscanAPI(base, m.DB, 25))
			code := hexutil.Bytes{byte(vm.INVALID)}
			result, err := api.CallConstructor(m.Ctx, contractAddress, code)
			require.Nil(t, result)
			var invalidOpcode *vm.ErrInvalidOpCode
			require.ErrorAs(t, err, &invalidOpcode)
		})
	}
}

func TestCallConstructorIntrinsicExecutionGasCap(t *testing.T) {
	m, key, sender := fundedBankGenesis(t, chain.AllProtocolChanges)
	accessList := make(types.AccessList, 4_000)
	for i := range accessList {
		accessList[i].Address = sender
	}
	txn, err := types.SignTx(&types.AccessListTx{
		LegacyTx:   *types.NewContractCreation(0, new(uint256.Int), 30_000_000, uint256.NewInt(2_000_000_000), program.New().ReturnData([]byte{byte(vm.STOP)}).Bytes()),
		ChainID:    *m.ChainConfig.ChainID,
		AccessList: accessList,
	}, *types.LatestSigner(m.ChainConfig), key)
	require.NoError(t, err)
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) { b.AddTx(txn) })
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	base := newBaseApiForTest(m)
	want := []byte{byte(vm.PUSH0), byte(vm.STOP)}
	// The access list fits under the execution cap, but the larger replacement
	// initcode takes the intrinsic gas above it.
	code := make(hexutil.Bytes, 16_384)
	copy(code, program.New().ReturnData(want).Bytes())
	for _, tc := range []struct {
		name   string
		gasCap uint64
	}{
		{name: "RPC budget", gasCap: 50_000_000},
		{name: "unlimited"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: tc.gasCap}, NewOtterscanAPI(base, m.DB, 25))
			result, err := api.CallConstructor(m.Ctx, types.CreateAddress(sender, 0), code)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.NotNil(t, result.Code)
			require.Equal(t, hexutil.Bytes(want), *result.Code)
		})
	}
}

func TestFrameCallConstructor(t *testing.T) {
	cfg := chain.AllProtocolChanges.Copy()
	cfg.BogotaTime = common.NewUint64(0)
	sender := common.HexToAddress("0x1000")
	initCode := program.New().ReturnData([]byte{byte(vm.STOP)}).Bytes()
	create := program.New().Mstore(initCode, 0).Push(len(initCode)).Push0().Push0().Op(vm.CREATE, vm.POP, vm.STOP).Bytes()
	verifyOffset := 4 + len(create)
	require.Less(t, verifyOffset, 256)
	code := append([]byte{byte(vm.CALLDATASIZE), byte(vm.PUSH1), byte(verifyOffset), byte(vm.JUMPI)}, create...)
	code = append(code, program.New().Op(vm.JUMPDEST).Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes()...)
	genesis := &types.Genesis{
		Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
		Alloc: types.GenesisAlloc{sender: {Balance: big.NewInt(1_000_000_000_000_000_000), Code: code}},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
	frameTx := &types.FrameTransaction{
		ChainID: *cfg.ChainID, NonceKeys: nil, SenderAddress: sender,
		Fees: types.FrameFees{MaxFee: uint256.Int{1}},
		Frames: []types.Frame{
			{Mode: types.FrameDefault, Limits: mdgas.MdGas{Execution: 200_000, State: 1_000_000}},
			{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Data: []byte{1}, Limits: mdgas.MdGas{Execution: 1_000}},
		},
	}
	frameTx.Fees.MaxFee.Mul(&frameTx.Fees.MaxFee, uint256.NewInt(frameTx.GetGasLimit()))
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) { b.AddTx(frameTx) })
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	base := newBaseApiForTest(m)
	want := []byte{byte(vm.PUSH0), byte(vm.STOP)}
	_, _, limits, err := frameTx.GasLimits()
	require.NoError(t, err)
	require.Greater(t, limits.State, limits.Execution)
	for _, tc := range []struct {
		name    string
		gasCap  uint64
		wantErr bool
	}{
		{name: "within cap", gasCap: 2_000_000},
		{name: "below consumed gas", gasCap: pack.Receipts[0][0].GasUsed - 1, wantErr: true},
		{name: "below state budget", gasCap: limits.State - 1, wantErr: true},
		{name: "at state budget", gasCap: limits.State},
		{name: "unlimited", gasCap: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: tc.gasCap}, NewOtterscanAPI(base, m.DB, 25))
			result, err := api.CallConstructor(m.Ctx, types.CreateAddress(sender, 0), program.New().ReturnData(want).Bytes())
			if tc.wantErr {
				require.ErrorContains(t, err, "exceeds the RPC gas cap")
				require.Nil(t, result)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result)
			require.NotNil(t, result.Code)
			require.Equal(t, hexutil.Bytes(want), *result.Code)
		})
	}
}

func TestFrameCallConstructorFeeAccounting(t *testing.T) {
	cfg := chain.AllProtocolChanges.Copy()
	cfg.BogotaTime = common.NewUint64(0)
	sender, factory := common.HexToAddress("0x1000"), common.HexToAddress("0x2000")
	const initialBalance = 1_000_000_000_000_000_000
	initCode := program.New().ReturnData([]byte{byte(vm.STOP)}).Bytes()
	// Skipping the fee debit takes the revert branch before CREATE.
	code := program.New().Push(sender).Op(vm.BALANCE).Push(initialBalance).Op(vm.EQ)
	revertJump := code.Size()
	code.Push(0).Op(vm.JUMPI).Mstore(initCode, 0).Push(len(initCode)).Push0().Push0().Op(vm.CREATE, vm.POP, vm.STOP)
	_, revertPC := code.Jumpdest()
	require.Less(t, revertPC, uint64(256))
	code.Bytes()[revertJump+1] = byte(revertPC)
	code.Push0().Push0().Op(vm.REVERT)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
		Alloc: types.GenesisAlloc{
			sender: {
				Balance: big.NewInt(initialBalance),
				Code:    program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes(),
			},
			factory: {Code: code.Bytes()},
		},
	}))
	frameTx := &types.FrameTransaction{
		ChainID: *cfg.ChainID, NonceKeys: nil, SenderAddress: sender,
		Fees: types.FrameFees{MaxFee: uint256.Int{1}},
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
			{Mode: types.FrameSender, Target: &factory, Limits: mdgas.MdGas{Execution: 200_000, State: 1_000_000}},
		},
	}
	frameTx.Fees.MaxFee.Mul(&frameTx.Fees.MaxFee, uint256.NewInt(frameTx.GetGasLimit()))
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) { b.AddTx(frameTx) })
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	require.EqualValues(t, types.ReceiptStatusSuccessful, pack.Receipts[0][0].Status)
	base := newBaseApiForTest(m)
	api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: 2_000_000}, NewOtterscanAPI(base, m.DB, 25))
	want := []byte{byte(vm.PUSH0), byte(vm.STOP)}
	result, err := api.CallConstructor(m.Ctx, types.CreateAddress(factory, 0), program.New().ReturnData(want).Bytes())
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Code)
	require.Equal(t, hexutil.Bytes(want), *result.Code)
}

func TestCallConstructorRejectsNonContract(t *testing.T) {
	m, bankAddress, _, _ := chainWithDeployedContract(t)
	base := newBaseApiForTest(m)
	api := NewOverlayAPI(
		base,
		m.DB,
		&rpccfg.OverlayApiConfig{GasCap: 1_000_000},
		NewOtterscanAPI(base, m.DB, 25),
	)
	code := hexutil.Bytes{byte(vm.STOP)}
	for _, addr := range []common.Address{bankAddress, common.HexToAddress("0xdead")} {
		result, err := api.CallConstructor(m.Ctx, addr, code)
		require.Nil(t, result)
		require.Error(t, err)
	}
}

func TestCallConstructorRequiresCode(t *testing.T) {
	m, _, contractAddress, _ := chainWithDeployedContract(t)
	base := newBaseApiForTest(m)
	api := NewOverlayAPI(
		base,
		m.DB,
		&rpccfg.OverlayApiConfig{GasCap: 1_000_000},
		NewOtterscanAPI(base, m.DB, 25),
	)
	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterName("overlay", api))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(func() { client.Close(); server.Stop() })

	for name, params := range map[string][]any{
		"omitted": {contractAddress},
		"null":    {contractAddress, nil},
	} {
		t.Run(name, func(t *testing.T) {
			var result json.RawMessage
			err := client.CallContext(t.Context(), &result, "overlay_callConstructor", params...)
			var rpcErr rpc.Error
			require.ErrorAs(t, err, &rpcErr)
			require.Equal(t, rpc.ErrCodeInvalidParams, rpcErr.ErrorCode())
		})
	}
}
