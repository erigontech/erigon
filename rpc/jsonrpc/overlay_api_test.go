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
	"github.com/erigontech/erigon/execution/vm/testutil"
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
		ChainID: *cfg.ChainID, NonceKeys: []uint256.Int{{}}, SenderAddress: sender,
		Fees: types.FrameFees{FeeCap: uint256.Int{1}},
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
			{Mode: types.FrameSender, Target: &successfulTarget, Limits: mdgas.MdGas{Execution: 10_000, State: 5_000}},
			{Mode: types.FrameSender, Target: &revertingTarget, Limits: mdgas.MdGas{Execution: 10_000}},
		},
	}
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
		{name: "below consumed gas", gasCap: receipt.GasUsed - 1},
		{name: "below execution budget", gasCap: limits.Execution - 1},
		{name: "at execution budget", gasCap: limits.Execution},
		{name: "unlimited", gasCap: 0},
		{name: "empty overrides", gasCap: limits.Execution, overrides: &ethapi.StateOverrides{}},
		{
			name: "state override below consumed gas", gasCap: receipt.GasUsed - 1,
			overrides: &ethapi.StateOverrides{accounts.InternAddress(successfulTarget): {Balance: &overrideBalance}},
		},
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
			m, pack := newMixedFrameReplayChain(t, tc.failed)
			base := newBaseApiForTest(m)
			blockNumber := new(big.Int).SetUint64(pack.Blocks[0].NumberU64())
			balance := (*hexutil.U256)(uint256.NewInt(1))
			overrides := ethapi.StateOverrides{accounts.InternAddress(common.HexToAddress("0x9999")): {Balance: &balance}}
			for _, replay := range []struct {
				name      string
				gasCap    uint64
				overrides *ethapi.StateOverrides
			}{
				{name: "canonical", gasCap: 1_000_000},
				{name: "unrelated override", gasCap: 1_000_000, overrides: &overrides},
				{name: "unlimited with override", overrides: &overrides},
			} {
				t.Run(replay.name, func(t *testing.T) {
					api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: replay.gasCap}, NewOtterscanAPI(base, m.DB, 25))
					logs, err := api.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: blockNumber, ToBlock: blockNumber}, replay.overrides, nil)
					require.NoError(t, err)
					require.Len(t, logs, 1)
					require.Equal(t, pack.Receipts[0][1].Logs[0].Data, logs[0].Data)
				})
			}
		})
	}
}

func newOverlayGetLogsSingleLogSetup(t *testing.T) (overlayGetLogsTestSetup, filters.FilterCriteria) {
	target := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	logCode := []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.LOG0)}
	setup := newOverlayGetLogsTestSetup(t, map[common.Address][]byte{target: logCode}, target, 21_100, target)
	return setup, filters.FilterCriteria{FromBlock: setup.blockNumber, ToBlock: setup.blockNumber, Addresses: []common.Address{target}}
}

func TestOverlayGetLogsWithoutOverridesIgnoresRPCGasCap(t *testing.T) {
	setup, crit := newOverlayGetLogsSingleLogSetup(t)
	for _, tc := range []struct {
		name      string
		gasCap    uint64
		overrides *ethapi.StateOverrides
	}{
		{name: "below declared limit", gasCap: 99_999},
		{name: "at declared limit", gasCap: 100_000},
		{name: "unlimited", gasCap: 0},
		{name: "empty overrides", gasCap: 99_999, overrides: &ethapi.StateOverrides{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setup.api.GasCap = tc.gasCap
			logs, err := setup.api.GetLogs(setup.m.Ctx, crit, tc.overrides, nil)
			require.NoError(t, err)
			require.Len(t, logs, 1)
		})
	}
}

func TestOverlayGetLogsOverridesSkipOrdinaryFees(t *testing.T) {
	cfg := chain.AllProtocolChanges.Copy()
	cfg.AmsterdamTime, cfg.BogotaTime = nil, nil
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender, target := crypto.PubkeyToAddress(key.PublicKey), common.HexToAddress("0x2000")
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
		Alloc: types.GenesisAlloc{
			sender: {Balance: big.NewInt(1_000_000_000_000_000_000)},
			target: {Code: program.New().Push(sender).Op(vm.BALANCE).Push0().Op(vm.MSTORE).
				Push(32).Push0().Op(vm.LOG0, vm.STOP).Bytes()},
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
		for i, log := range logs {
			require.Equal(t, uint256.NewInt(1_000_000_000_000_000_000), new(uint256.Int).SetBytes(log.Data), "gas cap %d, transaction %d", cap, i)
		}
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
		name      string
		gasCap    uint64
		overrides *ethapi.StateOverrides
	}{
		{name: "canonical replay", gasCap: 50_000_000},
		{name: "canonical replay below declared limit", gasCap: 1_000_000},
		{name: "at declared limit", gasCap: 50_000_000, overrides: &overrides},
		{name: "below declared limit", gasCap: 1_000_000, overrides: &overrides},
		{name: "unlimited", overrides: &overrides},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: tc.gasCap}, NewOtterscanAPI(base, m.DB, 25))
			logs, err := api.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: blockNumber, ToBlock: blockNumber}, tc.overrides, nil)
			require.NoError(t, err)
			require.Len(t, logs, 1)
			gasLeft := new(uint256.Int).SetBytes(logs[0].Data).Uint64()
			if tc.overrides == nil {
				require.Less(t, gasLeft, params.MaxTxnGasLimit)
				require.Equal(t, pack.Receipts[0][0].Logs[0].Data, logs[0].Data)
			} else {
				msg := new(types.Message)
				msg.ChangeGas(tc.gasCap, tc.gasCap)
				intrinsic := params.TxBaseEIP2780 + params.ColdAccountAccessEIP2780
				require.Equal(t, msg.Gas()-intrinsic-vm.GasQuickStep, gasLeft)
			}
		})
	}
}

func TestOverlayGetLogsOverrideNeedsMoreGas(t *testing.T) {
	for _, tc := range []struct {
		name   string
		bogota bool
	}{
		{name: "before Bogota"},
		{name: "Bogota", bogota: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := chain.AllProtocolChanges.Copy()
			if tc.bogota {
				cfg.BogotaTime = common.NewUint64(0)
			} else {
				cfg.AmsterdamTime, cfg.BogotaTime = nil, nil
			}
			key, err := crypto.GenerateKey()
			require.NoError(t, err)
			sender, target := crypto.PubkeyToAddress(key.PublicKey), common.HexToAddress("0x2000")
			oneLog := program.New().Push0().Push0().Op(vm.LOG0, vm.STOP).Bytes()
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
				Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
				Alloc: types.GenesisAlloc{
					sender: {Balance: big.NewInt(1_000_000_000)},
					target: {Code: oneLog},
				},
			}), execmoduletester.WithKey(key))
			generate := func(gas uint64) (*blockgen.ChainPack, error) {
				return m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
					txn, err := types.SignTx(types.NewTransaction(0, target, new(uint256.Int), gas, uint256.NewInt(2), nil), *types.LatestSigner(cfg), key)
					require.NoError(t, err)
					b.AddTx(txn)
				})
			}
			probe, err := generate(100_000)
			require.NoError(t, err)
			pack, err := generate(probe.Receipts[0][0].GasUsed + 100)
			require.NoError(t, err)
			require.Equal(t, tc.bogota, cfg.IsBogota(pack.Blocks[0].Time()))
			require.NoError(t, m.InsertChain(pack))
			require.Len(t, pack.Receipts[0][0].Logs, 1)
			base := newBaseApiForTest(m)
			api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: rpccfg.DefaultGasCap}, NewOtterscanAPI(base, m.DB, 25))
			code := hexutil.Bytes(program.New().Push0().Push0().Op(vm.LOG0).Push0().Push0().Op(vm.LOG0, vm.STOP).Bytes())
			overrides := ethapi.StateOverrides{accounts.InternAddress(target): {Code: &code}}
			logs, err := api.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(1)}, &overrides, nil)
			require.NoError(t, err)
			require.Len(t, logs, 2)
		})
	}
}

func TestOverlayGetLogsOverrideRemovesFunding(t *testing.T) {
	cfg := chain.AllProtocolChanges.Copy()
	cfg.AmsterdamTime, cfg.BogotaTime = nil, nil
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	spendingKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender, recipient := crypto.PubkeyToAddress(key.PublicKey), crypto.PubkeyToAddress(spendingKey.PublicKey)
	forwarder, target := common.HexToAddress("0x2000"), common.HexToAddress("0x3000")
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
		Alloc: types.GenesisAlloc{
			sender: {Balance: big.NewInt(1_000_000_000)},
			forwarder: {Code: program.New().Push0().Push0().Push0().Push0().Op(vm.CALLVALUE).
				Push(recipient).Op(vm.GAS, vm.CALL, vm.POP, vm.STOP).Bytes()},
			target: {Code: program.New().Push0().Push0().Op(vm.LOG0, vm.STOP).Bytes()},
		},
	}), execmoduletester.WithKey(key))
	signer := types.LatestSigner(cfg)
	pack, err := m.GenerateChain(2, func(i int, b *blockgen.BlockGen) {
		if i == 0 {
			txn, err := types.SignTx(types.NewTransaction(0, target, new(uint256.Int), 60_000, uint256.NewInt(2), nil), *signer, key)
			require.NoError(t, err)
			b.AddTx(txn)
			return
		}
		fund, err := types.SignTx(types.NewTransaction(1, forwarder, uint256.NewInt(1_000_000), 100_000, uint256.NewInt(2), nil), *signer, key)
		require.NoError(t, err)
		b.AddTx(fund)
		spend, err := types.SignTx(types.NewTransaction(0, target, new(uint256.Int), 60_000, uint256.NewInt(2), nil), *signer, spendingKey)
		require.NoError(t, err)
		b.AddTx(spend)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	base := newBaseApiForTest(m)
	api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: rpccfg.DefaultGasCap}, NewOtterscanAPI(base, m.DB, 25))
	criteria := filters.FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(2)}
	canonical, err := api.GetLogs(m.Ctx, criteria, nil, nil)
	require.NoError(t, err)
	require.Len(t, canonical, 2)
	code := hexutil.Bytes{byte(vm.STOP)}
	overrides := ethapi.StateOverrides{accounts.InternAddress(forwarder): {Code: &code}}
	logs, err := api.GetLogs(m.Ctx, criteria, &overrides, nil)
	require.NoError(t, err)
	require.Len(t, logs, 2)
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

func TestCallConstructorFactoryRejectsFailedCreation(t *testing.T) {
	for _, tc := range []struct {
		name          string
		opcode        vm.OpCode
		frameTx       bool
		repeatCreate2 bool
		emptyRuntime  bool
	}{
		{name: "ordinary CREATE", opcode: vm.CREATE},
		{name: "ordinary CREATE2", opcode: vm.CREATE2},
		{name: "frame CREATE", opcode: vm.CREATE, frameTx: true},
		{name: "frame CREATE2", opcode: vm.CREATE2, frameTx: true},
		{name: "ordinary repeated CREATE2", opcode: vm.CREATE2, repeatCreate2: true},
		{name: "ordinary empty repeated CREATE2", opcode: vm.CREATE2, repeatCreate2: true, emptyRuntime: true},
		{name: "frame empty repeated CREATE2", opcode: vm.CREATE2, frameTx: true, repeatCreate2: true, emptyRuntime: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
			require.NoError(t, err)
			sender := crypto.PubkeyToAddress(key.PublicKey)
			factory := common.HexToAddress("0x2000")
			initCode := program.New().ReturnData([]byte{byte(vm.STOP)}).Bytes()
			create := program.New().Mstore(initCode, 0).Push(len(initCode)).Push0().Push0().Op(vm.CREATE)
			contract := types.CreateAddress(factory, 0)
			if tc.opcode == vm.CREATE2 {
				create = program.New().Create2(initCode, 0)
				contract = types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))
			}
			factoryCode := testutil.AssertStackWord(create.Op(vm.ISZERO), 0)
			if tc.repeatCreate2 {
				factoryCode.Create2(initCode, 0).Op(vm.POP)
			}
			factoryCode.Op(vm.STOP)
			cfg := chain.AllProtocolChanges.Copy()
			cfg.BogotaTime = common.NewUint64(0)
			var approval []byte
			if tc.frameTx {
				approval = program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE).Bytes()
			}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
				Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
				Alloc: types.GenesisAlloc{
					sender:  {Balance: big.NewInt(1_000_000_000_000_000_000), Code: approval},
					factory: {Code: factoryCode.Bytes()},
				},
			}), execmoduletester.WithKey(key))
			var txn types.Transaction
			if tc.frameTx {
				txn = &types.FrameTransaction{
					ChainID: *cfg.ChainID, NonceKeys: []uint256.Int{{}}, SenderAddress: sender,
					Fees: types.FrameFees{FeeCap: uint256.Int{1}},
					Frames: []types.Frame{
						{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
						{Mode: types.FrameSender, Target: &factory, Limits: mdgas.MdGas{Execution: 200_000, State: 1_000_000}},
					},
				}
			} else {
				txn, err = types.SignTx(types.NewTransaction(0, factory, new(uint256.Int), 500_000, uint256.NewInt(2_000_000_000), nil), *types.LatestSigner(cfg), key)
				require.NoError(t, err)
			}
			pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) { b.AddTx(txn) })
			require.NoError(t, err)
			require.NoError(t, m.InsertChain(pack))
			require.EqualValues(t, types.ReceiptStatusSuccessful, pack.Receipts[0][0].Status)
			base := newBaseApiForTest(m)
			api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: 2_000_000}, NewOtterscanAPI(base, m.DB, 25))
			want := hexutil.Bytes{byte(vm.PUSH0), byte(vm.STOP)}
			if tc.emptyRuntime {
				want = nil
			}
			result, err := api.CallConstructor(m.Ctx, contract, program.New().ReturnData(want).Bytes())
			require.NoError(t, err)
			if tc.emptyRuntime {
				require.Nil(t, result)
				return
			}
			require.NotNil(t, result, "successful replacement constructor must return its captured code")
			require.NotNil(t, result.Code)
			require.Equal(t, want, *result.Code)
		})
	}
}

func TestCallConstructorRetryAfterRollback(t *testing.T) {
	factory := common.HexToAddress("0x4000")
	wrapper := common.HexToAddress("0x3000")
	runtime := hexutil.Bytes{byte(vm.STOP)}
	initCode := program.New().ReturnData(runtime).Bytes()
	target := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))
	cfg := chain.AllProtocolChanges.Copy()
	cfg.BogotaTime = common.NewUint64(0)

	checkRetry := func(t *testing.T, alloc types.GenesisAlloc, txn types.Transaction) {
		t.Helper()
		deploy := program.New().Mstore(initCode, 0).Push0().Push(len(initCode)).Push0().Op(vm.CALLVALUE, vm.CREATE2, vm.POP)
		alloc[factory] = types.GenesisAccount{Code: deploy.Bytes()}
		m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
			Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1), Alloc: alloc,
		}), execmoduletester.WithKey(testKey))
		pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) { b.AddTx(txn) })
		require.NoError(t, err)
		require.NoError(t, m.InsertChain(pack))
		base := newBaseApiForTest(m)
		api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: 5_000_000}, NewOtterscanAPI(base, m.DB, 25))
		result, err := api.CallConstructor(m.Ctx, target, initCode)
		require.NoError(t, err)
		require.Equal(t, &CreationCode{Code: &runtime}, result)

		// Only the first, rolled-back deployment receives an endowment.
		replacement := testutil.AssertStackWord(program.New().Op(vm.CALLVALUE), 1).ReturnData(runtime)
		result, err = api.CallConstructor(m.Ctx, target, replacement.Bytes())
		require.Nil(t, result)
		require.ErrorIs(t, err, vm.ErrExecutionReverted)
	}

	t.Run("ancestor revert", func(t *testing.T) {
		entry := common.HexToAddress("0x2000")
		// The prefunded account still exists after the deployment rolls back.
		calls := program.New().Call(nil, target, 1, 0, 0, 0, 0).Op(vm.POP)
		calls.Call(nil, wrapper, 0, 0, 0, 0, 0).Op(vm.POP).Call(nil, factory, 0, 0, 0, 0, 0).Op(vm.POP)
		rollback := program.New().Call(nil, factory, 1, 0, 0, 0, 0).Op(vm.POP, vm.PUSH0, vm.PUSH0, vm.REVERT)
		txn, err := types.SignTx(types.NewTransaction(0, entry, new(uint256.Int), 2_000_000, uint256.NewInt(2_000_000_000), nil), *types.LatestSigner(cfg), testKey)
		require.NoError(t, err)
		checkRetry(t, types.GenesisAlloc{
			testAddr: {Balance: big.NewInt(1_000_000_000_000_000_000)},
			entry:    {Balance: big.NewInt(1), Code: calls.Bytes()},
			wrapper:  {Balance: big.NewInt(1), Code: rollback.Bytes()},
		}, txn)
	})
	t.Run("atomic batch revert", func(t *testing.T) {
		limits := mdgas.MdGas{Execution: 200_000, State: 1_000_000}
		txn := &types.FrameTransaction{
			ChainID: *cfg.ChainID, NonceKeys: []uint256.Int{{}}, SenderAddress: testAddr,
			Fees: types.FrameFees{FeeCap: uint256.Int{1}},
			Frames: []types.Frame{
				{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
				{Mode: types.FrameSender, Flags: types.FrameAtomicBatch, Target: &factory, Value: uint256.Int{1}, Limits: limits},
				{Mode: types.FrameSender, Target: &wrapper, Limits: limits},
				{Mode: types.FrameSender, Target: &factory, Limits: limits},
			},
		}
		approval := program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE)
		checkRetry(t, types.GenesisAlloc{
			testAddr: {Balance: big.NewInt(1_000_000_000_000_000_000), Code: approval.Bytes()},
			wrapper:  {Code: program.New().Push0().Push0().Op(vm.REVERT).Bytes()},
		}, txn)
	})
}

func TestCallConstructorRetryInsufficientBalance(t *testing.T) {
	entry := common.HexToAddress("0x2000")
	factory := common.HexToAddress("0x3000")
	runtime := hexutil.Bytes{byte(vm.STOP)}
	initCode := program.New().ReturnData(runtime).Bytes()
	target := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))

	// The factory accepts one byte of runtime code and uses the calldata length as the endowment.
	create := program.New().Mstore(initCode, 0).Push0().Push(len(initCode)).Push0().Op(vm.CALLDATASIZE, vm.CREATE2, vm.EXTCODESIZE)
	factoryCode := testutil.AssertStackWord(create, 1)
	calls := program.New().Call(nil, factory, 0, 0, 0, 0, 0).Op(vm.POP)
	calls.Call(nil, factory, 0, 0, 1, 0, 0).Op(vm.POP)

	for _, fork := range []struct {
		name   string
		config *chain.Config
	}{
		{"Osaka", chain.TestChainOsakaConfig},
		{"Amsterdam", chain.AllProtocolChanges},
	} {
		t.Run(fork.name, func(t *testing.T) {
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
				Config: fork.config.Copy(), GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
				Alloc: types.GenesisAlloc{
					testAddr: {Balance: big.NewInt(1_000_000_000_000_000_000)},
					entry:    {Code: calls.Bytes()},
					factory:  {Code: factoryCode.Bytes()},
				},
			}), execmoduletester.WithKey(testKey))
			txn, err := types.SignTx(types.NewTransaction(0, entry, new(uint256.Int), 2_000_000, uint256.NewInt(2_000_000_000), nil), *types.LatestSigner(m.ChainConfig), testKey)
			require.NoError(t, err)
			pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) { b.AddTx(txn) })
			require.NoError(t, err)
			require.NoError(t, m.InsertChain(pack))
			base := newBaseApiForTest(m)
			ots := NewOtterscanAPI(base, m.DB, 25)
			creator, err := ots.GetContractCreator(m.Ctx, target)
			require.NoError(t, err)
			require.Equal(t, &ContractCreatorData{Tx: txn.Hash(), Creator: factory}, creator)
			api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: 5_000_000}, ots)
			result, err := api.CallConstructor(m.Ctx, target, initCode)
			require.NoError(t, err)
			require.Equal(t, &CreationCode{Code: &runtime}, result)

			for _, tc := range []struct {
				name        string
				replacement []byte
			}{
				{"first deployment rolled back", program.New().Return(0, 2).Bytes()},
				{"first constructor reverted", program.New().Push0().Push0().Op(vm.REVERT).Bytes()},
			} {
				t.Run(tc.name, func(t *testing.T) {
					result, err := api.CallConstructor(m.Ctx, target, tc.replacement)
					require.Nil(t, result)
					require.ErrorIs(t, err, vm.ErrInsufficientBalance)
				})
			}
		})
	}
}

func TestCallConstructorRetryOutOfGas(t *testing.T) {
	entry := common.HexToAddress("0x2000")
	factory := common.HexToAddress("0x3000")
	otherFactory := common.HexToAddress("0x4000")
	runtime := hexutil.Bytes{byte(vm.STOP)}
	initCode := program.New().ReturnData(runtime).Bytes()
	for _, tc := range []struct {
		name        string
		op          vm.OpCode
		secondCall  common.Address
		inputSize   int
		retryTarget bool
	}{
		{"CREATE retry", vm.CREATE, factory, 0, true},
		{"CREATE other factory", vm.CREATE, otherFactory, 0, false},
		{"CREATE2 retry", vm.CREATE2, factory, 0, true},
		{"CREATE2 other salt", vm.CREATE2, factory, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			create := program.New().Mstore(initCode, 0)
			target := types.CreateAddress(factory, 0)
			if tc.op == vm.CREATE2 {
				create.Op(vm.CALLDATASIZE)
				target = types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))
			}
			create.Push(len(initCode)).Push0().Push0().Op(tc.op, vm.EXTCODESIZE)
			factoryCode := testutil.AssertStackWord(create, 1).Bytes()
			calls := program.New().Call(nil, factory, 0, 0, 0, 0, 0).Op(vm.POP)
			calls.Call(uint256.NewInt(1_000), tc.secondCall, 0, 0, tc.inputSize, 0, 0).Op(vm.POP)
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
				Config: chain.TestChainOsakaConfig.Copy(), GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
				Alloc: types.GenesisAlloc{
					testAddr:     {Balance: big.NewInt(1_000_000_000_000_000_000)},
					entry:        {Code: calls.Bytes()},
					factory:      {Code: factoryCode},
					otherFactory: {Code: factoryCode},
				},
			}), execmoduletester.WithKey(testKey))
			txn, err := types.SignTx(types.NewTransaction(0, entry, new(uint256.Int), 500_000, uint256.NewInt(2_000_000_000), nil), *types.LatestSigner(m.ChainConfig), testKey)
			require.NoError(t, err)
			pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) { b.AddTx(txn) })
			require.NoError(t, err)
			require.NoError(t, m.InsertChain(pack))
			client := newOverlayRPCClient(t, m)

			var result *CreationCode
			require.NoError(t, client.CallContext(t.Context(), &result, "overlay_callConstructor", target, hexutil.Bytes(initCode)))
			require.Equal(t, &CreationCode{Code: &runtime}, result)
			for _, first := range []struct {
				name string
				code []byte
				err  error
			}{
				{"deployment rolled back", program.New().Return(0, 2).Bytes(), nil},
				{"constructor reverted", program.New().Push0().Push0().Op(vm.REVERT).Bytes(), vm.ErrExecutionReverted},
			} {
				t.Run(first.name, func(t *testing.T) {
					var result *CreationCode
					err := client.CallContext(t.Context(), &result, "overlay_callConstructor", target, hexutil.Bytes(first.code))
					require.Nil(t, result)
					switch {
					case tc.retryTarget:
						require.EqualError(t, err, vm.ErrOutOfGas.Error())
					case first.err != nil:
						require.EqualError(t, err, first.err.Error())
					default:
						require.NoError(t, err)
					}
				})
			}
		})
	}
}

func TestCallConstructorPreservesSiblingCreate(t *testing.T) {
	factory := common.HexToAddress("0x2000")
	helper := types.CreateAddress(factory, 0)
	target := types.CreateAddress(factory, 1)
	initCode := program.New().ReturnData([]byte{byte(vm.ADD)}).Bytes()
	create := program.New().Push(len(initCode)).Push0().Push0().Op(vm.CREATE, vm.POP).Bytes()
	factoryCode := program.New().Mstore(initCode, 0).Append(create).Append(create)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: chain.TestChainOsakaConfig.Copy(), GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
		Alloc: types.GenesisAlloc{
			testAddr: {Balance: big.NewInt(1_000_000_000_000_000_000)},
			factory:  {Code: factoryCode.Bytes()},
		},
	}), execmoduletester.WithKey(testKey))
	txn, err := types.SignTx(types.NewTransaction(0, factory, new(uint256.Int), 500_000, uint256.NewInt(2_000_000_000), nil), *types.LatestSigner(m.ChainConfig), testKey)
	require.NoError(t, err)
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) { b.AddTx(txn) })
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	base := newBaseApiForTest(m)
	api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: 2_000_000}, NewOtterscanAPI(base, m.DB, 25))

	replacement := program.New().ExtcodeCopy(helper, 0, 0, 1).Return(0, 2)
	result, err := api.CallConstructor(m.Ctx, target, replacement.Bytes())
	require.NoError(t, err)
	want := hexutil.Bytes{byte(vm.ADD), byte(vm.STOP)}
	require.Equal(t, &CreationCode{Code: &want}, result)
}

func TestCallConstructorNestedInitCodeErrors(t *testing.T) {
	entry, factory := common.HexToAddress("0x2000"), common.HexToAddress("0x3000")
	initCode := program.New().ReturnData([]byte{byte(vm.STOP)}).Bytes()
	for _, op := range []vm.OpCode{vm.CREATE, vm.CREATE2} {
		t.Run(op.String(), func(t *testing.T) {
			create := program.New().Mstore(initCode, 0).Push(len(initCode)).Push0().Push0().Op(vm.CREATE)
			target := types.CreateAddress(factory, 0)
			if op == vm.CREATE2 {
				create = program.New().Create2(initCode, 0)
				target = types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))
			}
			// The entry contract ignores a failed factory call. Its gas allowance
			// covers the original initcode and leaves little room for a replacement.
			call := program.New().Call(uint256.NewInt(params.CreateGas+700), factory, 0, 0, 0, 0, 0).Op(vm.POP, vm.STOP)
			factoryCode := testutil.AssertStackWord(create.Op(vm.ISZERO), 0).Op(vm.STOP)
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
				Config: chain.TestChainOsakaConfig.Copy(), GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
				Alloc: types.GenesisAlloc{
					testAddr: {Balance: big.NewInt(1_000_000_000_000_000_000)},
					entry:    {Code: call.Bytes()},
					factory:  {Code: factoryCode.Bytes()},
				},
			}), execmoduletester.WithKey(testKey))
			txn, err := types.SignTx(types.NewTransaction(0, entry, new(uint256.Int), 500_000, uint256.NewInt(2_000_000_000), nil), *types.LatestSigner(m.ChainConfig), testKey)
			require.NoError(t, err)
			pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) { b.AddTx(txn) })
			require.NoError(t, err)
			require.NoError(t, m.InsertChain(pack))
			base := newBaseApiForTest(m)
			api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: 2_000_000}, NewOtterscanAPI(base, m.DB, 25))
			result, err := api.CallConstructor(m.Ctx, target, initCode)
			require.NoError(t, err)
			want := hexutil.Bytes{byte(vm.STOP)}
			require.Equal(t, &CreationCode{Code: &want}, result)

			for _, tc := range []struct {
				name        string
				replacement []byte
				wantErr     error
			}{
				{"empty initcode", nil, nil},
				{"size limit", common.RightPadBytes(initCode, params.MaxInitCodeSize+1), vm.ErrMaxInitCodeSizeExceeded},
				{"analysis gas", common.RightPadBytes(initCode, 32*1024), vm.ErrOutOfGas},
				{"code deposit gas", program.New().Return(0, 100).Bytes(), vm.ErrCodeStoreOutOfGas},
				{"constructor fault before factory failure", []byte{byte(vm.INVALID)}, &vm.ErrInvalidOpCode{}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					result, err := api.CallConstructor(m.Ctx, target, tc.replacement)
					require.Nil(t, result)
					require.ErrorIs(t, err, tc.wantErr)
				})
			}
		})
	}
}

func TestCallConstructorSelfdestruct(t *testing.T) {
	factory, destroyer := common.HexToAddress("0x2000"), common.HexToAddress("0x3000")
	initCode := program.New().ReturnData([]byte{byte(vm.STOP)}).Bytes()
	target := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: chain.AllProtocolChanges.Copy(), GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
		Alloc: types.GenesisAlloc{
			testAddr:  {Balance: big.NewInt(1_000_000_000_000_000_000)},
			factory:   {Code: program.New().Create2ThenCall(initCode, 0).Bytes()},
			destroyer: {Code: program.New().Op(vm.ADDRESS, vm.SELFDESTRUCT).Bytes()},
		},
	}), execmoduletester.WithKey(testKey))
	txn, err := types.SignTx(types.NewTransaction(0, factory, new(uint256.Int), 1_000_000, uint256.NewInt(2_000_000_000), nil), *types.LatestSigner(m.ChainConfig), testKey)
	require.NoError(t, err)
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) { b.AddTx(txn) })
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	base := newBaseApiForTest(m)
	api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: 2_000_000}, NewOtterscanAPI(base, m.DB, 25))
	call := program.New().DelegateCall(nil, destroyer, 0, 0, 0, 0).Bytes()

	t.Run("deletion survives", func(t *testing.T) {
		result, err := api.CallConstructor(m.Ctx, target, program.New().ReturnData(call).Bytes())
		require.NoError(t, err)
		require.Nil(t, result)
	})
	t.Run("deletion is reverted", func(t *testing.T) {
		runtime := hexutil.Bytes(program.New().Append(call).Push0().Push0().Op(vm.REVERT).Bytes())
		result, err := api.CallConstructor(m.Ctx, target, program.New().ReturnData(runtime).Bytes())
		require.NoError(t, err)
		require.Equal(t, &CreationCode{Code: &runtime}, result)
	})
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
		ChainID: *cfg.ChainID, NonceKeys: []uint256.Int{{}}, SenderAddress: sender,
		Fees: types.FrameFees{FeeCap: uint256.Int{1}},
		Frames: []types.Frame{
			{Mode: types.FrameDefault, Limits: mdgas.MdGas{Execution: 200_000, State: 1_000_000}},
			{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Data: []byte{1}, Limits: mdgas.MdGas{Execution: 1_000}},
		},
	}
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

func TestFrameCallConstructorPaymentApprovalError(t *testing.T) {
	cfg := chain.AllProtocolChanges.Copy()
	cfg.BogotaTime = common.NewUint64(0)
	sender := common.HexToAddress("0x1000")
	initCode := program.New().ReturnData([]byte{byte(vm.STOP)}).Bytes()
	create := program.New().Mstore(initCode, 0).Push(len(initCode)).Push0().Push0().Op(vm.CREATE, vm.EXTCODESIZE)
	approval := testutil.AssertStackWord(create, 1).Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
		Alloc: types.GenesisAlloc{sender: {Balance: big.NewInt(1_000_000_000_000_000_000), Code: approval.Bytes()}},
	}))
	frameTx := &types.FrameTransaction{
		ChainID: *cfg.ChainID, NonceKeys: []uint256.Int{{}}, SenderAddress: sender,
		Fees: types.FrameFees{FeeCap: uint256.Int{1}},
		Frames: []types.Frame{
			{Mode: types.FrameDefault, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 500_000, State: 1_000_000}},
		},
	}
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) { b.AddTx(frameTx) })
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	base := newBaseApiForTest(m)
	api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: 2_000_000}, NewOtterscanAPI(base, m.DB, 25))
	target := types.CreateAddress(sender, 0)
	for _, tc := range []struct {
		op      vm.OpCode
		wantErr string
	}{
		{vm.INVALID, "invalid opcode: INVALID"},
		{vm.STOP, "frame transaction without payment approval"},
	} {
		t.Run(tc.op.String(), func(t *testing.T) {
			result, err := api.CallConstructor(m.Ctx, target, hexutil.Bytes{byte(tc.op)})
			require.EqualError(t, err, tc.wantErr)
			require.Nil(t, result)
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
		ChainID: *cfg.ChainID, NonceKeys: []uint256.Int{{}}, SenderAddress: sender,
		Fees: types.FrameFees{FeeCap: uint256.Int{1}},
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
			{Mode: types.FrameSender, Target: &factory, Limits: mdgas.MdGas{Execution: 200_000, State: 1_000_000}},
		},
	}
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

func TestFrameCallConstructorAssertionRollback(t *testing.T) {
	for _, tc := range []struct {
		name             string
		createInApproval bool
	}{
		{name: "body creation is reverted"},
		{name: "payment approval creation survives", createInApproval: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := chain.AllProtocolChanges.Copy()
			cfg.BogotaTime = common.NewUint64(0)
			sender, factory, assertion := common.HexToAddress("0x1000"), common.HexToAddress("0x2000"), common.HexToAddress("0x3000")
			contract := types.CreateAddress(factory, 0)
			initCode := program.New().ReturnData([]byte{byte(vm.STOP)}).Bytes()
			create := program.New().Mstore(initCode, 0).Push(len(initCode)).Push0().Push0().Op(vm.CREATE, vm.POP, vm.STOP)
			assertCode := testutil.AssertStackWord(program.New().Push(contract).Op(vm.EXTCODESIZE), 1).Op(vm.STOP)
			approval := program.New().Push(types.FrameApproveBoth).Push0().Push0().Op(vm.APPROVE)
			frames := []types.Frame{
				{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
				{Mode: types.FrameSender, Target: &factory, Limits: mdgas.MdGas{Execution: 200_000, State: 1_000_000}},
				{Mode: types.FramePostTx, Target: &assertion, Limits: mdgas.MdGas{Execution: 10_000}},
			}
			if tc.createInApproval {
				approval = program.New().Call(uint256.NewInt(200_000), factory, 0, 0, 0, 0, 0).Op(vm.POP).Append(approval.Bytes())
				frames = append(frames[:1], frames[2:]...)
				frames[0].Mode = types.FrameDefault
				frames[0].Limits = mdgas.MdGas{Execution: 300_000, State: 1_000_000}
			}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
				Config: cfg, GasLimit: 30_000_000, BaseFee: uint256.NewInt(1),
				Alloc: types.GenesisAlloc{
					sender:    {Balance: big.NewInt(1_000_000_000_000_000_000), Code: approval.Bytes()},
					factory:   {Code: create.Bytes()},
					assertion: {Code: assertCode.Bytes()},
				},
			}))
			frameTx := &types.FrameTransaction{
				ChainID: *cfg.ChainID, NonceKeys: []uint256.Int{{}}, SenderAddress: sender,
				Fees: types.FrameFees{FeeCap: uint256.Int{1}}, Frames: frames,
			}
			pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) { b.AddTx(frameTx) })
			require.NoError(t, err)
			require.NoError(t, m.InsertChain(pack))
			require.EqualValues(t, types.ReceiptStatusSuccessful, pack.Receipts[0][0].Status)
			require.EqualValues(t, types.ReceiptStatusSuccessful, pack.Receipts[0][0].FrameReceipts[len(frames)-1].Status)

			base := newBaseApiForTest(m)
			api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: 2_000_000}, NewOtterscanAPI(base, m.DB, 25))
			// The replacement changes the code size, so POST_TX rolls back the body.
			want := hexutil.Bytes{byte(vm.PUSH0), byte(vm.STOP)}
			result, err := api.CallConstructor(m.Ctx, contract, program.New().ReturnData(want).Bytes())
			require.NoError(t, err)
			if tc.createInApproval {
				require.NotNil(t, result)
				require.NotNil(t, result.Code)
				require.Equal(t, want, *result.Code)
			} else {
				require.Nil(t, result)
			}
		})
	}
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

func newOverlayRPCClient(t *testing.T, m *execmoduletester.ExecModuleTester) *rpc.Client {
	t.Helper()
	base := newBaseApiForTest(m)
	ots := struct{ OtterscanAPI }{NewOtterscanAPI(base, m.DB, 25)}
	api := NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: 1_000_000}, ots)
	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	t.Cleanup(server.Stop)
	require.NoError(t, server.RegisterName("overlay", api))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(client.Close)
	return client
}

func TestCallConstructorRPCErrors(t *testing.T) {
	m, _, contractAddress, _ := chainWithDeployedContract(t)
	client := newOverlayRPCClient(t, m)
	revert := hexutil.Bytes(program.New().Push(0).Push(0).Op(vm.REVERT).Bytes())
	for _, tc := range []struct {
		name        string
		params      []any
		wantCode    int
		wantMessage string
	}{
		{"omitted code", []any{contractAddress}, rpc.ErrCodeInvalidParams, "missing value for required argument 1"},
		{"null code", []any{contractAddress, nil}, rpc.ErrCodeInvalidParams, "missing value for required argument 1"},
		{"invalid opcode", []any{contractAddress, hexutil.Bytes{byte(vm.INVALID)}}, rpc.ErrCodeDefault, "invalid opcode: INVALID"},
		{"constructor revert", []any{contractAddress, revert}, rpc.ErrCodeDefault, "execution reverted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var result json.RawMessage
			err := client.CallContext(t.Context(), &result, "overlay_callConstructor", tc.params...)
			var rpcErr rpc.Error
			require.ErrorAs(t, err, &rpcErr)
			require.EqualError(t, err, tc.wantMessage)
			require.Equal(t, tc.wantCode, rpcErr.ErrorCode())
		})
	}
}
