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

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
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

	latestExecuted, _, _, err := rpchelper.GetBlockNumber(m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.LatestExecutedBlockNumber), tx, api._blockReader, nil)
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
}

// Without overrides an originally failed txn must keep being skipped, even one
// that would succeed on replay (the replay raises its gas limit to the gas cap,
// so an out-of-gas failure would flip to success and emit a spurious log).
func TestOverlayGetLogsSkipsFailedTxWithoutOverrides(t *testing.T) {
	target := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	logCode := []byte{byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.LOG0)}

	setup := newOverlayGetLogsTestSetup(t, map[common.Address][]byte{target: logCode}, target, 21_100, target)

	logs, err := setup.api.GetLogs(setup.m.Ctx, filters.FilterCriteria{
		FromBlock: setup.blockNumber,
		ToBlock:   setup.blockNumber,
		Addresses: []common.Address{target},
	}, nil, setup.rules)
	require.NoError(t, err, "nonce accounting after the skip must let the next txn of the same sender replay")
	require.Len(t, logs, 1, "only the successful control transaction should emit a log")
	require.Equal(t, hexutil.Uint(1), logs[0].TxIndex, "the log must come from the control transaction, not from a gas-capped replay of the failed one")
}

func TestCallConstructorReturnsOverlayError(t *testing.T) {
	m, _, contractAddress, _ := chainWithDeployedContract(t)
	base := newBaseApiForTest(m)
	api := NewOverlayAPI(
		base,
		m.DB,
		&rpccfg.OverlayApiConfig{GasCap: 1_000_000},
		NewOtterscanAPI(base, m.DB, 25),
	)
	code := hexutil.Bytes{byte(vm.INVALID)}
	result, err := api.CallConstructor(m.Ctx, contractAddress, &code)
	require.Nil(t, result)
	var invalidOpcode *vm.ErrInvalidOpCode
	require.ErrorAs(t, err, &invalidOpcode)
}
