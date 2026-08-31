// Copyright 2024 The Erigon Authors
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
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func TestEstimateGas(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTestEthAPIWithFilters(t, m)
	var from = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	_, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From: &from,
		To:   &to,
	}, nil, nil, nil)
	require.NoError(t, err)
}

// TestEstimateGasBlockOverridesGasLimit verifies that blockOverrides.gasLimit is
// used as the binary-search ceiling rather than the on-chain header gas limit.
// A contract call is used to bypass the plain-transfer short-circuit path.
func TestEstimateGasBlockOverridesGasLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, bankAddr, contractAddr, _ := chainWithDeployedContract(t)
	api := newTestEthAPIWithFilters(t, m)

	callData := hexutil.Bytes(contractInvocationData(1))

	// Sanity check: without overrides the estimation succeeds.
	_, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From: &bankAddr,
		To:   &contractAddr,
		Data: &callData,
	}, nil, nil, nil)
	require.NoError(t, err)

	// Override gasLimit to below intrinsic gas (21000). The binary search ceiling
	// becomes 20999, so execution must fail regardless of the actual gas needed.
	lowGasLimit := hexutil.Uint64(params.TxGas - 1)
	_, err = api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From: &bankAddr,
		To:   &contractAddr,
		Data: &callData,
	}, nil, nil, &ethapi.BlockOverrides{GasLimit: &lowGasLimit})
	require.EqualError(t, err, fmt.Sprintf("gas required exceeds allowance (%d)", params.TxGas-1))
}

func TestEstimateGasBlockOverridesBlobBaseFee(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, bankAddr, contractAddr, _ := chainWithDeployedContractAndConfig(t, chain.AllProtocolChanges)
	api := newTestEthAPIWithFilters(t, m)

	callData := hexutil.Bytes(contractInvocationData(1))
	blobFeeCap := (*hexutil.U256)(uint256.NewInt(10))
	blobBaseFee := (*hexutil.U256)(uint256.NewInt(11))
	args := &ethapi.CallArgs{
		From:                &bankAddr,
		To:                  &contractAddr,
		Data:                &callData,
		MaxFeePerBlobGas:    blobFeeCap,
		BlobVersionedHashes: []common.Hash{{1}},
	}

	_, err := api.EstimateGas(context.Background(), args, nil, nil, nil)
	require.NoError(t, err)

	_, err = api.EstimateGas(context.Background(), args, nil, nil, &ethapi.BlockOverrides{BlobBaseFee: blobBaseFee})
	require.ErrorIs(t, err, protocol.ErrMaxFeePerBlobGas)
}

func TestEstimateGasBlockOverridesBlobBaseFeeSkipsZeroBlobFeeCap(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	m, bankAddr, contractAddr, _ := chainWithDeployedContractAndConfig(t, chain.AllProtocolChanges)
	api := newTestEthAPIWithFilters(t, m)

	callData := hexutil.Bytes(contractInvocationData(1))
	_, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From:                &bankAddr,
		To:                  &contractAddr,
		Data:                &callData,
		BlobVersionedHashes: []common.Hash{{1}},
	}, nil, nil, &ethapi.BlockOverrides{
		BlobBaseFee: (*hexutil.U256)(uint256.NewInt(11)),
	})
	require.NoError(t, err)
}

// TestEstimateGasEIP2780SubTxGasTransfers verifies that eth_estimateGas returns
// the true EIP-2780 cost for transfers that are cheaper than the legacy 21000,
// rather than clamping up to it: a self-transfer costs only TX_BASE (12000), and
// a zero-value no-data call to a distinct existing account costs TX_BASE +
// COLD_ACCOUNT_ACCESS (15000).
func TestEstimateGasEIP2780SubTxGasTransfers(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	m, bankAddr, _, receiverAddr := chainWithDeployedContractAndConfig(t, chain.AllProtocolChanges)
	api := newTestEthAPIWithFilters(t, m)

	// Self-transfer: TX_BASE only (12000), not clamped up to the legacy 21000.
	selfGas, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From: &bankAddr,
		To:   &bankAddr,
	}, nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, hexutil.Uint64(12_000), selfGas)

	// Zero-value no-data call to a distinct existing account: TX_BASE +
	// COLD_ACCOUNT_ACCESS (15000).
	distinctGas, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From: &bankAddr,
		To:   &receiverAddr,
	}, nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, hexutil.Uint64(15_000), distinctGas)
}

// gasGuardCode succeeds only while more than 10000 gas remains, so its minimum
// viable gas limit is far above the gas a single unconstrained trial reports.
//
//	GAS; PUSH2 10000; LT; PUSH1 9; JUMPI; INVALID; JUMPDEST; STOP
var gasGuardCode = hexutil.Bytes(hexutil.MustDecode("0x5a61271010600957fe5b00"))

// TestEstimateGasStateOverrideFundsSender verifies the balance recap reads the
// overridden balance: a sender funded only by an override must not be rejected
// nor capped to an unusable allowance.
func TestEstimateGasStateOverrideFundsSender(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, _, _, receiverAddr := chainWithDeployedContract(t)
	api := newTestEthAPIWithFilters(t, m)

	poor := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	balance := (*hexutil.Big)(big.NewInt(1e18))
	args := &ethapi.CallArgs{
		From:         &poor,
		To:           &receiverAddr,
		Value:        (*hexutil.U256)(uint256.NewInt(1)),
		MaxFeePerGas: (*hexutil.U256)(uint256.NewInt(1e9)),
	}
	overrides := &ethapi.StateOverrides{
		accounts.InternAddress(poor): {Balance: &balance},
	}

	historical := rpc.BlockNumberOrHashWithNumber(4)
	for _, at := range []*rpc.BlockNumberOrHash{nil, &historical} {
		gas, err := api.EstimateGas(context.Background(), args, at, overrides, nil)
		require.NoError(t, err)
		require.Equal(t, hexutil.Uint64(params.TxGas), gas)
	}
}

// TestEstimateGasStateOverrideCodeSkipsTransferShortcut verifies recipient code
// supplied by an override keeps the estimate out of the codeless-transfer
// shortcut, whose single trial at the ceiling would under-report the minimum.
func TestEstimateGasStateOverrideCodeSkipsTransferShortcut(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, bankAddr, _, _ := chainWithDeployedContract(t)
	api := newTestEthAPIWithFilters(t, m)

	target := common.HexToAddress("0x00000000000000000000000000000000000000cc")
	overrides := &ethapi.StateOverrides{
		accounts.InternAddress(target): {Code: &gasGuardCode},
	}

	args := &ethapi.CallArgs{From: &bankAddr, To: &target}
	historical := rpc.BlockNumberOrHashWithNumber(4)
	for _, at := range []*rpc.BlockNumberOrHash{nil, &historical} {
		gas, err := api.EstimateGas(context.Background(), args, at, overrides, nil)
		require.NoError(t, err)
		require.Greater(t, uint64(gas), params.TxGas+10_000)

		// The estimate has to be usable: below the guard's threshold the code hits
		// INVALID.
		_, err = api.Call(context.Background(), ethapi.CallArgs{
			From: &bankAddr,
			To:   &target,
			Gas:  &gas,
		}, at, overrides, nil)
		require.NoError(t, err)
	}
}

func TestEstimateGasStateOverrideClearedCodeKeepsTransferShortcut(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, bankAddr, contractAddr, _ := chainWithDeployedContract(t)
	api := newTestEthAPIWithFilters(t, m)

	noCode := hexutil.Bytes{}
	gas, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From: &bankAddr,
		To:   &contractAddr,
	}, nil, &ethapi.StateOverrides{
		accounts.InternAddress(contractAddr): {Code: &noCode},
	}, nil)
	require.NoError(t, err)
	require.Equal(t, hexutil.Uint64(params.TxGas), gas)
}

// TestEstimateGasStateOverrideAppliedToEveryTrial verifies every binary-search
// trial starts from the same overridden state: writing a fresh slot costs 20000
// only on clean state, so a write leaking from an earlier trial would let the
// search settle below the true minimum.
func TestEstimateGasStateOverrideAppliedToEveryTrial(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, bankAddr, _, _ := chainWithDeployedContract(t)
	api := newTestEthAPIWithFilters(t, m)

	// PUSH1 1; PUSH1 0; SSTORE; STOP
	sstoreCode := hexutil.Bytes(hexutil.MustDecode("0x600160005500"))
	target := common.HexToAddress("0x00000000000000000000000000000000000000dd")

	gas, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From: &bankAddr,
		To:   &target,
	}, nil, &ethapi.StateOverrides{
		accounts.InternAddress(target): {Code: &sstoreCode},
	}, nil)
	require.NoError(t, err)
	require.Greater(t, uint64(gas), params.TxGas+20_000)
}

// TestEstimateGasStateOverrideLowersSenderBalance verifies the balance recap
// also honours an override that takes funds away: the fundable allowance has to
// cap the ceiling even when the committed balance would cover the call.
func TestEstimateGasStateOverrideLowersSenderBalance(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, bankAddr, contractAddr, _ := chainWithDeployedContract(t)
	api := newTestEthAPIWithFilters(t, m)

	const feePerGas = 1e9
	const allowance = 25_000 // below what the contract call needs
	callData := hexutil.Bytes(contractInvocationData(1))
	args := &ethapi.CallArgs{
		From:         &bankAddr,
		To:           &contractAddr,
		Data:         &callData,
		MaxFeePerGas: (*hexutil.U256)(uint256.NewInt(feePerGas)),
	}

	// Sanity check: the committed balance funds the call.
	_, err := api.EstimateGas(context.Background(), args, nil, nil, nil)
	require.NoError(t, err)

	poorBalance := (*hexutil.Big)(big.NewInt(feePerGas * allowance))
	_, err = api.EstimateGas(context.Background(), args, nil, &ethapi.StateOverrides{
		accounts.InternAddress(bankAddr): {Balance: &poorBalance},
	}, nil)
	require.EqualError(t, err, fmt.Sprintf("gas required exceeds allowance (%d)", allowance))
}

// TestEstimateGasStateOverrideErrorPrecedesFundsCheck verifies a rejected
// override is reported as such instead of surfacing as a funds error from a
// precheck that ran on the unmodified state.
func TestEstimateGasStateOverrideErrorPrecedesFundsCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, _, _, receiverAddr := chainWithDeployedContract(t)
	api := newTestEthAPIWithFilters(t, m)

	poor := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	notAPrecompile := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	moveTo := common.HexToAddress("0x00000000000000000000000000000000000000ee")

	_, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From:         &poor,
		To:           &receiverAddr,
		Value:        (*hexutil.U256)(uint256.NewInt(1)),
		MaxFeePerGas: (*hexutil.U256)(uint256.NewInt(1e9)),
	}, nil, &ethapi.StateOverrides{
		accounts.InternAddress(notAPrecompile): {MovePrecompileTo: &moveTo},
	}, nil)
	require.ErrorContains(t, err, "is not a precompile")
}

// TestEstimateGasStateOverrideMovedPrecompile verifies the prechecks read a
// state built with the precompile moves applied, without disturbing the
// precompiles the trials execute against.
func TestEstimateGasStateOverrideMovedPrecompile(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, bankAddr, _, _ := chainWithDeployedContract(t)
	api := newTestEthAPIWithFilters(t, m)

	ecrecover := common.HexToAddress("0x0000000000000000000000000000000000000001")
	moveTo := common.HexToAddress("0x00000000000000000000000000000000000000ee")
	input := hexutil.Bytes(make([]byte, 128))

	gas, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From: &bankAddr,
		To:   &moveTo,
		Data: &input,
	}, nil, &ethapi.StateOverrides{
		accounts.InternAddress(ecrecover): {MovePrecompileTo: &moveTo},
	}, nil)
	require.NoError(t, err)
	require.Greater(t, uint64(gas), params.TxGas+params.EcrecoverGas)
}

// TestEstimateGasCallDataFieldDoesNotChangeEstimate verifies that the same
// calldata estimates the same whether it arrives as "input" or as "data".
// Input wins in ToMessage, so keying the plain-transfer shortcut on args.Data
// alone lets the field the caller picked decide which estimation path runs.
func TestEstimateGasCallDataFieldDoesNotChangeEstimate(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, bankAddr, _, _ := chainWithDeployedContract(t)
	api := newTestEthAPIWithFilters(t, m)

	codeless := common.HexToAddress("0x00000000000000000000000000000000000000f1")
	payload := hexutil.Bytes{0xde, 0xad, 0xbe, 0xef, 0x00, 0x00}

	viaInput, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From: &bankAddr, To: &codeless, Input: &payload,
	}, nil, nil, nil)
	require.NoError(t, err)

	viaData, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From: &bankAddr, To: &codeless, Data: &payload,
	}, nil, nil, nil)
	require.NoError(t, err)

	require.Equal(t, viaData, viaInput)
}

// TestEstimateGasZeroFundableAllowance verifies that a sender whose funds cover
// the transfer but not a single unit of gas is told so, instead of being
// estimated at the gas cap: the balance recap caps the ceiling to zero, and the
// trial has to honour it.
func TestEstimateGasZeroFundableAllowance(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, _, _, receiverAddr := chainWithDeployedContract(t)
	api := newTestEthAPIWithFilters(t, m)

	poor := common.HexToAddress("0x00000000000000000000000000000000000000ab")
	dust := (*hexutil.Big)(big.NewInt(1000))
	_, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From:         &poor,
		To:           &receiverAddr,
		MaxFeePerGas: (*hexutil.U256)(uint256.NewInt(1e9)),
	}, nil, &ethapi.StateOverrides{
		accounts.InternAddress(poor): {Balance: &dust},
	}, nil)
	require.EqualError(t, err, "gas required exceeds allowance (0)")
}

// TestEstimateGasMissingValueCountsAsZero verifies that a request without a
// "value" field is checked against the sender's funds all the same: ToMessage
// resolves a missing value to zero, and the recap has to see the resolved one.
func TestEstimateGasMissingValueCountsAsZero(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m, _, _, receiverAddr := chainWithDeployedContract(t)
	api := newTestEthAPIWithFilters(t, m)

	broke := common.HexToAddress("0x00000000000000000000000000000000000000ac")
	_, err := api.EstimateGas(context.Background(), &ethapi.CallArgs{
		From:         &broke,
		To:           &receiverAddr,
		MaxFeePerGas: (*hexutil.U256)(uint256.NewInt(1e9)),
	}, nil, nil, nil)
	require.EqualError(t, err, "insufficient funds for transfer")
}

func TestEthCallBlockOverridesBaseFeeAffectsGasPrice(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	m, bankAddr, contractAddr, _ := chainWithDeployedContractAndConfig(t, chain.AllProtocolChanges)
	api := newTestEthAPIWithFilters(t, m)

	callData := hexutil.Bytes{0x3a, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
	result, err := api.Call(context.Background(), ethapi.CallArgs{
		From:                 &bankAddr,
		To:                   &contractAddr,
		Data:                 &callData,
		MaxFeePerGas:         (*hexutil.U256)(uint256.NewInt(100)),
		MaxPriorityFeePerGas: (*hexutil.U256)(uint256.NewInt(2)),
	}, nil, &ethapi.StateOverrides{
		accounts.InternAddress(contractAddr): {
			Code: &callData,
		},
	}, &ethapi.BlockOverrides{
		BaseFeePerGas: (*hexutil.U256)(uint256.NewInt(10)),
	})
	require.NoError(t, err)
	require.Equal(t, "0x000000000000000000000000000000000000000000000000000000000000000c", result.String())
}

func newTestEthAPIWithFilters(t *testing.T, m *execmoduletester.ExecModuleTester) *APIImpl {
	t.Helper()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, execmoduletester.New(t))
	mining := txpoolproto.NewMiningClient(conn)
	filters := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, mining, func() {}, m.Log, nil)
	return newEthApiForTest(newBaseApiWithFiltersForTest(filters, stateCache, m), m.DB, nil, nil)
}

type stubTxPoolClient struct{ txpoolproto.TxpoolClient }

func (stubTxPoolClient) Nonce(context.Context, *txpoolproto.NonceRequest, ...grpc.CallOption) (*txpoolproto.NonceReply, error) {
	return &txpoolproto.NonceReply{}, nil
}

func TestCreateAccessListContractCreationWithoutFromDoesNotPanic(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)

	var (
		res *accessListResult
		err error
	)
	require.NotPanics(t, func() {
		res, err = api.CreateAccessList(context.Background(), ethapi.CallArgs{}, nil, nil, nil)
	})
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestStateCallMethodsRejectPendingTag(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	base := newBaseApiForTest(m)
	api := newEthApiForTest(base, m.DB, stubTxPoolClient{}, nil)
	graphqlAPI := NewGraphQLAPI(base, m.DB, api, stubTxPoolClient{}, &rpccfg.GraphQLApiConfig{})
	ctx := context.Background()
	pending := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)

	t.Run("eth_call", func(t *testing.T) {
		_, err := api.Call(ctx, ethapi.CallArgs{}, &pending, nil, nil)
		require.ErrorIs(t, err, errPendingStateNotSupported)
	})

	t.Run("eth_createAccessList", func(t *testing.T) {
		_, err := api.CreateAccessList(ctx, ethapi.CallArgs{}, &pending, nil, nil)
		require.ErrorIs(t, err, errPendingStateNotSupported)
	})

	t.Run("graphql_call", func(t *testing.T) {
		_, err := graphqlAPI.Call(ctx, rpc.PendingBlockNumber, ethapi.CallArgs{})
		require.ErrorIs(t, err, errPendingStateNotSupported)
	})
}

// TestCreateAccessListTracesStorage covers a target that touches storage, which is
// the path where the tracer writes its contract sets. A plain value transfer never
// reaches it.
func TestCreateAccessListTracesStorage(t *testing.T) {
	m, bankAddress, contractAddress, _ := chainWithDeployedContractAndConfig(t, chain.AllProtocolChanges)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, stubTxPoolClient{}, nil)
	data := hexutil.Bytes(contractInvocationData(42))

	for _, from := range []*common.Address{&bankAddress, nil} {
		res, err := api.CreateAccessList(context.Background(), ethapi.CallArgs{
			From: from,
			To:   &contractAddress,
			Data: &data,
		}, nil, nil, nil)
		require.NoError(t, err)
		require.Empty(t, res.Error)
		require.Len(t, *res.Accesslist, 1)
		require.Equal(t, contractAddress, (*res.Accesslist)[0].Address)
		// store() writes slots 0x00..0x10.
		require.Len(t, (*res.Accesslist)[0].StorageKeys, 17)
	}
}

func TestCreateAccessList(t *testing.T) {
	m, bankAddress, contractAddress, receiverAddress := chainWithDeployedContractAndConfig(t, chain.AllProtocolChanges)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

	bankNonce, err := api.GetTransactionCount(context.Background(), bankAddress, &latest)
	require.NoError(t, err)

	storeData := hexutil.Bytes(contractInvocationData(42))
	// Init code that writes slot 0x81 and then reverts, so the tracer has to report
	// a slot the transaction did not keep.
	revertingInit := hexutil.Bytes(hexutil.MustDecode("0x608060806080608155fd"))

	t.Run("plain transfer touches nothing", func(t *testing.T) {
		res, err := api.CreateAccessList(context.Background(), ethapi.CallArgs{
			From: &bankAddress,
			To:   &receiverAddress,
		}, nil, nil, nil)
		require.NoError(t, err)
		require.Empty(t, res.Error)
		require.Empty(t, *res.Accesslist)
	})

	t.Run("call reports the touched contract and its slots", func(t *testing.T) {
		res, err := api.CreateAccessList(context.Background(), ethapi.CallArgs{
			From: &bankAddress,
			To:   &contractAddress,
			Data: &storeData,
		}, nil, nil, nil)
		require.NoError(t, err)
		require.Empty(t, res.Error)
		require.Len(t, *res.Accesslist, 1)
		require.Equal(t, contractAddress, (*res.Accesslist)[0].Address)
		// store() writes slots 0x00..0x10.
		require.Len(t, (*res.Accesslist)[0].StorageKeys, 17)
	})

	t.Run("reverting creation still reports its slot", func(t *testing.T) {
		res, err := api.CreateAccessList(context.Background(), ethapi.CallArgs{
			From:  &bankAddress,
			Nonce: bankNonce,
			Data:  &revertingInit,
		}, nil, nil, nil)
		require.NoError(t, err)
		require.Equal(t, vm.ErrExecutionReverted.Error(), res.Error)
		require.Len(t, *res.Accesslist, 1)
		require.Equal(t, types.CreateAddress(bankAddress, uint64(*bankNonce)), (*res.Accesslist)[0].Address)
		require.Equal(t, []common.Hash{common.HexToHash("0x81")}, (*res.Accesslist)[0].StorageKeys)
	})

	t.Run("fee below the block base fee is rejected", func(t *testing.T) {
		gasPrice := (*hexutil.U256)(uint256.NewInt(1))
		_, err := api.CreateAccessList(context.Background(), ethapi.CallArgs{
			From:     &bankAddress,
			To:       &receiverAddress,
			GasPrice: gasPrice,
		}, nil, nil, nil)
		require.ErrorContains(t, err, "fee cap less than block base fee")
	})

	t.Run("sender and precompiles are excluded", func(t *testing.T) {
		identity := common.BytesToAddress([]byte{4})
		// CALL(gas, addr, value=0, argsOffset=0, argsLen=0, retOffset=0, retLen=0); POP
		callTo := func(addr common.Address) []byte {
			code := []byte{
				byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.PUSH1), 0,
				byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.PUSH20),
			}
			code = append(code, addr[:]...)
			return append(code, byte(vm.PUSH3), 0x01, 0x86, 0xa0, byte(vm.CALL), byte(vm.POP))
		}
		data := hexutil.Bytes(append(callTo(identity), callTo(contractAddress)...))

		res, err := api.CreateAccessList(context.Background(), ethapi.CallArgs{
			From:  &bankAddress,
			Nonce: bankNonce,
			Data:  &data,
		}, nil, nil, nil)
		require.NoError(t, err)
		require.Empty(t, res.Error)

		got := make([]common.Address, 0, len(*res.Accesslist))
		for _, tuple := range *res.Accesslist {
			got = append(got, tuple.Address)
		}
		require.Contains(t, got, contractAddress, "a plain call target belongs in the list")
		require.NotContains(t, got, bankAddress, "the sender is pre-warmed by EIP-2929")
		require.NotContains(t, got, identity, "precompiles are pre-warmed by EIP-2929")
	})

	t.Run("berlin plain transfer costs the intrinsic gas", func(t *testing.T) {
		mBerlin, bank, _, recv := chainWithDeployedContract(t)
		apiBerlin := newEthApiForTest(newBaseApiForTest(mBerlin), mBerlin.DB, nil, nil)
		res, err := apiBerlin.CreateAccessList(context.Background(), ethapi.CallArgs{
			From: &bank,
			To:   &recv,
		}, nil, nil, nil)
		require.NoError(t, err)
		require.Empty(t, *res.Accesslist)
		require.Equal(t, hexutil.Uint64(params.TxGas), res.GasUsed)
	})
}

// TestCreateAccessListConvergesOnCleanState pins that every convergence iteration
// starts from the pre-state. Seeding the converged list makes the run execute
// exactly once, which is the oracle for the multi-iteration run beside it.
func TestCreateAccessListConvergesOnCleanState(t *testing.T) {
	m, bankAddress, contractAddress, _ := chainWithDeployedContract(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	data := hexutil.Bytes(contractInvocationData(42))
	args := ethapi.CallArgs{From: &bankAddress, To: &contractAddress, Data: &data}

	converged, err := api.CreateAccessList(context.Background(), args, nil, nil, nil)
	require.NoError(t, err)
	require.Empty(t, converged.Error)
	require.NotEmpty(t, *converged.Accesslist, "store() must touch storage, else this converges in one pass and proves nothing")

	seeded := args
	seeded.AccessList = converged.Accesslist
	single, err := api.CreateAccessList(context.Background(), seeded, nil, nil, nil)
	require.NoError(t, err)
	require.Empty(t, single.Error)
	require.Equal(t, *single.Accesslist, *converged.Accesslist)
	require.Equal(t, single.GasUsed, converged.GasUsed)
}

func TestEthCallNonCanonical(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(nil, stateCache, m), m.DB, nil, nil)
	var from = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	var to = common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	blockNumberOrHash := rpc.BlockNumberOrHashWithHash(common.HexToHash("0x3fcb7c0d4569fddc89cbea54b42f163e0c789351d98810a513895ab44b47020b"), true)
	var blockNumberOrHashRef = &blockNumberOrHash

	_, err := api.Call(context.Background(), ethapi.CallArgs{
		From: &from,
		To:   &to,
	}, blockNumberOrHashRef, nil, nil)
	require.EqualError(t, err, "hash 3fcb7c0d4569fddc89cbea54b42f163e0c789351d98810a513895ab44b47020b is not currently canonical")
}

func TestEthCallToPrunedBlock(t *testing.T) {
	pruneTo := uint64(3)
	ethCallBlockNumber := rpc.BlockNumber(2)

	m, bankAddress, contractAddress, _ := chainWithDeployedContract(t)
	doPrune(t, m.DB, pruneTo)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	callData := hexutil.MustDecode("0x2e64cec1")
	callDataBytes := hexutil.Bytes(callData)

	blockNumberOrHash := rpc.BlockNumberOrHashWithNumber(ethCallBlockNumber)
	var blockNumberOrHashRef = &blockNumberOrHash

	_, err := api.Call(context.Background(), ethapi.CallArgs{
		From: &bankAddress,
		To:   &contractAddress,
		Data: &callDataBytes,
	}, blockNumberOrHashRef, nil, nil)
	require.NoError(t, err)
}

func TestGetProof(t *testing.T) {
	var maxGetProofRewindBlockCount = 1   // Note, this is unsafe for parallel tests, but, this test is the only consumer for now
	statecfg.EnableHistoricalCommitment() // enable commitment history to test historical proofs
	m, bankAddr, contractAddr, receiverAddress := chainWithDeployedContract(t)
	cfg := &rpccfg.EthApiConfig{
		GasCap:                      5000000,
		FeeCap:                      ethconfig.Defaults.RPCTxFeeCap,
		ReturnDataLimit:             100_000,
		AllowUnprotectedTxs:         false,
		MaxGetProofRewindBlockCount: maxGetProofRewindBlockCount,
		SubscribeLogsChannelSize:    128,
		RpcTxSyncDefaultTimeout:     20 * time.Second,
		RpcTxSyncMaxTimeout:         1 * time.Minute,
	}
	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, cfg, log.New())

	key := func(b byte) hexutil.Bytes {
		result := common.Hash{}
		result[31] = b
		return result[:]
	}
	_ = bankAddr

	tests := []struct {
		name        string
		blockNum    uint64
		addr        common.Address
		storageKeys []hexutil.Bytes
		stateVal    uint64
		expectedErr string
	}{
		{
			name:     "genesisBlockEOA",
			addr:     bankAddr,
			blockNum: 0,
		},
		{
			name:     "genesisBlockNoAccount",
			addr:     common.HexToAddress("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead0"),
			blockNum: 0,
		},
		{
			name:     "currentBlockNoState",
			addr:     contractAddr,
			blockNum: 6,
		},
		{
			name:     "currentBlockEOA",
			addr:     bankAddr,
			blockNum: 6,
		},
		{
			name:     "currentBlockNoAccount",
			addr:     common.HexToAddress("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead0"),
			blockNum: 6,
		},
		{
			name:        "currentBlockWithState",
			addr:        contractAddr,
			blockNum:    6,
			storageKeys: []hexutil.Bytes{key(0), key(4), key(8), key(10)},
			stateVal:    2,
		},
		{
			name:        "currentBlockWithStateAndShortKeys",
			addr:        contractAddr,
			blockNum:    6,
			storageKeys: []hexutil.Bytes{{0x0}, {0x4}, {0x8}, {0x0a}},
			stateVal:    2,
		},
		{
			name:        "currentBlockWithMissingState",
			addr:        contractAddr,
			storageKeys: []hexutil.Bytes{hexutil.FromHex("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddead")},
			blockNum:    6,
			stateVal:    0,
		},
		{
			name:        "currentBlockEOAMissingState",
			addr:        bankAddr,
			storageKeys: []hexutil.Bytes{hexutil.FromHex("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddead")},
			blockNum:    6,
			stateVal:    0,
		},
		{
			name:        "currentBlockNoAccountMissingState",
			addr:        common.HexToAddress("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddead0"),
			storageKeys: []hexutil.Bytes{hexutil.FromHex("0xdeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddeaddead")},
			blockNum:    6,
			stateVal:    0,
		},
		{
			name:        "olderBlockWithState",
			addr:        contractAddr,
			blockNum:    2,
			storageKeys: []hexutil.Bytes{key(1), key(5), key(9), key(13)},
			stateVal:    1,
		},
		{
			name:     "notCreatedYetAccount",
			addr:     receiverAddress, // receiver address only starts existing at block 4
			blockNum: 3,
		},
		{
			name:     "createdAccountAtBlock", // account created at block 4, proof requested at block 4, latest=6
			addr:     receiverAddress,         // receiver address only starts existing at block 4
			blockNum: 4,
		},
		{
			name:     "createdAccountBlockAfter", // account created at block 4, proof requested at block 5, latest=6
			addr:     receiverAddress,            // receiver address only starts existing at block 4
			blockNum: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proof, err := api.GetProof(
				context.Background(),
				tt.addr,
				tt.storageKeys,
				bnhPtr(rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(tt.blockNum))),
			)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				require.Nil(t, proof)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, proof)

			tx, err := m.DB.BeginTemporalRo(context.Background())
			require.NoError(t, err)
			defer tx.Rollback()
			header, err := api.headerByNumber(context.Background(), rpc.BlockNumber(tt.blockNum), tx)
			require.NoError(t, err)

			require.Equal(t, tt.addr, proof.Address)
			err = trie.VerifyAccountProof(header.Root, proof)
			require.NoError(t, err)

			require.Len(t, proof.StorageProof, len(tt.storageKeys))
			for _, storageKey := range tt.storageKeys {
				found := false
				for _, storageProof := range proof.StorageProof {
					var proofKeyHashBytes, storageKeyBytes []byte
					proofKeyHashBytes = hexutil.FromHex(storageProof.Key)
					storageKeyBytes = storageKey
					if !bytes.Equal(proofKeyHashBytes, storageKeyBytes) {
						continue
					}
					found = true
					require.Equal(t, tt.stateVal, (*uint256.Int)(storageProof.Value).Uint64())
					err = trie.VerifyStorageProof(proof.StorageHash, storageProof)
					require.NoError(t, err)
				}
				require.True(t, found, "did not find storage proof for key=%x", storageKey)
			}
		})
	}
}

type missingHeaderBlockReader struct {
	dbservices.FullBlockReader
}

func (missingHeaderBlockReader) HeaderByNumber(context.Context, kv.Getter, uint64) (*types.Header, error) {
	return nil, nil
}

func TestGetProofMissingHeader(t *testing.T) {
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() {
		statecfg.Schema = previousSchema
	})

	m, bankAddr, _, _ := chainWithDeployedContract(t)
	base := newBaseApiForTest(m)
	base._blockReader = missingHeaderBlockReader{FullBlockReader: base._blockReader}
	api := newEthApiForTest(base, m.DB, nil, nil)

	proof, err := api.GetProof(
		context.Background(),
		bankAddr,
		nil,
		bnhPtr(rpc.BlockNumberOrHashWithNumber(6)),
	)
	require.EqualError(t, err, "header not found for block 6")
	require.Nil(t, proof)
}

func TestGetProofPinsReadSnapshot(t *testing.T) {
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() {
		statecfg.Schema = previousSchema
	})

	m, _, contractAddress, _ := chainWithDeployedContract(t)

	roTx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	publishedDomains, err := execctx.NewSharedDomains(m.Ctx, roTx, m.Log)
	require.NoError(t, err)
	defer publishedDomains.Close()

	storageKey := common.Hash{}
	compositeKey := make([]byte, 0, len(contractAddress)+len(storageKey))
	compositeKey = append(compositeKey, contractAddress[:]...)
	compositeKey = append(compositeKey, storageKey[:]...)
	require.NoError(t, publishedDomains.DomainPut(kv.StorageDomain, roTx, compositeKey, []byte{3}, 1, nil))

	stateCache := &execmodule.Cache{}
	stateCache.SetPublishedSD(func() *execctx.SharedDomains { return publishedDomains })
	base := newBaseApiForTest(m)
	base.stateCache = stateCache
	api := newEthApiForTest(base, m.DB, nil, nil)

	proof, err := api.getProof(
		m.Ctx,
		roTx,
		contractAddress,
		[]StorageKeysInfo{{Hash: storageKey, KeyLength: len(storageKey)}},
		6,
		true,
		log.New(),
	)
	require.NoError(t, err)
	require.NotNil(t, proof)
	require.Equal(t, uint64(2), (*uint256.Int)(proof.StorageProof[0].Value).Uint64())
}

func TestGetProofIgnoresNewerSharedBranchCache(t *testing.T) {
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() {
		statecfg.Schema = previousSchema
	})

	m, _, contractAddress, _ := chainWithDeployedContract(t)
	roTx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	provider, ok := roTx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok)
	branchCache := provider.BranchCache()
	require.NotNil(t, branchCache)
	branchCache.Clear()
	t.Cleanup(branchCache.Clear)

	rootKey := nibbles.HexToCompact(nil)
	_, snapshotTxNum, err := rawdbv3.TxNums.Last(roTx)
	require.NoError(t, err)
	newerRootBranch := make(commitment.BranchData, 4)
	branchCache.Put(rootKey, newerRootBranch, 0, snapshotTxNum+1)
	cachedRootBranch, _, ok := branchCache.Get(rootKey)
	require.True(t, ok)
	require.Equal(t, []byte(newerRootBranch), cachedRootBranch)

	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	storageKey := common.Hash{}
	proof, err := api.getProof(
		m.Ctx,
		roTx,
		contractAddress,
		[]StorageKeysInfo{{Hash: storageKey, KeyLength: len(storageKey)}},
		6,
		true,
		log.New(),
	)
	require.NoError(t, err)
	require.NotNil(t, proof)
	require.Equal(t, uint64(2), (*uint256.Int)(proof.StorageProof[0].Value).Uint64())
}

func TestGetProofGenesisPrunedCommitmentHistory(t *testing.T) {
	statecfg.EnableHistoricalCommitment()
	m, bankAddr, _, _ := chainWithDeployedContract(t)

	ctx := context.Background()
	tx, err := m.DB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	pruneTo, err := m.BlockReader.TxnumReader().Min(ctx, tx, 3)
	require.NoError(t, err)
	c, err := tx.RwCursorDupSort(kv.TblCommitmentHistoryKeys)
	require.NoError(t, err)
	defer c.Close()
	for {
		k, _, err := c.First()
		require.NoError(t, err)
		if k == nil || binary.BigEndian.Uint64(k) >= pruneTo {
			break
		}
		require.NoError(t, c.DeleteCurrentDuplicates())
	}
	c.Close()
	require.NoError(t, tx.Commit())

	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	proof, err := api.GetProof(ctx, bankAddr, nil, bnhPtr(rpc.BlockNumberOrHashWithNumber(0)))
	require.ErrorIs(t, err, state.PrunedError)
	require.Nil(t, proof)
}

func TestGetBlockByTimestampLatestTime(t *testing.T) {
	ctx := context.Background()
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	api := NewErigonAPI(newBaseApiForTest(m), m.DB, nil)

	latestBlock, err := m.BlockReader.CurrentBlock(tx)
	require.NoError(t, err)
	response, err := ethapi.RPCMarshalBlockDeprecated(latestBlock, true, false)
	require.NoError(t, err)

	if err == nil && rpc.BlockNumber(latestBlock.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	block, err := api.GetBlockByTimestamp(ctx, rpc.Timestamp(latestBlock.Time()), false)
	require.NoError(t, err)

	require.Equal(t, response["timestamp"], block["timestamp"])
	require.Equal(t, response["hash"], block["hash"])
}

func TestGetBlockByTimestampOldestTime(t *testing.T) {
	ctx := context.Background()
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	api := NewErigonAPI(newBaseApiForTest(m), m.DB, nil)

	oldestBlock, err := m.BlockReader.BlockByNumber(m.Ctx, tx, 0)
	require.NoError(t, err)

	response, err := ethapi.RPCMarshalBlockDeprecated(oldestBlock, true, false)
	require.NoError(t, err)

	if err == nil && rpc.BlockNumber(oldestBlock.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	block, err := api.GetBlockByTimestamp(ctx, rpc.Timestamp(oldestBlock.Time()), false)
	require.NoError(t, err)

	require.Equal(t, response["timestamp"], block["timestamp"])
	require.Equal(t, response["hash"], block["hash"])
}

func TestGetBlockByTimeHigherThanLatestBlock(t *testing.T) {
	ctx := context.Background()
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	api := NewErigonAPI(newBaseApiForTest(m), m.DB, nil)

	latestBlock, err := m.BlockReader.CurrentBlock(tx)
	require.NoError(t, err)

	response, err := ethapi.RPCMarshalBlockDeprecated(latestBlock, true, false)
	require.NoError(t, err)

	if err == nil && rpc.BlockNumber(latestBlock.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	block, err := api.GetBlockByTimestamp(ctx, rpc.Timestamp(latestBlock.Time()+999999999999), false)
	require.NoError(t, err)

	require.Equal(t, response["timestamp"], block["timestamp"])
	require.Equal(t, response["hash"], block["hash"])
}

func TestGetBlockByTimeMiddle(t *testing.T) {
	ctx := context.Background()
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	api := NewErigonAPI(newBaseApiForTest(m), m.DB, nil)

	currentHeader := rawdb.ReadCurrentHeader(tx)
	oldestHeader, err := api._blockReader.HeaderByNumber(ctx, tx, 0)
	require.NoError(t, err)
	require.NotNil(t, oldestHeader)

	middleNumber := (currentHeader.Number.Uint64() + oldestHeader.Number.Uint64()) / 2
	middleBlock, err := m.BlockReader.BlockByNumber(m.Ctx, tx, middleNumber)
	require.NoError(t, err)

	response, err := ethapi.RPCMarshalBlockDeprecated(middleBlock, true, false)
	require.NoError(t, err)

	if err == nil && rpc.BlockNumber(middleBlock.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	block, err := api.GetBlockByTimestamp(ctx, rpc.Timestamp(middleBlock.Time()), false)
	require.NoError(t, err)
	require.Equal(t, response["timestamp"], block["timestamp"])
	require.Equal(t, response["hash"], block["hash"])
}

func TestGetBlockByTimestamp(t *testing.T) {
	ctx := context.Background()
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	tx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	api := NewErigonAPI(newBaseApiForTest(m), m.DB, nil)

	highestBlockNumber := rawdb.ReadCurrentHeader(tx).Number
	pickedBlock, err := m.BlockReader.BlockByNumber(m.Ctx, tx, highestBlockNumber.Uint64()/3)
	require.NoError(t, err)
	require.NotNil(t, pickedBlock)
	response, err := ethapi.RPCMarshalBlockDeprecated(pickedBlock, true, false)
	require.NoError(t, err)

	if err == nil && rpc.BlockNumber(pickedBlock.NumberU64()) == rpc.PendingBlockNumber {
		// Pending blocks need to nil out a few fields
		for _, field := range []string{"hash", "nonce", "miner"} {
			response[field] = nil
		}
	}

	block, err := api.GetBlockByTimestamp(ctx, rpc.Timestamp(pickedBlock.Time()), false)
	require.NoError(t, err)

	require.Equal(t, response["timestamp"], block["timestamp"])
	require.Equal(t, response["hash"], block["hash"])
}

// contractHexString is the output of compiling the following solidity contract:
//
// pragma solidity ^0.8.0;
//
//	contract Box {
//	    uint256 private _value0x0;
//	    uint256 private _value0x1;
//	    uint256 private _value0x2;
//	    uint256 private _value0x3;
//	    uint256 private _value0x4;
//	    uint256 private _value0x5;
//	    uint256 private _value0x6;
//	    uint256 private _value0x7;
//	    uint256 private _value0x8;
//	    uint256 private _value0x9;
//	    uint256 private _value0xa;
//	    uint256 private _value0xb;
//	    uint256 private _value0xc;
//	    uint256 private _value0xd;
//	    uint256 private _value0xe;
//	    uint256 private _value0xf;
//	    uint256 private _value0x10;
//
//	    // Emitted when the stored value changes
//	    event ValueChanged(uint256 value);
//
//	    // Stores a new value in the contract
//	    function store(uint256 value) public {
//	        _value0x0 = value;
//	        _value0x1 = value;
//	        _value0x2 = value;
//	        _value0x3 = value;
//	        _value0x4 = value;
//	        _value0x5 = value;
//	        _value0x6 = value;
//	        _value0x7 = value;
//	        _value0x8 = value;
//	        _value0x9 = value;
//	        _value0xa = value;
//	        _value0xb = value;
//	        _value0xc = value;
//	        _value0xd = value;
//	        _value0xe = value;
//	        _value0xf = value;
//	        _value0x10 = value;
//	        emit ValueChanged(value);
//	    }
//
//	    // Reads the last stored value
//	    function retrieve() public view returns (uint256) {
//	        return _value0x0;
//	    }
//	}
//
// You may produce this hex string by saving the contract into a file
// Box.sol and invoking
//
//	solc Box.sol --bin --abi --optimize
//
// This contract is a slight modification of Box.sol to use more storage nodes
// and ensure the contract storage will contain at least 1 non-leaf node (by
// storing 17 values).
const contractHexString = "0x608060405234801561001057600080fd5b5061013f806100206000396000f3fe608060405234801561001057600080fd5b50600436106100365760003560e01c80632e64cec11461003b5780636057361d14610050575b600080fd5b60005460405190815260200160405180910390f35b61006361005e3660046100f0565b610065565b005b6000819055600181905560028190556003819055600481905560058190556006819055600781905560088190556009819055600a819055600b819055600c819055600d819055600e819055600f81905560108190556040518181527f93fe6d397c74fdf1402a8b72e47b68512f0510d7b98a4bc4cbdf6ac7108b3c599060200160405180910390a150565b60006020828403121561010257600080fd5b503591905056fea2646970667358221220031e17f1bd1d1dcbee088287a905b152410b180064c149763590a0bbc516d95e64736f6c63430008130033"

var contractFuncSelector = crypto.Keccak256([]byte("store(uint256)"))[:4]

// contractInvocationData returns data suitable for invoking the 'store'
// function of the contract in contractHexString, note
func contractInvocationData(val byte) []byte {
	return hexutil.MustDecode(fmt.Sprintf("0x%x00000000000000000000000000000000000000000000000000000000000000%02x", contractFuncSelector, val))
}

func generatePseudoRandomECDSAKey(rand io.Reader) (*ecdsa.PrivateKey, error) {
	return ecdsa.GenerateKey(crypto.S256(), rand)
}

func generatePseudoRandomECDSAKeyPairs(rand io.Reader, n int) ([]*ecdsa.PrivateKey, []*ecdsa.PublicKey, error) {
	privateKeys := make([]*ecdsa.PrivateKey, n)
	publicKeys := make([]*ecdsa.PublicKey, n)
	var err error
	for i := range n {
		privateKeys[i], err = generatePseudoRandomECDSAKey(rand)
		if err != nil {
			return nil, nil, err
		}
		publicKeys[i] = &privateKeys[i].PublicKey
	}
	return privateKeys, publicKeys, nil
}

func chainWithDeployedContract(t *testing.T) (*execmoduletester.ExecModuleTester, common.Address, common.Address, common.Address) {
	t.Helper()
	return chainWithDeployedContractAndConfig(t, chain.TestChainBerlinConfig)
}

// fundedBankGenesis returns a fresh ExecModuleTester whose genesis funds a
// bank account keyed by a fixed, well-known private key, under cfg.
func fundedBankGenesis(t *testing.T, cfg *chain.Config) (m *execmoduletester.ExecModuleTester, bankKey *ecdsa.PrivateKey, bankAddress common.Address) {
	t.Helper()

	bankKey, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	bankAddress = crypto.PubkeyToAddress(bankKey.PublicKey)

	bankFunds, ok := new(big.Int).SetString("100000000000000000000", 10)
	require.True(t, ok)

	chainConfig := new(chain.Config)
	require.NoError(t, copier.CopyWithOption(chainConfig, cfg, copier.Option{DeepCopy: true}))
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc:  types.GenesisAlloc{bankAddress: {Balance: bankFunds}},
	}
	if cfg.AmsterdamTime != nil {
		// EIP-2780 account-creating transfers cost 204600 (incl. 183600 NEW_ACCOUNT
		// state gas); MPT-filler blocks need the larger budget.
		gspec.GasLimit = 60_000_000
	}

	m = execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(bankKey))
	return m, bankKey, bankAddress
}

func chainWithDeployedContractAndConfig(t *testing.T, cfg *chain.Config) (*execmoduletester.ExecModuleTester, common.Address, common.Address, common.Address) {
	t.Helper()

	var (
		seed            = int64(12345)
		rng             = rand.New(rand.NewSource(seed)) // rng for filler accounts
		nFillerAccounts = 400                            // nr. of accounts to fill up MPT
		signer          = types.LatestSignerForChainID(nil)
		txFeeCap        = uint256.NewInt(1_000_000_000_000)
		contract        = hexutil.MustDecode(contractHexString)
	)
	m, bankKey, bankAddress := fundedBankGenesis(t, cfg)

	receiverKey, err := crypto.HexToECDSA("a71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f292")
	require.NoError(t, err)
	receiverAddress := crypto.PubkeyToAddress(receiverKey.PublicKey)

	transferGasLimit := uint64(21000)
	if cfg.AmsterdamTime != nil {
		// A value transfer that creates the recipient costs 21000 (value-transfer
		// intrinsic) + 183600 (EIP-2780 NEW_ACCOUNT state gas) = 204600;
		// fundedBankGenesis budgets the matching block gas under Amsterdam.
		transferGasLimit = 204_600
	}
	// accounts to fill up MPT
	_, fillerPublicKeys, err := generatePseudoRandomECDSAKeyPairs(rng, nFillerAccounts)
	require.NoError(t, err)

	db := m.DB

	var contractAddr common.Address

	chain, err := m.GenerateChain(6, func(i int, block *blockgen.BlockGen) {
		nonce := block.TxNonce(bankAddress)
		switch i {
		case 0:
			tx, err := types.SignTx(&types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    nonce,
					GasLimit: 1e6,
					Value:    uint256.Int{},
					Data:     contract,
				},
				GasPrice: *txFeeCap,
			}, *signer, bankKey)
			require.NoError(t, err)
			block.AddTx(tx)
			contractAddr = types.CreateAddress(bankAddress, nonce)
		case 1:
			txn, err := types.SignTx(&types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    nonce,
					To:       &contractAddr,
					GasLimit: 900000,
					Value:    uint256.Int{},
					Data:     contractInvocationData(1),
				},
				GasPrice: *txFeeCap,
			}, *signer, bankKey)
			require.NoError(t, err)
			block.AddTx(txn)
			// send txs to filler addresses, so that MPT may be populated ( populate only half in this block, to not exceed gas limit)
			nonce++
			for idx := 0; idx < nFillerAccounts/2; idx++ {
				transferAmount := big.NewInt(1e1)
				fillerAddress := crypto.PubkeyToAddress(*fillerPublicKeys[idx])
				txn, err := types.SignTx(&types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce:    nonce,
						To:       &fillerAddress,
						GasLimit: transferGasLimit,
						Value:    *uint256.MustFromBig(transferAmount),
					},
					GasPrice: *txFeeCap,
				}, *signer, bankKey)
				require.NoError(t, err)
				block.AddTx(txn)
				nonce++
			}
		case 2:
			txn, err := types.SignTx(&types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    nonce,
					To:       &contractAddr,
					GasLimit: 900000,
					Value:    uint256.Int{},
					Data:     contractInvocationData(2),
				},
				GasPrice: *txFeeCap,
			}, *signer, bankKey)
			require.NoError(t, err)
			block.AddTx(txn)
			// send txs to filler addresses, so that MPT may be populated
			// ( populate the second half in this block)
			nonce++
			for idx := nFillerAccounts / 2; idx < nFillerAccounts; idx++ {
				transferAmount := big.NewInt(1e1)
				fillerAddress := crypto.PubkeyToAddress(*fillerPublicKeys[idx])
				txn, err := types.SignTx(&types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce:    nonce,
						To:       &fillerAddress,
						GasLimit: transferGasLimit,
						Value:    *uint256.MustFromBig(transferAmount),
					},
					GasPrice: *txFeeCap,
				}, *signer, bankKey)
				require.NoError(t, err)
				block.AddTx(txn)
				nonce++
			}

		case 3:
			transferAmount := big.NewInt(1e2)
			txn, err := types.SignTx(&types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    nonce,
					To:       &receiverAddress,
					GasLimit: transferGasLimit,
					Value:    *uint256.MustFromBig(transferAmount),
				},
				GasPrice: *txFeeCap,
			}, *signer, bankKey)
			require.NoError(t, err)
			block.AddTx(txn)
		case 4:
			// empty block
		case 5:
			// empty block
		}
	})
	require.NoError(t, err)

	err = m.InsertChain(chain)
	require.NoError(t, err)

	ctx := context.Background()
	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	stateReader, err := rpchelper.CreateHistoryStateReader(ctx, tx, 1, 0, rawdbv3.TxNums)
	require.NoError(t, err)
	st := state.New(stateReader)
	defer st.Close()
	exist, err := st.Exist(accounts.InternAddress(contractAddr))
	require.NoError(t, err)
	assert.False(t, exist, "Contract should not exist at block #1")

	stateReader, err = rpchelper.CreateHistoryStateReader(ctx, tx, 2, 0, rawdbv3.TxNums)
	require.NoError(t, err)
	st = state.New(stateReader)
	defer st.Close()
	exist, err = st.Exist(accounts.InternAddress(contractAddr))
	require.NoError(t, err)
	assert.True(t, exist, "Contract should exist at block #2")

	// Confirm the filler transfers actually created their accounts: an
	// under-budgeted transfer silently OOGs (failed receipt, no panic) and leaves
	// the MPT unpopulated, which would defeat the point of the fillers.
	stateReader, err = rpchelper.CreateHistoryStateReader(ctx, tx, 6, 0, rawdbv3.TxNums)
	require.NoError(t, err)
	st = state.New(stateReader)
	defer st.Close()
	createdFillers := 0
	for _, pk := range fillerPublicKeys {
		exist, err := st.Exist(accounts.InternAddress(crypto.PubkeyToAddress(*pk)))
		require.NoError(t, err)
		if exist {
			createdFillers++
		}
	}
	require.Equal(t, nFillerAccounts, createdFillers, "all filler transfers should have created their accounts")

	return m, bankAddress, contractAddr, receiverAddress
}

func doPrune(t *testing.T, db kv.RwDB, pruneTo uint64) {
	ctx := context.Background()
	//logger := testlog.Logger(t, log.LvlCrit)
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()

	err = rawdb.PruneTableDupSort(tx, kv.TblAccountVals, "", pruneTo, logEvery, ctx)
	require.NoError(t, err)

	// kv.StorageChangeSetDeprecated is no longer part of the active
	// schema (drop_legacy_e2_tables migration drops it), so there is
	// nothing to prune from that table.

	//err = rawdb.PruneTable(tx, kv.RCacheDomain, pruneTo, ctx, math.MaxInt32, time.Hour, logger, "")
	//require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)
}

func TestOptimizeWarmAddrAndAdjustGas(t *testing.T) {
	addr := common.HexToAddress("0xbeefbabeea323f07c59926295205d3b7a17e8638")
	other := common.HexToAddress("0x0000000000000000000000000000000000000001")
	slot := common.HexToHash("0x01")
	const baseGas = hexutil.Uint64(50000)

	t.Run("zero_slots_removed_gas_adjusted", func(t *testing.T) {
		al := types.AccessList{{Address: addr, StorageKeys: []common.Hash{}}}
		res := &accessListResult{Accesslist: &al, GasUsed: baseGas}
		optimizeWarmAddrAndAdjustGas(res, addr)
		require.Empty(t, *res.Accesslist)
		require.Equal(t, baseGas-hexutil.Uint64(params.TxAccessListAddressGas), res.GasUsed)
	})

	t.Run("with_slots_not_removed", func(t *testing.T) {
		al := types.AccessList{{Address: addr, StorageKeys: []common.Hash{slot}}}
		res := &accessListResult{Accesslist: &al, GasUsed: baseGas}
		optimizeWarmAddrAndAdjustGas(res, addr)
		require.Len(t, *res.Accesslist, 1)
		require.Equal(t, baseGas, res.GasUsed)
	})

	t.Run("addr_not_in_list_noop", func(t *testing.T) {
		al := types.AccessList{{Address: other, StorageKeys: []common.Hash{}}}
		res := &accessListResult{Accesslist: &al, GasUsed: baseGas}
		optimizeWarmAddrAndAdjustGas(res, addr)
		require.Len(t, *res.Accesslist, 1)
		require.Equal(t, baseGas, res.GasUsed)
	})

	t.Run("gas_no_underflow", func(t *testing.T) {
		al := types.AccessList{{Address: addr, StorageKeys: []common.Hash{}}}
		res := &accessListResult{Accesslist: &al, GasUsed: hexutil.Uint64(100)}
		optimizeWarmAddrAndAdjustGas(res, addr)
		require.Empty(t, *res.Accesslist)
		require.Equal(t, hexutil.Uint64(100), res.GasUsed) // 100 < 2400, no underflow
	})
}

func TestBlockOrLatest(t *testing.T) {
	number := rpc.BlockNumberOrHashWithNumber(5)
	hash := rpc.BlockNumberOrHashWithHash(common.Hash{0x01}, true)
	pending := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)

	for _, tc := range []struct {
		name string
		arg  *rpc.BlockNumberOrHash
		want rpc.BlockNumberOrHash
	}{
		{name: "omitted defaults to latest", arg: nil, want: rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)},
		{name: "number is kept", arg: &number, want: number},
		{name: "hash keeps requireCanonical", arg: &hash, want: hash},
		{name: "pending is left to the caller to reject", arg: &pending, want: pending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, blockOrLatest(tc.arg))
		})
	}
}
