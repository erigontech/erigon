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
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	executionbuilder "github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

type fixedRevisionProvider uint64

func (p fixedRevisionProvider) TransactionSetRevision(uint64, uint64) uint64 { return uint64(p) }

type changingRevisionProvider struct{ calls atomic.Uint64 }

func (p *changingRevisionProvider) TransactionSetRevision(uint64, uint64) uint64 {
	return p.calls.Add(1)
}

func fixedPrivateBundleTarget(slot uint64) func(context.Context, uint64) (*privateBundleTargetContext, error) {
	return func(_ context.Context, requestedSlot uint64) (*privateBundleTargetContext, error) {
		if requestedSlot != slot {
			return nil, errors.New("unexpected slot")
		}
		timestamp := hexutil.Uint64(1_000)
		slotNumber := hexutil.Uint64(slot)
		number := hexutil.U256(*uint256.NewInt(101))
		return &privateBundleTargetContext{
			Parent:            rpc.BlockNumberOrHashWithHash(common.Hash{0x55}, true),
			BlockTime:         uint64(timestamp),
			ParentBlockNumber: 100,
			Overrides: &ethapi.BlockOverrides{
				Number:     &number,
				Time:       &timestamp,
				SlotNumber: &slotNumber,
			},
		}, nil
	}
}

func signedSimulatorTransaction(t *testing.T, nonce, gasPrice uint64) types.Transaction {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	txn, err := types.SignTx(
		types.NewTransaction(nonce, common.Address{0x44}, uint256.NewInt(3), 50_000, uint256.NewInt(gasPrice), []byte{0xaa}),
		*types.LatestSigner(chain.AllProtocolChanges),
		key,
	)
	require.NoError(t, err)
	sender, err := txn.Sender(*types.LatestSigner(chain.AllProtocolChanges))
	require.NoError(t, err)
	txn.SetSender(sender)
	return txn
}

func TestPrivateBundleSimulatorRunsTargetThenPrivateAndReportsPriorityFees(t *testing.T) {
	targetTxn := signedSimulatorTransaction(t, 1, 20)
	privateTxn := signedSimulatorTransaction(t, 2, 30)
	targetHash := targetTxn.Hash()
	parentHash := common.Hash{0x55}
	baseFee := hexutil.U256(*uint256.NewInt(10))
	blockNumber := hexutil.U256(*uint256.NewInt(101))
	var simulated SimulationRequest
	simulator := &privateBundleSimulatorImpl{
		chainConfig: chain.AllProtocolChanges,
		targetBlock: fixedPrivateBundleTarget(42),
		targetTransaction: func(_ context.Context, hash common.Hash) (types.Transaction, error) {
			require.Equal(t, targetHash, hash)
			return targetTxn, nil
		},
		simulate: func(_ context.Context, request SimulationRequest, block rpc.BlockNumberOrHash) (SimulationResult, error) {
			simulated = request
			require.Equal(t, parentHash, *block.BlockHash)
			require.True(t, block.RequireCanonical)
			require.NotNil(t, request.BlockStateCalls[0].BlockOverrides)
			require.Equal(t, uint64(42), uint64(*request.BlockStateCalls[0].BlockOverrides.SlotNumber))
			return SimulationResult{{
				"calls": []CallResult{
					{GasUsed: hexutil.Uint64(21_000), Status: hexutil.Uint64(types.ReceiptStatusSuccessful)},
					{GasUsed: hexutil.Uint64(30_000), Status: hexutil.Uint64(types.ReceiptStatusSuccessful)},
				},
				"baseFeePerGas": &baseFee,
				"parentHash":    parentHash,
				"number":        &blockNumber,
				"timestamp":     hexutil.Uint64(1_000),
			}}, nil
		},
		revisions: fixedRevisionProvider(99),
	}

	result, err := simulator.SimulatePrivateBundle(t.Context(), targetHash, privateTxn, 42)
	require.NoError(t, err)
	require.True(t, result.Success)
	require.Equal(t, targetHash, result.TargetTransaction)
	require.Equal(t, privateTxn.Hash(), result.PrivateTransaction)
	require.Equal(t, uint64(42), uint64(result.TargetSlot))
	require.Equal(t, uint64(51_000), uint64(result.GasUsed))
	require.Equal(t, uint64(99), uint64(result.TransactionSetRevision))
	require.Equal(t, parentHash, result.ParentBlockHash)
	require.Equal(t, uint64(810_000), (*uint256.Int)(result.EstimatedPriorityFeeValue).Uint64())
	require.Len(t, simulated.BlockStateCalls, 1)
	require.Len(t, simulated.BlockStateCalls[0].Calls, 2)
	require.Equal(t, targetTxn.GetNonce(), uint64(*simulated.BlockStateCalls[0].Calls[0].Nonce))
	require.Equal(t, privateTxn.GetNonce(), uint64(*simulated.BlockStateCalls[0].Calls[1].Nonce))
}

func TestPrivateBundleSimulatorReportsRevertWithoutSubmitting(t *testing.T) {
	targetTxn := signedSimulatorTransaction(t, 1, 20)
	privateTxn := signedSimulatorTransaction(t, 2, 30)
	baseFee := hexutil.U256(*uint256.NewInt(10))
	blockNumber := hexutil.U256(*uint256.NewInt(101))
	simulator := &privateBundleSimulatorImpl{
		chainConfig:       chain.AllProtocolChanges,
		targetBlock:       fixedPrivateBundleTarget(42),
		targetTransaction: func(context.Context, common.Hash) (types.Transaction, error) { return targetTxn, nil },
		simulate: func(context.Context, SimulationRequest, rpc.BlockNumberOrHash) (SimulationResult, error) {
			return SimulationResult{{
				"calls": []CallResult{
					{GasUsed: hexutil.Uint64(21_000), Status: hexutil.Uint64(types.ReceiptStatusSuccessful)},
					{GasUsed: hexutil.Uint64(30_000), Status: hexutil.Uint64(types.ReceiptStatusFailed), Error: "reverted"},
				},
				"baseFeePerGas": &baseFee,
				"parentHash":    common.Hash{0x55},
				"number":        &blockNumber,
				"timestamp":     hexutil.Uint64(1_000),
			}}, nil
		},
	}

	result, err := simulator.SimulatePrivateBundle(t.Context(), targetTxn.Hash(), privateTxn, 42)
	require.NoError(t, err)
	require.False(t, result.Success)
}

type privateBundleTxPoolClient struct {
	txpoolproto.TxpoolClient
	reply *txpoolproto.TransactionsReply
}

func (c privateBundleTxPoolClient) Transactions(context.Context, *txpoolproto.TransactionsRequest, ...grpc.CallOption) (*txpoolproto.TransactionsReply, error) {
	return c.reply, nil
}

func TestPendingTransactionRejectsNilAndMismatchedReplies(t *testing.T) {
	targetTxn := signedSimulatorTransaction(t, 1, 20)
	requestedHash := common.Hash{0x99}

	_, err := pendingTransaction(t.Context(), privateBundleTxPoolClient{}, requestedHash, chain.AllProtocolChanges)
	require.ErrorContains(t, err, "not pending")

	var encoded bytes.Buffer
	require.NoError(t, targetTxn.MarshalBinary(&encoded))
	_, err = pendingTransaction(t.Context(), privateBundleTxPoolClient{
		reply: &txpoolproto.TransactionsReply{RlpTxs: [][]byte{encoded.Bytes()}},
	}, requestedHash, chain.AllProtocolChanges)
	require.ErrorContains(t, err, "different transaction")
}

func TestTransactionCallArgsRejectsAccountAbstractionTransaction(t *testing.T) {
	_, err := transactionCallArgs(&types.AccountAbstractionTransaction{}, chain.AllProtocolChanges)
	require.ErrorContains(t, err, "account-abstraction")
}

func TestTransactionCallArgsRejectsSetCodeWithoutAuthorizations(t *testing.T) {
	_, err := transactionCallArgs(&types.SetCodeTransaction{}, chain.AllProtocolChanges)
	require.ErrorContains(t, err, "without authorizations")
}

func TestPrivateBundleSimulatorRejectsGasUsedOverflow(t *testing.T) {
	targetTxn := signedSimulatorTransaction(t, 1, 20)
	privateTxn := signedSimulatorTransaction(t, 2, 30)
	baseFee := hexutil.U256(*uint256.NewInt(10))
	blockNumber := hexutil.U256(*uint256.NewInt(101))
	simulator := &privateBundleSimulatorImpl{
		chainConfig:       chain.AllProtocolChanges,
		targetBlock:       fixedPrivateBundleTarget(42),
		targetTransaction: func(context.Context, common.Hash) (types.Transaction, error) { return targetTxn, nil },
		simulate: func(context.Context, SimulationRequest, rpc.BlockNumberOrHash) (SimulationResult, error) {
			return SimulationResult{{
				"calls": []CallResult{
					{GasUsed: hexutil.Uint64(^uint64(0)), Status: hexutil.Uint64(types.ReceiptStatusSuccessful)},
					{GasUsed: 1, Status: hexutil.Uint64(types.ReceiptStatusSuccessful)},
				},
				"baseFeePerGas": &baseFee,
				"parentHash":    common.Hash{0x55},
				"number":        &blockNumber,
				"timestamp":     hexutil.Uint64(1_000),
			}}, nil
		},
	}

	_, err := simulator.SimulatePrivateBundle(t.Context(), targetTxn.Hash(), privateTxn, 42)
	require.ErrorContains(t, err, "gas used overflow")
}

func TestPrivateBundleSimulatorAllowsUnrelatedTransactionSetChangeDuringSimulation(t *testing.T) {
	targetTxn := signedSimulatorTransaction(t, 1, 20)
	privateTxn := signedSimulatorTransaction(t, 2, 30)
	baseFee := hexutil.U256(*uint256.NewInt(10))
	simulator := &privateBundleSimulatorImpl{
		chainConfig:       chain.AllProtocolChanges,
		targetBlock:       fixedPrivateBundleTarget(42),
		targetTransaction: func(context.Context, common.Hash) (types.Transaction, error) { return targetTxn, nil },
		simulate: func(context.Context, SimulationRequest, rpc.BlockNumberOrHash) (SimulationResult, error) {
			return SimulationResult{{
				"calls": []CallResult{
					{GasUsed: 21_000, Status: hexutil.Uint64(types.ReceiptStatusSuccessful)},
					{GasUsed: 30_000, Status: hexutil.Uint64(types.ReceiptStatusSuccessful)},
				},
				"baseFeePerGas": &baseFee,
				"parentHash":    common.Hash{0x55},
			}}, nil
		},
		revisions: &changingRevisionProvider{},
	}

	_, err := simulator.SimulatePrivateBundle(t.Context(), targetTxn.Hash(), privateTxn, 42)
	require.NoError(t, err)
}

func TestPrivateBundleSimulatorRejectsTargetDisappearingDuringSimulation(t *testing.T) {
	targetTxn := signedSimulatorTransaction(t, 1, 20)
	privateTxn := signedSimulatorTransaction(t, 2, 30)
	baseFee := hexutil.U256(*uint256.NewInt(10))
	var targetCalls atomic.Uint64
	simulator := &privateBundleSimulatorImpl{
		chainConfig: chain.AllProtocolChanges,
		targetBlock: fixedPrivateBundleTarget(42),
		targetTransaction: func(context.Context, common.Hash) (types.Transaction, error) {
			if targetCalls.Add(1) == 1 {
				return targetTxn, nil
			}
			return nil, errors.New("not pending")
		},
		simulate: func(context.Context, SimulationRequest, rpc.BlockNumberOrHash) (SimulationResult, error) {
			return SimulationResult{{
				"calls": []CallResult{
					{GasUsed: 21_000, Status: hexutil.Uint64(types.ReceiptStatusSuccessful)},
					{GasUsed: 30_000, Status: hexutil.Uint64(types.ReceiptStatusSuccessful)},
				},
				"baseFeePerGas": &baseFee,
				"parentHash":    common.Hash{0x55},
			}}, nil
		},
	}

	_, err := simulator.SimulatePrivateBundle(t.Context(), targetTxn.Hash(), privateTxn, 42)
	require.ErrorContains(t, err, "public target changed during simulation")
}

func TestPrivateBundleTargetValidationRejectsChangedBuildContext(t *testing.T) {
	targetTxn := signedSimulatorTransaction(t, 1, 20)
	simulator := &privateBundleSimulatorImpl{
		targetBlock: func(context.Context, uint64) (*privateBundleTargetContext, error) {
			return &privateBundleTargetContext{
				Parent:     rpc.BlockNumberOrHashWithHash(common.Hash{0x66}, true),
				Generation: 8,
			}, nil
		},
		targetTransaction: func(context.Context, common.Hash) (types.Transaction, error) {
			return targetTxn, nil
		},
	}

	err := simulator.ValidateTarget(t.Context(), targetTxn.Hash(), 42, common.Hash{0x55}, 7)
	require.ErrorContains(t, err, "build context changed")
}

func TestTransactionCallArgsOmitsZeroChainIDForUnprotectedLegacyTransaction(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	txn, err := types.SignTx(
		types.NewTransaction(1, common.Address{0x44}, uint256.NewInt(3), 50_000, uint256.NewInt(20), nil),
		*types.LatestSignerForChainID(nil),
		key,
	)
	require.NoError(t, err)
	require.False(t, txn.Protected())

	args, err := transactionCallArgs(txn, chain.AllProtocolChanges)
	require.NoError(t, err)
	require.Nil(t, args.ChainID)
}

func TestPrivateBundleSimulatorCapsStrictBlobBudget(t *testing.T) {
	cap := uint64(2)
	simulator := &privateBundleSimulatorImpl{
		chainConfig:      chain.AllProtocolChanges,
		maxBlobsPerBlock: &cap,
	}

	require.Equal(t, uint64(2*params.GasPerBlob), simulator.strictBlobGasLimit(1_000))
}

func TestPrivateBundleSimulatorRejectsParentChangeDuringSimulation(t *testing.T) {
	targetTxn := signedSimulatorTransaction(t, 1, 20)
	privateTxn := signedSimulatorTransaction(t, 2, 30)
	baseFee := hexutil.U256(*uint256.NewInt(10))
	var targetCalls atomic.Uint64
	simulator := &privateBundleSimulatorImpl{
		chainConfig: chain.AllProtocolChanges,
		targetBlock: func(context.Context, uint64) (*privateBundleTargetContext, error) {
			target := fixedPrivateBundleTarget(42)
			resolved, err := target(t.Context(), 42)
			if err != nil {
				return nil, err
			}
			if targetCalls.Add(1) > 1 {
				resolved.Parent = rpc.BlockNumberOrHashWithHash(common.Hash{0x66}, true)
				resolved.ParentBlockNumber++
			}
			return resolved, nil
		},
		targetTransaction: func(context.Context, common.Hash) (types.Transaction, error) { return targetTxn, nil },
		simulate: func(context.Context, SimulationRequest, rpc.BlockNumberOrHash) (SimulationResult, error) {
			return SimulationResult{{
				"calls": []CallResult{
					{GasUsed: 21_000, Status: hexutil.Uint64(types.ReceiptStatusSuccessful)},
					{GasUsed: 30_000, Status: hexutil.Uint64(types.ReceiptStatusSuccessful)},
				},
				"baseFeePerGas": &baseFee,
				"parentHash":    common.Hash{0x55},
			}}, nil
		},
	}

	_, err := simulator.SimulatePrivateBundle(t.Context(), targetTxn.Hash(), privateTxn, 42)
	require.ErrorContains(t, err, "target changed during simulation")
}

func TestPrivateBundleTargetContextUsesExactBuildParameters(t *testing.T) {
	parent := types.NewEmptyHeaderForAssembling()
	parent.Number.SetUint64(100)
	parent.Time = 1_000
	parent.GasLimit = 30_000_000
	parent.BaseFee = uint256.NewInt(1_000_000_000)
	parentSlot := uint64(40)
	parent.SlotNumber = &parentSlot
	parentHash := parent.Hash()
	feeRecipient := common.Address{0x77}
	prevRandao := common.Hash{0x88}
	beaconRoot := common.Hash{0x99}
	targetSlot := uint64(42)
	targetGasLimit := uint64(36_000_000)
	params := &executionbuilder.Parameters{
		ParentHash:            parentHash,
		Timestamp:             1_007,
		PrevRandao:            prevRandao,
		SuggestedFeeRecipient: feeRecipient,
		Withdrawals:           []*types.Withdrawal{{Index: 1, Address: common.Address{0xaa}}},
		ParentBeaconBlockRoot: &beaconRoot,
		SlotNumber:            &targetSlot,
		TargetGasLimit:        &targetGasLimit,
	}

	target, err := privateBundleTargetFromParameters(parent, chain.AllProtocolChanges, params, targetSlot)
	require.NoError(t, err)
	require.Equal(t, parentHash, *target.Parent.BlockHash)
	require.Equal(t, uint64(1_007), target.BlockTime)
	require.Equal(t, uint64(100), target.ParentBlockNumber)
	require.Equal(t, uint64(42), uint64(*target.Overrides.SlotNumber))
	require.Equal(t, uint64(1_007), uint64(*target.Overrides.Time))
	require.Equal(t, feeRecipient, *target.Overrides.FeeRecipient)
	require.Equal(t, prevRandao, *target.Overrides.PrevRandao)
	require.Equal(t, beaconRoot, *target.Overrides.BeaconRoot)
	require.Equal(t, executionbuilder.MakeEmptyHeader(parent, chain.AllProtocolChanges, params.Timestamp, params.TargetGasLimit).GasLimit, uint64(*target.Overrides.GasLimit))
	require.Equal(t, types.Withdrawals(params.Withdrawals), *target.Overrides.Withdrawals)

	wrongSlot := targetSlot + 1
	_, err = privateBundleTargetFromParameters(parent, chain.AllProtocolChanges, params, wrongSlot)
	require.ErrorContains(t, err, "does not match")
}

func TestPrivateBundleTargetAndSimulationUsePublishedOverlay(t *testing.T) {
	base, m, overlayHeader, events := newOverlayAheadTestAPIWithEvents(t)
	overlay := events.LatestSD().BlockOverlay()
	require.NoError(t, stages.SaveStageProgress(overlay, stages.Execution, overlayHeader.Number.Uint64()))
	api := newEthApiForTest(base, m.DB, nil, nil)
	store := executionbuilder.NewBuildContextStore()
	slot := uint64(42)
	gasLimit := overlayHeader.GasLimit
	targetTimestamp := max(overlayHeader.Time+1, uint64(time.Now().Add(time.Hour).Unix()))
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	wrapped := store.Wrap(func(context.Context, *executionbuilder.Parameters, *atomic.Bool) (*types.BlockWithReceipts, error) {
		close(started)
		<-release
		return nil, nil
	})
	go func() {
		defer close(done)
		_, _ = wrapped(t.Context(), &executionbuilder.Parameters{
			ParentHash:               overlayHeader.Hash(),
			Timestamp:                targetTimestamp,
			SlotNumber:               &slot,
			TargetGasLimit:           &gasLimit,
			ValidatedProposerContext: true,
		}, nil)
	}()
	<-started
	t.Cleanup(func() {
		close(release)
		<-done
	})

	target, err := resolvePrivateBundleTarget(t.Context(), api, m.ChainConfig, store, slot)
	require.NoError(t, err)
	require.Equal(t, overlayHeader.Hash(), *target.Parent.BlockHash)
	require.Equal(t, overlayHeader.Number.Uint64(), target.ParentBlockNumber)

	result, err := api.simulateV1(t.Context(), SimulationRequest{
		BlockStateCalls: []SimulatedBlock{{BlockOverrides: target.Overrides}},
		Validation:      true,
	}, target.Parent, true)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, overlayHeader.Hash(), result[0]["parentHash"])

	staleStore := executionbuilder.NewBuildContextStore()
	staleStarted := make(chan struct{})
	staleRelease := make(chan struct{})
	staleDone := make(chan struct{})
	staleWrapped := staleStore.Wrap(func(context.Context, *executionbuilder.Parameters, *atomic.Bool) (*types.BlockWithReceipts, error) {
		close(staleStarted)
		<-staleRelease
		return nil, nil
	})
	go func() {
		defer close(staleDone)
		_, _ = staleWrapped(t.Context(), &executionbuilder.Parameters{
			ParentHash:               overlayHeader.ParentHash,
			Timestamp:                targetTimestamp,
			SlotNumber:               &slot,
			TargetGasLimit:           &gasLimit,
			ValidatedProposerContext: true,
		}, nil)
	}()
	<-staleStarted
	_, err = resolvePrivateBundleTarget(t.Context(), api, m.ChainConfig, staleStore, slot)
	require.ErrorContains(t, err, "unavailable")
	close(staleRelease)
	<-staleDone
}
