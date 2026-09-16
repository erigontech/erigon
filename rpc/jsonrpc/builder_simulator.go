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
	"fmt"
	"math"
	"reflect"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	executionbuilder "github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/txnprovider"
)

type transactionRevision interface {
	TransactionSetRevision(uint64, uint64) uint64
}

type privateBundleTargetContext struct {
	Parent            rpc.BlockNumberOrHash
	Overrides         *ethapi.BlockOverrides
	BlockTime         uint64
	ParentBlockNumber uint64
	Generation        uint64
}

type privateBundleSimulatorImpl struct {
	chainConfig       *chain.Config
	targetBlock       func(context.Context, uint64) (*privateBundleTargetContext, error)
	targetTransaction func(context.Context, common.Hash) (types.Transaction, error)
	simulate          func(context.Context, SimulationRequest, rpc.BlockNumberOrHash) (SimulationResult, error)
	revisions         transactionRevision
	maxBlobsPerBlock  *uint64
}

func NewPrivateBundleSimulator(ethAPI *APIImpl, chainConfig *chain.Config, contexts *executionbuilder.BuildContextStore, revisions txnprovider.RevisionedTxnProvider, maxBlobsPerBlock *uint64) *privateBundleSimulatorImpl {
	return &privateBundleSimulatorImpl{
		chainConfig:      chainConfig,
		maxBlobsPerBlock: maxBlobsPerBlock,
		targetBlock: func(ctx context.Context, targetSlot uint64) (*privateBundleTargetContext, error) {
			return resolvePrivateBundleTarget(ctx, ethAPI, chainConfig, contexts, targetSlot)
		},
		targetTransaction: func(ctx context.Context, hash common.Hash) (types.Transaction, error) {
			return pendingTransaction(ctx, ethAPI.txPool, hash, chainConfig)
		},
		simulate: func(ctx context.Context, request SimulationRequest, block rpc.BlockNumberOrHash) (SimulationResult, error) {
			return ethAPI.simulateV1(ctx, request, block, true)
		},
		revisions: revisions,
	}
}

func (s *privateBundleSimulatorImpl) SimulatePrivateBundle(ctx context.Context, targetHash common.Hash, privateTxn types.Transaction, targetSlot uint64) (*PrivateBundleSimulationResult, error) {
	if s == nil || s.chainConfig == nil || s.targetBlock == nil || s.targetTransaction == nil || s.simulate == nil {
		return nil, errors.New("private bundle simulator is unavailable")
	}
	if targetHash == (common.Hash{}) {
		return nil, errors.New("private bundle target transaction hash is required")
	}
	if privateTxn == nil {
		return nil, errors.New("private bundle transaction is required")
	}
	if targetSlot == 0 {
		return nil, errors.New("private bundle target slot is required")
	}
	target, err := s.targetBlock(ctx, targetSlot)
	if err != nil {
		return nil, err
	}
	if target == nil || target.Overrides == nil {
		return nil, errors.New("private bundle target context is unavailable")
	}
	var revision uint64
	if s.revisions != nil {
		revision = s.revisions.TransactionSetRevision(target.BlockTime, target.ParentBlockNumber)
	}
	targetTxn, err := s.targetTransaction(ctx, targetHash)
	if err != nil {
		return nil, err
	}
	targetCall, err := transactionCallArgs(targetTxn, s.chainConfig)
	if err != nil {
		return nil, fmt.Errorf("prepare target transaction: %w", err)
	}
	privateCall, err := transactionCallArgs(privateTxn, s.chainConfig)
	if err != nil {
		return nil, fmt.Errorf("prepare private transaction: %w", err)
	}

	simulation, err := s.simulate(ctx, SimulationRequest{
		BlockStateCalls: []SimulatedBlock{{
			BlockOverrides: target.Overrides,
			Calls:          []ethapi.CallArgs{targetCall, privateCall},
		}},
		Validation:         true,
		strictTransactions: true,
		strictBlobGasLimit: s.strictBlobGasLimit(target.BlockTime),
	}, target.Parent)
	if err != nil {
		return nil, err
	}
	if target.Parent.BlockHash == nil {
		return nil, errors.New("private bundle target context has no parent block")
	}
	if err := s.ValidateTarget(ctx, targetHash, targetSlot, *target.Parent.BlockHash, target.Generation); err != nil {
		return nil, fmt.Errorf("public target changed during simulation: %w", err)
	}
	refreshedTarget, err := s.targetBlock(ctx, targetSlot)
	if err != nil {
		return nil, fmt.Errorf("private bundle target changed during simulation: %w", err)
	}
	if !samePrivateBundleTarget(target, refreshedTarget) {
		return nil, errors.New("private bundle target changed during simulation")
	}
	if len(simulation) != 1 {
		return nil, fmt.Errorf("private bundle simulation returned %d blocks", len(simulation))
	}
	calls, ok := simulation[0]["calls"].([]CallResult)
	if !ok || len(calls) != 2 {
		return nil, errors.New("private bundle simulation returned invalid call results")
	}
	baseFeeJSON, ok := simulation[0]["baseFeePerGas"].(*hexutil.U256)
	if !ok || baseFeeJSON == nil {
		return nil, errors.New("private bundle simulation returned no base fee")
	}
	parentHash, ok := simulation[0]["parentHash"].(common.Hash)
	if !ok {
		return nil, errors.New("private bundle simulation returned no parent hash")
	}
	if target.Parent.BlockHash == nil || parentHash != *target.Parent.BlockHash {
		return nil, errors.New("private bundle simulation used an unexpected parent block")
	}

	firstGasUsed := uint64(calls[0].GasUsed)
	secondGasUsed := uint64(calls[1].GasUsed)
	if secondGasUsed > math.MaxUint64-firstGasUsed {
		return nil, errors.New("private bundle simulation gas used overflow")
	}
	gasUsed := firstGasUsed + secondGasUsed
	priorityFees, err := estimatedPriorityFees([]types.Transaction{targetTxn, privateTxn}, calls, (*uint256.Int)(baseFeeJSON))
	if err != nil {
		return nil, err
	}
	success := true
	for _, call := range calls {
		success = success && uint64(call.Status) == types.ReceiptStatusSuccessful
	}
	result := &PrivateBundleSimulationResult{
		Success:                   success,
		TargetTransaction:         targetHash,
		PrivateTransaction:        privateTxn.Hash(),
		TargetSlot:                hexutil.Uint64(targetSlot),
		GasUsed:                   hexutil.Uint64(gasUsed),
		EstimatedPriorityFeeValue: (*hexutil.U256)(priorityFees),
		ParentBlockHash:           parentHash,
		TransactionSetRevision:    hexutil.Uint64(revision),
		Calls:                     calls,
		contextGeneration:         target.Generation,
	}
	return result, nil
}

func (s *privateBundleSimulatorImpl) strictBlobGasLimit(blockTime uint64) uint64 {
	maxBlobs := s.chainConfig.GetMaxBlobsPerBlock(blockTime)
	if s.maxBlobsPerBlock != nil {
		maxBlobs = min(maxBlobs, *s.maxBlobsPerBlock)
	}
	return maxBlobs * params.GasPerBlob
}

func (s *privateBundleSimulatorImpl) ValidateTarget(ctx context.Context, targetHash common.Hash, targetSlot uint64, parentHash common.Hash, generation uint64) error {
	if s == nil || s.targetTransaction == nil || s.targetBlock == nil {
		return errors.New("private bundle target validator is unavailable")
	}
	if _, err := s.targetTransaction(ctx, targetHash); err != nil {
		return err
	}
	target, err := s.targetBlock(ctx, targetSlot)
	if err != nil {
		return err
	}
	if target == nil || target.Parent.BlockHash == nil || *target.Parent.BlockHash != parentHash || target.Generation != generation {
		return errors.New("private bundle build context changed")
	}
	return nil
}

func samePrivateBundleTarget(left, right *privateBundleTargetContext) bool {
	return left != nil && right != nil &&
		left.Parent.BlockHash != nil && right.Parent.BlockHash != nil &&
		*left.Parent.BlockHash == *right.Parent.BlockHash &&
		left.Parent.RequireCanonical == right.Parent.RequireCanonical &&
		left.BlockTime == right.BlockTime &&
		left.ParentBlockNumber == right.ParentBlockNumber &&
		left.Generation == right.Generation &&
		reflect.DeepEqual(left.Overrides, right.Overrides)
}

func resolvePrivateBundleTarget(ctx context.Context, api *APIImpl, chainConfig *chain.Config, contexts *executionbuilder.BuildContextStore, targetSlot uint64) (*privateBundleTargetContext, error) {
	if api == nil || api.db == nil || api.BaseAPI == nil || chainConfig == nil || contexts == nil {
		return nil, errors.New("private bundle target resolver is unavailable")
	}
	tx, err := api.filters.BeginTemporalRoWithOverlay(ctx, api.db)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	parentNumber, parentHash, _, err := rpchelper.GetCanonicalBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber), tx, api._blockReader, nil)
	if err != nil {
		return nil, err
	}
	params, generation, ok := contexts.ResolveForParent(targetSlot, parentHash)
	if !ok {
		return nil, fmt.Errorf("private bundle build context for slot %d is unavailable", targetSlot)
	}
	parent, err := api.headerByHashAndNumber(ctx, tx, parentHash, parentNumber)
	if err != nil {
		return nil, err
	}
	target, err := privateBundleTargetFromParameters(parent, chainConfig, params, targetSlot)
	if err != nil {
		return nil, err
	}
	target.Generation = generation
	return target, nil
}

func privateBundleTargetFromParameters(parent *types.Header, chainConfig *chain.Config, params *executionbuilder.Parameters, targetSlot uint64) (*privateBundleTargetContext, error) {
	if parent == nil || chainConfig == nil || params == nil || params.SlotNumber == nil {
		return nil, errors.New("private bundle build context is incomplete")
	}
	if *params.SlotNumber != targetSlot {
		return nil, fmt.Errorf("private bundle build context slot %d does not match target slot %d", *params.SlotNumber, targetSlot)
	}
	if params.ParentHash != parent.Hash() {
		return nil, errors.New("private bundle build context parent hash mismatch")
	}
	if parent.Number.Uint64() == math.MaxUint64 {
		return nil, errors.New("private bundle target block identity overflow")
	}
	if params.Timestamp <= parent.Time {
		return nil, errors.New("private bundle build context timestamp is not after parent")
	}
	header := executionbuilder.MakeEmptyHeader(parent, chainConfig, params.Timestamp, params.TargetGasLimit)
	timestampJSON := hexutil.Uint64(params.Timestamp)
	slotJSON := hexutil.Uint64(targetSlot)
	numberJSON := hexutil.U256(header.Number)
	gasLimitJSON := hexutil.Uint64(header.GasLimit)
	prevRandao := params.PrevRandao
	withdrawals := types.Withdrawals(params.Withdrawals)
	return &privateBundleTargetContext{
		Parent:            rpc.BlockNumberOrHashWithHash(params.ParentHash, true),
		BlockTime:         params.Timestamp,
		ParentBlockNumber: parent.Number.Uint64(),
		Overrides: &ethapi.BlockOverrides{
			Number:       &numberJSON,
			Time:         &timestampJSON,
			SlotNumber:   &slotJSON,
			GasLimit:     &gasLimitJSON,
			FeeRecipient: &params.SuggestedFeeRecipient,
			PrevRandao:   &prevRandao,
			BeaconRoot:   params.ParentBeaconBlockRoot,
			Withdrawals:  &withdrawals,
		},
	}, nil
}

func pendingTransaction(ctx context.Context, pool txpoolproto.TxpoolClient, hash common.Hash, chainConfig *chain.Config) (types.Transaction, error) {
	if pool == nil {
		return nil, errors.New("transaction pool is unavailable")
	}
	reply, err := pool.Transactions(ctx, &txpoolproto.TransactionsRequest{Hashes: []*typesproto.H256{gointerfaces.ConvertHashToH256(hash)}})
	if err != nil {
		return nil, err
	}
	if reply == nil || len(reply.RlpTxs) != 1 || len(reply.RlpTxs[0]) == 0 {
		return nil, fmt.Errorf("public target transaction %s is not pending", hash)
	}
	txn, err := types.DecodeWrappedTransaction(reply.RlpTxs[0])
	if err != nil {
		return nil, fmt.Errorf("decode public target transaction: %w", err)
	}
	if txn.Hash() != hash {
		return nil, fmt.Errorf("transaction pool returned a different transaction for public target %s", hash)
	}
	sender, err := txn.Sender(*types.LatestSigner(chainConfig))
	if err != nil {
		return nil, fmt.Errorf("recover public target transaction sender: %w", err)
	}
	txn.SetSender(sender)
	return txn, nil
}

func transactionCallArgs(txn types.Transaction, chainConfig *chain.Config) (ethapi.CallArgs, error) {
	if txn == nil || chainConfig == nil {
		return ethapi.CallArgs{}, errors.New("transaction and chain config are required")
	}
	if txn.Type() == types.AccountAbstractionTxType {
		return ethapi.CallArgs{}, errors.New("account-abstraction transactions are not supported")
	}
	if txn.Type() == types.SetCodeTxType && len(txn.GetAuthorizations()) == 0 {
		return ethapi.CallArgs{}, errors.New("set-code transaction without authorizations is not supported")
	}
	sender, ok := txn.GetSender()
	if !ok {
		var err error
		sender, err = txn.Sender(*types.LatestSigner(chainConfig))
		if err != nil {
			return ethapi.CallArgs{}, err
		}
	}
	from := sender.Value()
	gas := hexutil.Uint64(txn.GetGasLimit())
	nonce := hexutil.Uint64(txn.GetNonce())
	data := hexutil.Bytes(bytes.Clone(txn.GetData()))
	args := ethapi.CallArgs{
		From:  &from,
		Gas:   &gas,
		Value: cloneU256(txn.GetValue()),
		Nonce: &nonce,
		Data:  &data,
	}
	if chainID := txn.GetChainID(); !chainID.IsZero() {
		args.ChainID = cloneU256(chainID)
	}
	if to := txn.GetTo(); to != nil {
		copied := *to
		args.To = &copied
	}
	if accessList := txn.GetAccessList(); accessList != nil {
		copied := make(types.AccessList, len(accessList))
		for i := range accessList {
			copied[i].Address = accessList[i].Address
			copied[i].StorageKeys = append([]common.Hash(nil), accessList[i].StorageKeys...)
		}
		args.AccessList = &copied
	}
	if txn.Type() == types.LegacyTxType {
		args.GasPrice = cloneU256(txn.GetFeeCap())
	} else {
		args.MaxFeePerGas = cloneU256(txn.GetFeeCap())
		args.MaxPriorityFeePerGas = cloneU256(txn.GetTipCap())
	}
	if txn.Type() == types.BlobTxType {
		blobTxn, ok := txn.Unwrap().(*types.BlobTx)
		if !ok {
			return ethapi.CallArgs{}, errors.New("unsupported blob transaction wrapper")
		}
		args.MaxFeePerBlobGas = cloneU256(&blobTxn.MaxFeePerBlobGas)
		args.BlobVersionedHashes = append([]common.Hash(nil), blobTxn.BlobVersionedHashes...)
	}
	authorizations := txn.GetAuthorizations()
	if len(authorizations) > 0 {
		args.AuthorizationList = make([]types.JsonAuthorization, len(authorizations))
		for i := range authorizations {
			args.AuthorizationList[i] = types.JsonAuthorization{}.FromAuthorization(authorizations[i])
		}
	}
	return args, nil
}

func estimatedPriorityFees(txns []types.Transaction, calls []CallResult, baseFee *uint256.Int) (*uint256.Int, error) {
	if len(txns) != len(calls) || baseFee == nil {
		return nil, errors.New("invalid priority fee simulation inputs")
	}
	total := new(uint256.Int)
	for i, txn := range txns {
		tip := txn.GetEffectiveGasTip(baseFee)
		fee := new(uint256.Int).Mul(&tip, uint256.NewInt(uint64(calls[i].GasUsed)))
		var overflow bool
		total, overflow = new(uint256.Int).AddOverflow(total, fee)
		if overflow {
			return nil, errors.New("simulated priority fee value overflow")
		}
	}
	return total, nil
}

func cloneU256(value *uint256.Int) *hexutil.U256 {
	if value == nil {
		return nil
	}
	return (*hexutil.U256)(new(uint256.Int).Set(value))
}
