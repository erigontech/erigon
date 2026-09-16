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
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	executionbuilder "github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider/privatepool"
)

const maxPrivateTransactionSize = 128 * 1024

type BuilderAPI interface {
	SendPrivateBundle(context.Context, PrivateBundleRequest) (common.Hash, error)
	SimulateBundle(context.Context, PrivateBundleRequest) (*PrivateBundleSimulationResult, error)
}

type PrivateBundleRequest struct {
	TargetTransaction common.Hash    `json:"targetTransaction"`
	Transaction       hexutil.Bytes  `json:"transaction"`
	TargetSlot        hexutil.Uint64 `json:"targetSlot"`
}

type privateBundleSubmitter interface {
	Submit(privatepool.Bundle) (common.Hash, error)
}

type privateBundleAdmission interface {
	Submit(context.Context, privatepool.Bundle, *PrivateBundleSimulationResult) (common.Hash, error)
}

type privateBundleTargetValidator interface {
	ValidateTarget(context.Context, common.Hash) error
}

type activePrivateBundleAdmission struct {
	submitter privateBundleSubmitter
	contexts  *executionbuilder.BuildContextStore
	validator privateBundleTargetValidator
}

func (a *activePrivateBundleAdmission) Submit(ctx context.Context, bundle privatepool.Bundle, simulation *PrivateBundleSimulationResult) (common.Hash, error) {
	if a == nil || a.submitter == nil || a.contexts == nil || a.validator == nil || simulation == nil || simulation.contextGeneration == 0 {
		return common.Hash{}, errors.New("private bundle admission context is unavailable")
	}
	if err := a.validator.ValidateTarget(ctx, bundle.TargetHash); err != nil {
		return common.Hash{}, fmt.Errorf("public target changed before private bundle submission: %w", err)
	}
	bundle.TargetParentHash = simulation.ParentBlockHash
	bundle.TargetGeneration = simulation.contextGeneration
	var id common.Hash
	err := a.contexts.WithActive(bundle.TargetSlot, simulation.contextGeneration, func() error {
		var err error
		id, err = a.submitter.Submit(bundle)
		return err
	})
	return id, err
}

type privateBundleSimulator interface {
	SimulatePrivateBundle(context.Context, common.Hash, types.Transaction, uint64) (*PrivateBundleSimulationResult, error)
}

type PrivateBundleSimulationResult struct {
	Success                   bool           `json:"success"`
	TargetTransaction         common.Hash    `json:"targetTransaction"`
	PrivateTransaction        common.Hash    `json:"privateTransaction"`
	TargetSlot                hexutil.Uint64 `json:"targetSlot"`
	GasUsed                   hexutil.Uint64 `json:"gasUsed"`
	EstimatedPriorityFeeValue *hexutil.U256  `json:"estimatedPriorityFeeValue,omitempty"`
	ParentBlockHash           common.Hash    `json:"parentBlockHash"`
	TransactionSetRevision    hexutil.Uint64 `json:"transactionSetRevision"`
	Calls                     []CallResult   `json:"calls"`
	contextGeneration         uint64
}

type BuilderAPIImpl struct {
	admission   privateBundleAdmission
	chainConfig *chain.Config
	simulator   privateBundleSimulator
}

func NewBuilderAPI(submitter privateBundleSubmitter, chainConfig *chain.Config, simulator privateBundleSimulator, contexts *executionbuilder.BuildContextStore) *BuilderAPIImpl {
	validator, _ := simulator.(privateBundleTargetValidator)
	return newBuilderAPI(chainConfig, simulator, &activePrivateBundleAdmission{submitter: submitter, contexts: contexts, validator: validator})
}

func newBuilderAPI(chainConfig *chain.Config, simulator privateBundleSimulator, admission privateBundleAdmission) *BuilderAPIImpl {
	return &BuilderAPIImpl{admission: admission, chainConfig: chainConfig, simulator: simulator}
}

func (api *BuilderAPIImpl) SendPrivateBundle(ctx context.Context, request PrivateBundleRequest) (common.Hash, error) {
	txn, err := api.decodePrivateTransaction(request.Transaction)
	if err != nil {
		return common.Hash{}, err
	}
	if api.simulator == nil {
		return common.Hash{}, errors.New("private bundle simulation is unavailable")
	}
	simulation, err := api.simulator.SimulatePrivateBundle(ctx, request.TargetTransaction, txn, uint64(request.TargetSlot))
	if err != nil {
		return common.Hash{}, err
	}
	if simulation == nil || !simulation.Success {
		return common.Hash{}, errors.New("private bundle simulation failed")
	}
	if api.admission == nil {
		return common.Hash{}, errors.New("private bundle admission is unavailable")
	}
	return api.admission.Submit(ctx, privatepool.Bundle{
		TargetHash:  request.TargetTransaction,
		Transaction: txn,
		TargetSlot:  uint64(request.TargetSlot),
	}, simulation)
}

func (api *BuilderAPIImpl) SimulateBundle(ctx context.Context, request PrivateBundleRequest) (*PrivateBundleSimulationResult, error) {
	if api == nil || api.simulator == nil {
		return nil, errors.New("private bundle simulation is unavailable")
	}
	txn, err := api.decodePrivateTransaction(request.Transaction)
	if err != nil {
		return nil, err
	}
	return api.simulator.SimulatePrivateBundle(ctx, request.TargetTransaction, txn, uint64(request.TargetSlot))
}

func (api *BuilderAPIImpl) decodePrivateTransaction(raw hexutil.Bytes) (types.Transaction, error) {
	if api == nil || api.chainConfig == nil {
		return nil, errors.New("private bundle service is unavailable")
	}
	if len(raw) == 0 {
		return nil, errors.New("private transaction is required")
	}
	if len(raw) > maxPrivateTransactionSize {
		return nil, fmt.Errorf("private transaction exceeds %d bytes", maxPrivateTransactionSize)
	}
	txn, err := types.DecodeTransaction(raw)
	if err != nil {
		return nil, fmt.Errorf("decode private transaction: %w", err)
	}
	if txn.Type() == types.BlobTxType {
		return nil, errors.New("private blob transactions are not supported")
	}
	if txn.Type() == types.AccountAbstractionTxType {
		return nil, errors.New("private account-abstraction transactions are not supported")
	}
	chainID := txn.GetChainID()
	if !chainID.IsZero() && !chainID.Eq(api.chainConfig.ChainID) {
		return nil, fmt.Errorf("private transaction chain ID %s does not match node chain ID %s", chainID, api.chainConfig.ChainID)
	}
	sender, err := txn.Sender(*types.LatestSigner(api.chainConfig))
	if err != nil {
		return nil, fmt.Errorf("recover private transaction sender: %w", err)
	}
	txn.SetSender(sender)
	return txn, nil
}
