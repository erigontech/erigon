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

package protocol

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func CheckInclusionListTransactions(
	gp *GasPool,
	blockTxns types.Transactions,
	inclusionListTxns types.Transactions,
	stateReader state.StateReader,
	blockContext *evmtypes.BlockContext,
	chainConfig *chain.Config,
	vmConfig *vm.Config,
) (bool, error) {
	ibs := state.New(stateReader)
	defer ibs.Close()
	cfg := *vmConfig
	cfg.Tracer = nil
	evm := vm.NewEVM(*blockContext, evmtypes.TxContext{}, ibs, chainConfig, cfg)
	signer := *types.MakeSigner(chainConfig, blockContext.BlockNumber, blockContext.Time)

	rules := evm.ChainRules()
	baseFee := evm.Context.BaseFee

	for _, tx := range OmittedInclusionListTxns(blockTxns, inclusionListTxns) {
		msg, err := tx.AsMessage(signer, &baseFee, rules)
		if err != nil {
			continue
		}

		var valid bool
		if frameTxn, ok := tx.(*types.FrameTransaction); ok {
			valid, err = frameTxnValidAtBlockEnd(evm, ibs, gp, msg, frameTxn)
		} else {
			valid, err = txnValidAtBlockEnd(evm, gp, msg)
		}
		if err != nil {
			return false, err
		}
		if !valid {
			continue
		}

		log.Debug("[EIP-7805] Inclusion list transaction should have been included in the block",
			"txn", tx.Hash(), "sender", msg.From(), "nonce", msg.Nonce(), "block", evm.Context.BlockNumber)

		return false, nil
	}
	return true, nil
}

// OmittedInclusionListTxns returns the IL txns that the block omits and that a
// builder could include from the IL alone. Blob-carrying txns are excluded
// because the IL carries no sidecar.
func OmittedInclusionListTxns(blockTxns, inclusionListTxns types.Transactions) types.Transactions {
	inBlock := make(map[common.Hash]struct{}, len(blockTxns))
	for _, tx := range blockTxns {
		inBlock[tx.Hash()] = struct{}{}
	}
	var omitted types.Transactions
	for _, tx := range inclusionListTxns {
		if _, ok := inBlock[tx.Hash()]; ok {
			continue
		}
		if tx.Type() == types.BlobTxType || tx.GetBlobGas() != 0 {
			continue
		}
		omitted = append(omitted, tx)
	}
	return omitted
}

func txnValidAtBlockEnd(evm *vm.EVM, gp *GasPool, msg *types.Message) (bool, error) {
	executor := NewTxnExecutor(evm, msg, gp)
	rules, vmConfig := evm.ChainRules(), evm.Config()
	intrinsicGasResult, overflow := intrinsicGas(msg, rules, vmConfig.HasEip3860(rules))
	if overflow {
		return false, nil
	}
	if _, err := executor.preCheck(false, intrinsicGasResult); err != nil {
		if errors.Is(err, ErrTxnExecutionFailed) {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

// frameTxnValidAtBlockEnd executes the frame transaction, so approval and nonce
// rules match consensus execution. A rejected transaction reverts its own state
// changes, and a valid one ends the inclusion-list check.
func frameTxnValidAtBlockEnd(evm *vm.EVM, ibs *state.IntraBlockState, gp *GasPool, msg *types.Message, txn *types.FrameTransaction) (bool, error) {
	evm.Reset(NewEVMTxContext(msg), ibs)
	isolatedGas := NewBlockGasPool(gp.ExecutionGasAvailable(), gp.StateGasAvailable(), gp.BlobGas())
	_, err := NewTxnExecutor(evm, msg, isolatedGas).executeFrameTransaction(txn, true, false, nil)
	if readErr := ibs.StateReadError(); readErr != nil {
		return false, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, readErr)
	}
	if errors.Is(err, vm.ErrIntraBlockStateFailed) {
		return false, fmt.Errorf("%w: %w", ErrTxnExecutionFailed, err)
	}
	return err == nil, nil
}
