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
	"slices"

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
	evm := vm.NewEVM(*blockContext, evmtypes.TxContext{}, ibs, chainConfig, *vmConfig)
	signer := *types.MakeSigner(chainConfig, blockContext.BlockNumber, blockContext.Time)

	inBlock := make(map[common.Hash]struct{}, len(blockTxns))
	for _, tx := range blockTxns {
		inBlock[tx.Hash()] = struct{}{}
	}

	rules := evm.ChainRules()
	baseFee := evm.Context.BaseFee

	for _, tx := range inclusionListTxns {
		if _, ok := inBlock[tx.Hash()]; ok {
			continue
		}

		if tx.Type() == types.BlobTxType {
			continue
		}

		msg, err := tx.AsMessage(signer, &baseFee, rules)
		if err != nil {
			continue
		}

		executor := NewTxnExecutor(evm, msg, gp)

		intrinsicGasResult, overflow := executor.calcIntrinsicGas(
			msg.To().IsNil(),
			msg.Authorizations(),
			slices.Clone[types.AccessList](msg.AccessList()),
		)
		if overflow {
			continue
		}

		if _, err := executor.preCheck(false, intrinsicGasResult); err != nil {
			if errors.Is(err, ErrTxnExecutionFailed) {
				return false, err
			}
			continue
		}

		log.Debug("[EIP-7805] Inclusion list transaction should have been included in the block",
			"txn", tx.Hash(), "sender", msg.From(), "nonce", msg.Nonce(), "block", evm.Context.BlockNumber)

		return false, nil
	}
	return true, nil
}
