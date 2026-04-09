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

package execmodule

import (
	"context"
	"reflect"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

func (e *ExecModule) checkWithdrawalsPresence(time uint64, withdrawals []*types.Withdrawal) error {
	if !e.config.IsShanghai(time) && withdrawals != nil {
		return &rpc.InvalidParamsError{Message: "withdrawals before shanghai"}
	}
	if e.config.IsShanghai(time) && withdrawals == nil {
		return &rpc.InvalidParamsError{Message: "missing withdrawals list"}
	}
	return nil
}

func (e *ExecModule) evictOldBuilders() {
	ids := common.SortedKeys(e.builders)

	// remove old builders so that at most MaxBuilders - 1 remain
	for i := 0; i <= len(e.builders)-engine_helpers.MaxBuilders; i++ {
		delete(e.builders, ids[i])
	}
}

func (e *ExecModule) AssembleBlock(ctx context.Context, params *builder.Parameters) (AssembleBlockResult, error) {
	if !e.semaphore.TryAcquire(1) {
		return AssembleBlockResult{Busy: true}, nil
	}
	defer e.semaphore.Release(1)

	if err := e.checkWithdrawalsPresence(params.Timestamp, params.Withdrawals); err != nil {
		return AssembleBlockResult{}, err
	}

	// First check if we're already building a block with the requested parameters
	if e.lastParameters != nil {
		params.PayloadId = e.lastParameters.PayloadId
		if reflect.DeepEqual(e.lastParameters, params) {
			e.logger.Info("[ForkChoiceUpdated] duplicate build request")
			return AssembleBlockResult{PayloadID: e.lastParameters.PayloadId}, nil
		}
	}

	// Initiate payload building
	e.evictOldBuilders()

	e.nextPayloadId++
	params.PayloadId = e.nextPayloadId
	e.lastParameters = params

	e.builders[e.nextPayloadId] = builder.NewBlockBuilder(e.builderFunc, params, e.config.SecondsPerSlot()/4)
	e.logger.Info("[ForkChoiceUpdated] BlockBuilder added", "payload", e.nextPayloadId)

	return AssembleBlockResult{PayloadID: e.nextPayloadId}, nil
}

// blockValue computes the expected value received by the fee recipient in wei.
func blockValue(br *types.BlockWithReceipts, baseFee *uint256.Int) *uint256.Int {
	blockValue := uint256.NewInt(0)
	txs := br.Block.Transactions()
	for i := range txs {
		gas := new(uint256.Int).SetUint64(br.Receipts[i].GasUsed)
		effectiveTip := txs[i].GetEffectiveGasTip(baseFee)
		txValue := new(uint256.Int).Mul(gas, effectiveTip)
		blockValue.Add(blockValue, txValue)
	}
	return blockValue
}

func (e *ExecModule) GetAssembledBlock(_ context.Context, payloadID uint64) (AssembledBlockResult, error) {
	if !e.semaphore.TryAcquire(1) {
		return AssembledBlockResult{Busy: true}, nil
	}
	defer e.semaphore.Release(1)

	bldr, ok := e.builders[payloadID]
	if !ok {
		return AssembledBlockResult{}, nil
	}
	blockWithReceipts, err := bldr.Stop()
	if err != nil {
		e.logger.Error("Failed to build PoS block", "err", err)
		return AssembledBlockResult{}, err
	}
	if blockWithReceipts == nil {
		return AssembledBlockResult{}, nil
	}

	header := blockWithReceipts.Block.Header()
	baseFee := header.BaseFee
	value := blockValue(blockWithReceipts, baseFee)

	return AssembledBlockResult{
		Block:      blockWithReceipts,
		BlockValue: value,
	}, nil
}
