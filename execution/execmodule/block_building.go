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
	"bytes"
	"context"
	"reflect"
	"time"

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

// buildDuration spans the target slot for early requests while bounding late and implausibly future requests.
func buildDuration(payloadTimestamp uint64, now time.Time, secondsPerSlot uint64) time.Duration {
	slot := time.Duration(secondsPerSlot) * time.Second
	// Reject beyond the cap horizon before converting: a large enough timestamp overflows
	// time.Unix and lands in the past, which would take the floor instead of the cap.
	horizon := now.Add(2 * slot)
	if payloadTimestamp > uint64(max(horizon.Unix(), 0)) {
		return 2 * slot
	}
	d := time.Unix(int64(payloadTimestamp), 0).Add(slot / 3).Sub(now)
	return min(max(d, slot/4), 2*slot)
}

func cloneBuilderParameters(params *builder.Parameters) *builder.Parameters {
	if params == nil {
		return nil
	}
	cloned := *params
	cloned.ExtraData = bytes.Clone(params.ExtraData)
	if params.Withdrawals != nil {
		cloned.Withdrawals = make([]*types.Withdrawal, len(params.Withdrawals))
		for i, withdrawal := range params.Withdrawals {
			if withdrawal != nil {
				copy := *withdrawal
				cloned.Withdrawals[i] = &copy
			}
		}
	}
	if params.ParentBeaconBlockRoot != nil {
		copy := *params.ParentBeaconBlockRoot
		cloned.ParentBeaconBlockRoot = &copy
	}
	if params.SlotNumber != nil {
		copy := *params.SlotNumber
		cloned.SlotNumber = &copy
	}
	if params.TargetGasLimit != nil {
		copy := *params.TargetGasLimit
		cloned.TargetGasLimit = &copy
	}
	return &cloned
}

func (e *ExecModule) evictOldBuilders() {
	ids := common.SortedKeys(e.builders)

	// remove old builders so that at most MaxBuilders - 1 remain
	for i := 0; i <= len(e.builders)-engine_helpers.MaxBuilders; i++ {
		id := ids[i]
		if old := e.builders[id]; old != nil {
			old.Cancel()
		}
		delete(e.builders, id)
		delete(e.builderParameters, id)
		for timestamp, builderID := range e.buildersByTimestamp {
			if builderID == id {
				delete(e.buildersByTimestamp, timestamp)
			}
		}
	}
}

func (e *ExecModule) AssembleBlock(ctx context.Context, params *builder.Parameters) (AssembleBlockResult, error) {
	if !e.semaphore.TryAcquire(1) {
		return AssembleBlockResult{Busy: true}, nil
	}
	defer e.semaphore.Release(1)
	if err := ctx.Err(); err != nil {
		return AssembleBlockResult{}, err
	}

	if err := e.checkWithdrawalsPresence(params.Timestamp, params.Withdrawals); err != nil {
		return AssembleBlockResult{}, err
	}

	if previousID, ok := e.buildersByTimestamp[params.Timestamp]; ok {
		candidate := cloneBuilderParameters(params)
		candidate.PayloadId = previousID
		params.PayloadId = previousID
		if reflect.DeepEqual(e.builderParameters[previousID], candidate) {
			e.logger.Info("[ForkChoiceUpdated] duplicate build request")
			return AssembleBlockResult{PayloadID: previousID}, nil
		}
		if previous := e.builders[previousID]; previous != nil {
			previous.Cancel()
		}
	}

	// Initiate payload building
	e.evictOldBuilders()

	e.nextPayloadId++
	params.PayloadId = e.nextPayloadId
	ownedParams := cloneBuilderParameters(params)

	e.builders[e.nextPayloadId] = builder.NewBlockBuilder(e.builderFunc, ownedParams, buildDuration(params.Timestamp, time.Now(), e.config.SecondsPerSlot()))
	if e.buildersByTimestamp == nil {
		e.buildersByTimestamp = make(map[uint64]uint64)
	}
	if e.builderParameters == nil {
		e.builderParameters = make(map[uint64]*builder.Parameters)
	}
	e.buildersByTimestamp[params.Timestamp] = e.nextPayloadId
	e.builderParameters[e.nextPayloadId] = ownedParams
	e.logger.Info("[ForkChoiceUpdated] BlockBuilder added", "payload", e.nextPayloadId)

	return AssembleBlockResult{PayloadID: e.nextPayloadId}, nil
}

// blockValue computes the expected value received by the fee recipient in wei.
func blockValue(br *types.BlockWithReceipts, baseFee *uint256.Int) *uint256.Int {
	blockValue := uint256.NewInt(0)
	txs := br.Block.Transactions()
	var gas, txValue uint256.Int
	for i := range txs {
		gas.SetUint64(br.Receipts[i].GasUsed)

		effectiveTip := txs[i].GetEffectiveGasTip(baseFee)

		txValue.Mul(&gas, &effectiveTip)
		blockValue.Add(blockValue, &txValue)
	}
	return blockValue
}

func (e *ExecModule) GetAssembledBlock(ctx context.Context, payloadID uint64) (AssembledBlockResult, error) {
	if !e.semaphore.TryAcquire(1) {
		return AssembledBlockResult{Busy: true}, nil
	}
	defer e.semaphore.Release(1)

	bldr, ok := e.builders[payloadID]
	if !ok {
		return AssembledBlockResult{}, nil
	}
	blockWithReceipts, err := bldr.Stop(ctx)
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
