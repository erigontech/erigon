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
	"errors"
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
		if bldr := e.builders[ids[i]]; bldr != nil {
			// Cancel so the build goroutine exits and releases its scoped read
			// view (pinned roTx) rather than leaking it past eviction.
			bldr.Cancel()
		}
		delete(e.builders, ids[i])
	}
}

func (e *ExecModule) AssembleBlock(ctx context.Context, params *builder.Parameters) (AssembleBlockResult, error) {
	if !e.fgTryAcquire() {
		return AssembleBlockResult{Busy: true}, nil
	}
	defer e.fgRelease()

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

	// Pin a consistent, by-block read snapshot for the requested parent while we
	// hold the foreground semaphore (settled state, no commit mid-flight). If the
	// head is not yet at ParentHash, signal Busy so the CL retries rather than
	// building on the wrong block. The build reads only through this view — never
	// the raw DB directly nor the mutable global published SD.
	view, err := e.captureScopedReadView(ctx, params.ParentHash)
	if err != nil {
		if errors.Is(err, errHeadMismatch) {
			return AssembleBlockResult{Busy: true}, nil
		}
		return AssembleBlockResult{}, err
	}

	// Initiate payload building
	e.evictOldBuilders()

	e.nextPayloadId++
	params.PayloadId = e.nextPayloadId
	e.lastParameters = params

	// Carry the view on a per-build copy so it never enters e.lastParameters
	// (which the duplicate-request check DeepEquals).
	buildParams := *params
	buildParams.ScopedView = view
	e.builders[e.nextPayloadId] = builder.NewBlockBuilder(e.builderFunc, &buildParams, e.config.SecondsPerSlot()/4)
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

func (e *ExecModule) GetAssembledBlock(_ context.Context, payloadID uint64) (AssembledBlockResult, error) {
	if !e.fgTryAcquire() {
		return AssembledBlockResult{Busy: true}, nil
	}
	defer e.fgRelease()

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
