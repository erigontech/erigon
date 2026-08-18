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

// buildDuration returns how long a payload builder may run before it stops itself.
//
// A consensus layer may send payload attributes well ahead of the slot the payload is for and then
// call getPayload against the cached payload id, without sending fresh attributes. A builder that
// stops before that slot hands back a payload missing every transaction that arrived in between, so
// build until shortly into the target slot, derived from the payload's own timestamp. The floor
// leaves a late request no worse off than a fixed budget; the cap stops an implausible timestamp
// from pinning a builder and its resources.
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

// stopGraceDuration returns how long a builder that reached its budget may take to finish up - the
// transaction in flight, the packing tail, payload finalization - before it is taken as stuck and
// discarded.
func stopGraceDuration(secondsPerSlot uint64) time.Duration {
	return time.Duration(secondsPerSlot) * time.Second / 2
}

// sameBuildRequest reports whether a request is asking for the payload another one is already
// building. A custom transaction provider is never treated as the same request: it is stateful and
// hands its transactions over once, so a second request carrying one is not asking for what the
// first is building, and comparing the provider itself would read fields the running build writes.
func sameBuildRequest(previous, current *builder.Parameters) bool {
	if previous == nil || current == nil {
		return false
	}
	if previous.CustomTxnProvider != nil || current.CustomTxnProvider != nil {
		return false
	}
	// The payload id is this module's to assign, so it is not part of the question, and comparing
	// copies keeps the caller's parameters out of it.
	withoutID := func(p *builder.Parameters) builder.Parameters {
		stripped := *p
		stripped.PayloadId = 0
		return stripped
	}
	previousWithoutID, currentWithoutID := withoutID(previous), withoutID(current)
	return reflect.DeepEqual(&previousWithoutID, &currentWithoutID)
}

// builderEntry keeps a builder with the immutable parameters it was created for, so the two cannot
// drift apart and eviction can drop the timestamp index without scanning it.
type builderEntry struct {
	builder *builder.BlockBuilder
	params  *builder.Parameters
}

// isIndexedFor reports whether the timestamp index still resolves to this entry, which is what has
// to be cleaned up when the entry goes.
func (e *ExecModule) isIndexedFor(id uint64, entry *builderEntry) bool {
	return entry != nil && entry.params != nil && e.buildersByTimestamp[entry.params.Timestamp] == id
}

// isLiveProposalTarget reports whether an entry is what a proposal is still waiting on: the builder
// its timestamp resolves to, for a slot that has not passed. Being indexed is not enough on its own,
// because every slot has its own timestamp and successful entries stay indexed: protecting all of
// them would mean never evicting anything.
func (e *ExecModule) isLiveProposalTarget(id uint64, entry *builderEntry, now time.Time) bool {
	if !e.isIndexedFor(id, entry) {
		return false
	}
	// A builder that can no longer produce is not worth protecting, however live its slot.
	if entry.builder == nil || entry.builder.Failed() || entry.builder.Discarded() {
		return false
	}
	// Compared as seconds rather than times, so an implausible timestamp wraps into "not live"
	// instead of overflowing into the past.
	nowSeconds := uint64(max(now.Unix(), 0))
	slotSeconds := e.config.SecondsPerSlot()
	timestamp := entry.params.Timestamp
	return timestamp+slotSeconds > nowSeconds && timestamp <= nowSeconds+2*slotSeconds
}

// dropBuilder removes and discards a builder.
func (e *ExecModule) dropBuilder(id uint64, entry *builderEntry) {
	if entry != nil {
		if e.isIndexedFor(id, entry) {
			delete(e.buildersByTimestamp, entry.params.Timestamp)
		}
		if entry.builder != nil {
			entry.builder.Discard()
		}
	}
	delete(e.builders, id)
}

// evictOldBuilders makes room for one builder by dropping the oldest entries, except those a live
// proposal is still waiting on.
func (e *ExecModule) evictOldBuilders() {
	remaining := len(e.builders) - engine_helpers.MaxBuilders + 1
	if remaining <= 0 {
		return
	}
	now := time.Now()
	for _, id := range common.SortedKeys(e.builders) {
		if remaining <= 0 {
			return
		}
		entry := e.builders[id]
		if e.isLiveProposalTarget(id, entry, now) {
			continue
		}
		e.dropBuilder(id, entry)
		remaining--
	}
}

func (e *ExecModule) AssembleBlock(ctx context.Context, params *builder.Parameters) (AssembleBlockResult, error) {
	// Cancellation is checked first so an expired request reports why it stopped instead of
	// looking like contention, which callers retry. The module context check avoids starting work
	// after shutdown has already been observed.
	if err := ctx.Err(); err != nil {
		return AssembleBlockResult{}, err
	}
	if err := e.backgroundCtx.Err(); err != nil {
		return AssembleBlockResult{}, err
	}
	if !e.semaphore.TryAcquire(1) {
		return AssembleBlockResult{Busy: true}, nil
	}
	defer e.semaphore.Release(1)

	if err := e.checkWithdrawalsPresence(params.Timestamp, params.Withdrawals); err != nil {
		return AssembleBlockResult{}, err
	}

	// A stopped builder is still worth reusing: it holds the payload it was stopped for, which is
	// exactly what a repeated request is asking for. Only one that cannot produce - failed, or
	// discarded with its work still winding down - has to be passed over.
	if previousID, ok := e.buildersByTimestamp[params.Timestamp]; ok {
		if previous := e.builders[previousID]; previous != nil && previous.builder != nil &&
			!previous.builder.Failed() && !previous.builder.Discarded() {
			if sameBuildRequest(previous.params, params) {
				e.logger.Info("[ForkChoiceUpdated] duplicate build request")
				return AssembleBlockResult{PayloadID: previousID}, nil
			}
		}
	}
	// A superseded builder keeps running to its own deadline. The timestamp index moves to the new
	// one, so nothing reaches it by dedup, while an id already handed out goes on answering with a
	// payload that is still growing.
	e.evictOldBuilders()

	e.nextPayloadId++
	ownedParams := params.Copy()
	ownedParams.PayloadId = e.nextPayloadId

	secondsPerSlot := e.config.SecondsPerSlot()
	e.builders[e.nextPayloadId] = &builderEntry{
		builder: builder.NewBlockBuilder(e.backgroundCtx, e.builderFunc, ownedParams,
			buildDuration(params.Timestamp, time.Now(), secondsPerSlot), stopGraceDuration(secondsPerSlot)),
		params: ownedParams,
	}
	e.buildersByTimestamp[params.Timestamp] = e.nextPayloadId
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
	if err := ctx.Err(); err != nil {
		return AssembledBlockResult{}, err
	}
	if !e.semaphore.TryAcquire(1) {
		return AssembledBlockResult{Busy: true}, nil
	}
	defer e.semaphore.Release(1)

	entry, ok := e.builders[payloadID]
	if !ok || entry == nil || entry.builder == nil {
		return AssembledBlockResult{Unknown: true}, nil
	}
	// Nothing comes of a discarded build, and waiting for its goroutine to notice would hold the
	// caller for as long as whatever the build is blocked on.
	if entry.builder.Discarded() {
		e.dropBuilder(payloadID, entry)
		return AssembledBlockResult{Unknown: true}, nil
	}
	blockWithReceipts, err := entry.builder.Stop(ctx)
	if entry.builder.Discarded() {
		e.dropBuilder(payloadID, entry)
		return AssembledBlockResult{Unknown: true}, nil
	}
	if err != nil {
		// Stop reports the caller's wait expiring and the build's own failure through the same
		// error, and a build can fail with a context error of its own - a transaction provider
		// giving up, say. Only the builder knows which happened, so ask it rather than guess from
		// the error: a caller that gave up leaves a builder still worth collecting.
		if !entry.builder.Failed() {
			return AssembledBlockResult{}, err
		}
		// Keeping a failed entry would hand its latched error to every retry.
		e.dropBuilder(payloadID, entry)
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
