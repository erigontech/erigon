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

package forkchoice

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	lru "github.com/hashicorp/golang-lru/v2"
)

var ErrPayloadImportInProgress = errors.New("canonical payload import is in progress")

type PayloadPreparationImports struct {
	active           sync.Map
	pendingEnvelopes sync.Map
	preparing        atomic.Pointer[payloadPreparationAttempt]
}

type payloadPreparationAttempt struct {
	firstSlot, lastSlot uint64
	cancel              context.CancelCauseFunc
	interrupted         atomic.Bool
}

func (p *PayloadPreparationImports) BeginImport(slot uint64) func() {
	key := &slot
	p.active.Store(key, slot)
	if attempt := p.preparing.Load(); attempt != nil && slot >= attempt.firstSlot && slot <= attempt.lastSlot {
		attempt.interrupted.Store(true)
		attempt.cancel(ErrPayloadImportInProgress)
	}
	return func() { p.active.Delete(key) }
}

func (p *PayloadPreparationImports) ImportInProgress(slot uint64) bool {
	active := false
	p.active.Range(func(_, value any) bool {
		active = value.(uint64) == slot
		return !active
	})
	return active
}

func (p *PayloadPreparationImports) BeginPendingEnvelope(blockRoot common.Hash) func() {
	key := &blockRoot
	p.pendingEnvelopes.Store(key, blockRoot)
	return func() { p.pendingEnvelopes.Delete(key) }
}

// TryBegin returns a finish callback that reports import interruption independently of context cancellation.
func (p *PayloadPreparationImports) TryBegin(
	ctx context.Context,
	firstSlot, lastSlot uint64,
	pending func() bool,
	blockSlot func(common.Hash) (uint64, bool),
) (context.Context, func() bool, bool) {
	ctx, cancel := context.WithCancelCause(ctx)
	attempt := &payloadPreparationAttempt{firstSlot: firstSlot, lastSlot: lastSlot, cancel: cancel}
	// Publish before sampling active imports so each import is either sampled or cancels this attempt.
	if !p.preparing.CompareAndSwap(nil, attempt) {
		cancel(ErrPayloadImportInProgress)
		return ctx, nil, false
	}
	finish := func() bool {
		p.preparing.CompareAndSwap(attempt, nil)
		cancel(context.Canceled)
		return attempt.interrupted.Load()
	}
	available := true
	p.active.Range(func(_, value any) bool {
		slot := value.(uint64)
		available = slot < firstSlot || slot > lastSlot
		return available
	})
	if available && blockSlot != nil {
		p.pendingEnvelopes.Range(func(_, value any) bool {
			slot, known := blockSlot(value.(common.Hash))
			// Include the head slot so queued gossip envelopes get import priority before EMPTY fallback preparation.
			available = !known || slot < firstSlot || slot > lastSlot
			return available
		})
	}
	available = available && (pending == nil || !pending())
	if !available || ctx.Err() != nil {
		p.preparing.CompareAndSwap(attempt, nil)
		cancel(ErrPayloadImportInProgress)
		return ctx, nil, false
	}
	return ctx, finish, true
}

func (f *ForkChoiceStore) BeginPayloadImport(slot uint64) func() {
	return f.payloadPreparation.BeginImport(slot)
}

func (f *ForkChoiceStore) PayloadImportInProgress(slot uint64) bool {
	return f.payloadPreparation.ImportInProgress(slot)
}

func (f *ForkChoiceStore) BeginPendingPayloadEnvelope(blockRoot common.Hash) func() {
	return f.payloadPreparation.BeginPendingEnvelope(blockRoot)
}

func (f *ForkChoiceStore) TryBeginPayloadPreparation(ctx context.Context, firstSlot, lastSlot uint64) (context.Context, func() bool, bool) {
	return f.payloadPreparation.TryBegin(ctx, firstSlot, lastSlot, func() bool {
		for _, queue := range []*lru.Cache[common.Hash, *cltypes.SignedExecutionPayloadEnvelope]{f.pendingEnvelopes, f.pendingLocalSelfBuildEnvelopes} {
			if queue == nil {
				continue
			}
			for _, root := range queue.Keys() {
				slot, known := f.payloadPreparationBlockSlot(root)
				// The selected head's parked envelope may never import; preparation primes its EMPTY fallback,
				// and an import that starts cancels the attempt through BeginImport.
				if known && slot > firstSlot && slot <= lastSlot {
					return true
				}
			}
		}
		return false
	}, f.payloadPreparationBlockSlot)
}

func (f *ForkChoiceStore) payloadPreparationBlockSlot(root common.Hash) (uint64, bool) {
	if f.forkGraph.HasEnvelope(root) {
		return 0, false
	}
	block, ok := f.forkGraph.GetBlock(root)
	if !ok || block == nil || block.Block == nil {
		return 0, false
	}
	return block.Block.Slot, true
}
