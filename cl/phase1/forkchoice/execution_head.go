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

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
)

// GetExecutionHead resolves the execution hash for a Gloas fork-choice node.
// The boolean means the hash was resolved, not that the EL verified or adopted it.
// Canonical FCU permits an unverified FULL head; preparation checks verification separately.
func (f *ForkChoiceStore) GetExecutionHead(head ForkChoiceNode) (common.Hash, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	bid := f.executionPayloadBid(head.Root)
	if bid == nil {
		return common.Hash{}, false
	}
	if head.PayloadStatus == cltypes.PayloadStatusFull {
		return bid.BlockHash, true
	}
	return bid.ParentBlockHash, true
}

// GetHeadExecutionHash is the execution hash an FCU names for the head: the resolved bid hash,
// or the block's cached hash when no bid resolves.
func (f *ForkChoiceStore) GetHeadExecutionHash(head ForkChoiceNode) common.Hash {
	if hash, ok := f.GetExecutionHead(head); ok {
		return hash
	}
	return f.GetEth1Hash(head.Root)
}

// LockExecutionHead orders callers that resolve the head's execution hash and send its FCU.
func (f *ForkChoiceStore) LockExecutionHead(ctx context.Context) (unlock func(), err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case f.executionHeadLock <- struct{}{}:
		return func() { <-f.executionHeadLock }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TryLockExecutionHead is LockExecutionHead that gives up at once when the lock is held.
func (f *ForkChoiceStore) TryLockExecutionHead() (unlock func(), ok bool) {
	select {
	case f.executionHeadLock <- struct{}{}:
		return func() { <-f.executionHeadLock }, true
	default:
		return nil, false
	}
}

// ForkChoiceUpdateApplied reports whether an FCU result counts as applied: a success or a timeout.
// A busy or failed FCU does not.
func ForkChoiceUpdateApplied(err error) bool {
	return err == nil || errors.Is(err, execution_client.ErrForkChoiceUpdateTimeout)
}

// RecordExecutionHead stores the execution head decided under the execution head lock if its FCU was applied.
func (f *ForkChoiceStore) RecordExecutionHead(hash common.Hash, fcuErr error) {
	if ForkChoiceUpdateApplied(fcuErr) {
		f.lastExecutionHead.Store(hash)
	}
}

// LastExecutionHead is the hash of the last FCU recorded by RecordExecutionHead.
func (f *ForkChoiceStore) LastExecutionHead() common.Hash {
	hash, _ := f.lastExecutionHead.Load().(common.Hash)
	return hash
}
