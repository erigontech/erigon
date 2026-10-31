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

package services

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/common"
)

// lateBlockForkChoice hides one block, like the real store does for a block it
// has not imported yet, until arrived is set.
type lateBlockForkChoice struct {
	*mock_services.ForkChoiceStorageMock
	root          common.Hash
	arrived       atomic.Bool
	attestingSets atomic.Int32
}

func (f *lateBlockForkChoice) GetHeader(root common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	if root == f.root && !f.arrived.Load() {
		return nil, false
	}
	return f.ForkChoiceStorageMock.GetHeader(root)
}

func (f *lateBlockForkChoice) Ancestor(root common.Hash, slot uint64) forkchoice.ForkChoiceNode {
	if root == f.root && !f.arrived.Load() {
		return forkchoice.ForkChoiceNode{}
	}
	return f.ForkChoiceStorageMock.Ancestor(root, slot)
}

func (f *lateBlockForkChoice) ProcessAttestingIndicies(*solid.Attestation, []uint64) {
	f.attestingSets.Add(1)
}

func newTestBlockArrivalQueue(maxReplays int, ttl time.Duration) (*blockArrivalQueue, *mock_services.ForkChoiceStorageMock) {
	forkChoice := &mock_services.ForkChoiceStorageMock{Headers: map[common.Hash]*cltypes.BeaconBlockHeader{}}
	return newBlockArrivalQueue(forkChoice, maxReplays, ttl), forkChoice
}

func TestBlockArrivalQueueDropsReplaysBeyondCapacity(t *testing.T) {
	q, forkChoice := newTestBlockArrivalQueue(2, time.Minute)
	var replayed atomic.Int32
	root := common.Hash{1}
	for range 3 {
		q.add(root, func(context.Context) { replayed.Add(1) })
	}

	forkChoice.Headers[root] = &cltypes.BeaconBlockHeader{}
	q.replayArrived(context.Background(), time.Now())
	require.Equal(t, int32(2), replayed.Load())
}

func TestBlockArrivalQueueDropsRootsBeyondCapacity(t *testing.T) {
	q, forkChoice := newTestBlockArrivalQueue(2*maxBlockArrivalRoots, time.Minute)
	var replayed atomic.Int32
	for i := range maxBlockArrivalRoots + 1 {
		root := common.Hash{byte(i), 1}
		q.add(root, func(context.Context) { replayed.Add(1) })
		forkChoice.Headers[root] = &cltypes.BeaconBlockHeader{}
	}

	q.replayArrived(context.Background(), time.Now())
	require.Equal(t, int32(maxBlockArrivalRoots), replayed.Load())
}

func TestBlockArrivalQueueFreesCapacityAfterReplay(t *testing.T) {
	q, forkChoice := newTestBlockArrivalQueue(1, time.Minute)
	var replayed atomic.Int32
	root := common.Hash{1}
	forkChoice.Headers[root] = &cltypes.BeaconBlockHeader{}

	for range 2 {
		q.add(root, func(context.Context) { replayed.Add(1) })
		q.replayArrived(context.Background(), time.Now())
	}
	require.Equal(t, int32(2), replayed.Load())
}

func TestBlockArrivalQueueDropsExpiredReplays(t *testing.T) {
	ttl := time.Minute
	q, forkChoice := newTestBlockArrivalQueue(1, ttl)
	var expired, fresh atomic.Bool
	root := common.Hash{1}

	q.add(root, func(context.Context) { expired.Store(true) })
	q.replayArrived(context.Background(), time.Now().Add(ttl+time.Second))
	q.add(root, func(context.Context) { fresh.Store(true) })
	forkChoice.Headers[root] = &cltypes.BeaconBlockHeader{}
	q.replayArrived(context.Background(), time.Now())

	require.False(t, expired.Load(), "replay older than the ttl ran")
	require.True(t, fresh.Load(), "replay queued after the expired one did not run")
}
