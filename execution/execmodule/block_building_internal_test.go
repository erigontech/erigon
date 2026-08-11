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

package execmodule

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/types"
)

func TestAssembleBlockSupersedesBuilderForSameTimestamp(t *testing.T) {
	type runningBuilder struct {
		id        uint64
		interrupt *atomic.Bool
	}
	started := make(chan runningBuilder, 4)
	module := &ExecModule{
		semaphore: semaphore.NewWeighted(1),
		config:    &chain.Config{},
		logger:    log.Root(),
		builders:  map[uint64]*builder.BlockBuilder{},
		builderFunc: func(params *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
			started <- runningBuilder{id: params.PayloadId, interrupt: interrupt}
			for !interrupt.Load() {
				time.Sleep(time.Millisecond)
			}
			return nil, errors.New("builder stopped")
		},
	}
	t.Cleanup(func() {
		for _, blockBuilder := range module.builders {
			if blockBuilder != nil {
				_, _ = blockBuilder.Stop(context.Background())
			}
		}
	})

	waitStarted := func() runningBuilder {
		t.Helper()
		select {
		case running := <-started:
			return running
		case <-time.After(time.Second):
			t.Fatal("builder did not start")
			return runningBuilder{}
		}
	}
	assemble := func(timestamp uint64, parent common.Hash) (uint64, runningBuilder) {
		t.Helper()
		result, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp, ParentHash: parent})
		require.NoError(t, err)
		require.False(t, result.Busy)
		return result.PayloadID, waitStarted()
	}

	firstID, first := assemble(100, common.Hash{0x01})
	adjacentID, adjacent := assemble(101, common.Hash{0x02})
	require.NotEqual(t, firstID, adjacentID)
	require.False(t, first.interrupt.Load(), "a builder for another target timestamp must stay alive")

	firstDuplicate, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)
	require.Equal(t, firstID, firstDuplicate.PayloadID)
	require.False(t, first.interrupt.Load(), "an exact earlier-timestamp builder must stay alive")
	require.False(t, adjacent.interrupt.Load())

	secondID, second := assemble(100, common.Hash{0x03})
	require.NotEqual(t, firstID, secondID)
	require.Eventually(t, first.interrupt.Load, time.Second, time.Millisecond)
	require.False(t, adjacent.interrupt.Load(), "superseding timestamp 100 must not cancel timestamp 101")
	require.False(t, second.interrupt.Load())

	thirdID, third := assemble(100, common.Hash{0x04})
	require.NotEqual(t, secondID, thirdID)
	require.Eventually(t, second.interrupt.Load, time.Second, time.Millisecond)
	require.False(t, adjacent.interrupt.Load())
	require.False(t, third.interrupt.Load())

	duplicate, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x04}})
	require.NoError(t, err)
	require.Equal(t, thirdID, duplicate.PayloadID)
	require.False(t, third.interrupt.Load(), "a duplicate request must keep its builder alive")

	delete(module.builders, firstID)
	for id := thirdID + 1; len(module.builders) < engine_helpers.MaxBuilders; id++ {
		module.builders[id] = nil
	}
	module.evictOldBuilders()
	require.Eventually(t, adjacent.interrupt.Load, time.Second, time.Millisecond)
	require.NotContains(t, module.builders, adjacentID)
	require.NotContains(t, module.buildersByTimestamp, uint64(101))
	require.NotContains(t, module.builderParameters, adjacentID)
}

func TestSupersededPayloadIDStopsBeingRetrievable(t *testing.T) {
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	module := &ExecModule{
		logger:    log.Root(),
		config:    &chain.Config{},
		semaphore: semaphore.NewWeighted(1),
		builders:  map[uint64]*builder.BlockBuilder{},
		builderFunc: func(params *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
			started <- struct{}{}
			<-release
			return nil, nil
		},
	}
	t.Cleanup(func() {
		close(release)
		for _, blockBuilder := range module.builders {
			if blockBuilder != nil {
				_, _ = blockBuilder.Stop(context.Background())
			}
		}
	})

	first, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)
	<-started

	second, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x02}})
	require.NoError(t, err)
	require.NotEqual(t, first.PayloadID, second.PayloadID)

	// A cancelled builder still holds whatever it had packed; serving that would mean
	// proposing a near-empty block, so the superseded id must read as unknown instead.
	require.NotContains(t, module.builders, first.PayloadID)
	require.NotContains(t, module.builderParameters, first.PayloadID)

	assembled, err := module.GetAssembledBlock(t.Context(), first.PayloadID)
	require.NoError(t, err)
	require.Nil(t, assembled.Block)
}

func TestAssembleBlockOwnsParameters(t *testing.T) {
	type observedParameters struct {
		parentRoot common.Hash
		extraData  byte
	}
	readParameters := make(chan struct{})
	observed := make(chan observedParameters, 1)
	module := &ExecModule{
		semaphore: semaphore.NewWeighted(1),
		config:    &chain.Config{},
		logger:    log.Root(),
		builders:  map[uint64]*builder.BlockBuilder{},
		builderFunc: func(params *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
			<-readParameters
			observed <- observedParameters{parentRoot: *params.ParentBeaconBlockRoot, extraData: params.ExtraData[0]}
			for !interrupt.Load() {
				time.Sleep(time.Millisecond)
			}
			return nil, errors.New("builder stopped")
		},
	}
	root := common.Hash{0xaa}
	params := &builder.Parameters{
		Timestamp:             100,
		ParentHash:            common.Hash{0x01},
		ParentBeaconBlockRoot: &root,
		ExtraData:             []byte{0xbb},
	}
	result, err := module.AssembleBlock(t.Context(), params)
	require.NoError(t, err)
	require.False(t, result.Busy)

	root[0] = 0xcc
	params.ExtraData[0] = 0xdd
	close(readParameters)
	require.Equal(t, observedParameters{parentRoot: common.Hash{0xaa}, extraData: 0xbb}, <-observed)

	duplicate, err := module.AssembleBlock(t.Context(), &builder.Parameters{
		Timestamp:             100,
		ParentHash:            common.Hash{0x01},
		ParentBeaconBlockRoot: &common.Hash{0xaa},
		ExtraData:             []byte{0xbb},
	})
	require.NoError(t, err)
	require.Equal(t, result.PayloadID, duplicate.PayloadID)
	_, _ = module.builders[result.PayloadID].Stop(context.Background())
}

func TestAssembleBlockCanceledContextDoesNotSupersedeBuilder(t *testing.T) {
	started := make(chan *atomic.Bool, 1)
	module := &ExecModule{
		semaphore: semaphore.NewWeighted(1),
		config:    &chain.Config{},
		logger:    log.Root(),
		builders:  map[uint64]*builder.BlockBuilder{},
		builderFunc: func(_ *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
			started <- interrupt
			for !interrupt.Load() {
				time.Sleep(time.Millisecond)
			}
			return nil, errors.New("builder stopped")
		},
	}
	result, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)
	interrupt := <-started
	t.Cleanup(func() {
		_, _ = module.builders[result.PayloadID].Stop(context.Background())
	})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = module.AssembleBlock(ctx, &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x02}})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, interrupt.Load())
	require.Equal(t, result.PayloadID, module.buildersByTimestamp[100])
}

func TestCloneBuilderParametersPreservesRepresentations(t *testing.T) {
	require.Nil(t, cloneBuilderParameters(nil))

	empty := cloneBuilderParameters(&builder.Parameters{Withdrawals: []*types.Withdrawal{}, ExtraData: []byte{}})
	require.NotNil(t, empty.Withdrawals)
	require.NotNil(t, empty.ExtraData)

	root := common.Hash{0x01}
	slot := uint64(2)
	gasLimit := uint64(3)
	params := &builder.Parameters{
		Withdrawals:           []*types.Withdrawal{nil, {Index: 4}},
		ParentBeaconBlockRoot: &root,
		SlotNumber:            &slot,
		TargetGasLimit:        &gasLimit,
		ExtraData:             []byte{5},
	}
	cloned := cloneBuilderParameters(params)
	params.Withdrawals[1].Index = 40
	root[0] = 10
	slot = 20
	gasLimit = 30
	params.ExtraData[0] = 50

	require.Nil(t, cloned.Withdrawals[0])
	require.Equal(t, uint64(4), cloned.Withdrawals[1].Index)
	require.Equal(t, common.Hash{0x01}, *cloned.ParentBeaconBlockRoot)
	require.Equal(t, uint64(2), *cloned.SlotNumber)
	require.Equal(t, uint64(3), *cloned.TargetGasLimit)
	require.Equal(t, byte(5), cloned.ExtraData[0])
}

func TestBuildDuration(t *testing.T) {
	const ethereum, gnosis = uint64(12), uint64(5)
	slotStart := time.Unix(1_700_000_000, 0)
	payloadTimestamp := uint64(slotStart.Unix())

	for _, tc := range []struct {
		name           string
		secondsPerSlot uint64
		sentAt         time.Time
		want           time.Duration
	}{
		// A consensus layer sends payload attributes ahead of the slot and then calls getPayload
		// without refreshing, so the builder has to survive until that slot however early it was asked.
		{"attributes well before the slot", ethereum, slotStart.Add(-8 * time.Second), 12 * time.Second},
		{"attributes shortly before the slot", ethereum, slotStart.Add(-4 * time.Second), 8 * time.Second},
		{"attributes at production time", ethereum, slotStart.Add(400 * time.Millisecond), 3600 * time.Millisecond},

		// A request arriving too late to leave a useful window still gets the old fixed budget,
		// so late proposals are no worse off than before.
		{"very late request floors", ethereum, slotStart.Add(3900 * time.Millisecond), 3 * time.Second},

		// Bounds scale with the chain rather than assuming 12s slots.
		{"short slots, attributes before the slot", gnosis, slotStart.Add(-4 * time.Second), 4*time.Second + 5*time.Second/3},
		{"short slots, late request floors", gnosis, slotStart.Add(2 * time.Second), 1250 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, buildDuration(payloadTimestamp, tc.sentAt, tc.secondsPerSlot))
		})
	}
}

func TestBuildDurationCapsAbsurdTimestamp(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	farFuture := uint64(now.Add(time.Hour).Unix())

	// A bogus timestamp must not pin a builder, and its resources, indefinitely.
	require.Equal(t, 24*time.Second, buildDuration(farFuture, now, 12))
}

func TestBuildDurationCapsOverflowingTimestamp(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)

	// Beyond int64 seconds the conversion wraps into the past, which would collapse the budget to
	// the floor instead of the cap.
	require.Equal(t, 24*time.Second, buildDuration(math.MaxUint64, now, 12))
}
