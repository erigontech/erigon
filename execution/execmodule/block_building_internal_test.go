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
	"fmt"
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
	"github.com/erigontech/erigon/txnprovider"
)

func TestAssembleBlockKeepsBuildersApartByTimestamp(t *testing.T) {
	type runningBuilder struct {
		id        uint64
		interrupt *atomic.Bool
	}
	started := make(chan runningBuilder, 4)
	module := &ExecModule{
		semaphore:    semaphore.NewWeighted(1),
		config:       &chain.Config{},
		logger:       log.Root(),
		builders:     map[uint64]*builderEntry{},
		bacgroundCtx: t.Context(),
		builderFunc: func(_ context.Context, params *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
			started <- runningBuilder{id: params.PayloadId, interrupt: interrupt}
			for !interrupt.Load() {
				time.Sleep(time.Millisecond)
			}
			return nil, errors.New("builder stopped")
		},
	}
	t.Cleanup(func() {
		for _, entry := range module.builders {
			if entry != nil && entry.builder != nil {
				_, _ = entry.builder.Stop(context.Background())
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

	firstDuplicate, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)
	require.Equal(t, firstID, firstDuplicate.PayloadID)

	// Superseding hands the timestamp to a new builder and leaves the old ones running, so only the
	// index moves. Timestamp 101 is a different proposal and is untouched throughout.
	secondID, second := assemble(100, common.Hash{0x03})
	require.NotEqual(t, firstID, secondID)
	require.Equal(t, secondID, module.buildersByTimestamp[100])
	require.Equal(t, adjacentID, module.buildersByTimestamp[101])

	thirdID, third := assemble(100, common.Hash{0x04})
	require.NotEqual(t, secondID, thirdID)
	require.Equal(t, thirdID, module.buildersByTimestamp[100])

	duplicate, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x04}})
	require.NoError(t, err)
	require.Equal(t, thirdID, duplicate.PayloadID)

	for _, running := range []runningBuilder{first, second, third, adjacent} {
		require.False(t, running.interrupt.Load(), "builder %d must still be packing", running.id)
	}

	// Eviction is where a builder is actually stopped, and it takes the timestamp index with it.
	// Removing entries puts them beyond the registered cleanup, so release them here instead of
	// leaving two goroutines running until their watchdogs fire.
	module.builders[firstID].builder.Discard()
	module.builders[secondID].builder.Discard()
	delete(module.builders, firstID)
	delete(module.builders, secondID)
	for id := thirdID + 1; len(module.builders) < engine_helpers.MaxBuilders; id++ {
		module.builders[id] = nil
	}
	module.evictOldBuilders()
	require.Eventually(t, adjacent.interrupt.Load, time.Second, time.Millisecond)
	require.NotContains(t, module.builders, adjacentID)
	require.NotContains(t, module.buildersByTimestamp, uint64(101))
	require.False(t, third.interrupt.Load(), "the current builder for a timestamp must survive eviction")
}

func TestEvictionReleasesABuilderBlockedOnItsProvider(t *testing.T) {
	// A transaction provider can wait for most of a slot before returning, and the interrupt flag
	// is not read until it does. Only cancelling the build reaches it.
	entered := make(chan struct{}, 1)
	released := make(chan error, 1)
	module := &ExecModule{
		logger:       log.Root(),
		config:       &chain.Config{},
		semaphore:    semaphore.NewWeighted(1),
		builders:     map[uint64]*builderEntry{},
		bacgroundCtx: t.Context(),
		builderFunc: func(ctx context.Context, _ *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
			entered <- struct{}{}
			select {
			case <-ctx.Done():
				released <- ctx.Err()
				return nil, ctx.Err()
			case <-time.After(time.Minute):
				released <- errors.New("provider was never released")
				return nil, errors.New("provider was never released")
			}
		},
	}

	result, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)
	<-entered

	entry := module.builders[result.PayloadID]
	require.NotNil(t, entry)
	for id := result.PayloadID + 1; len(module.builders) < engine_helpers.MaxBuilders; id++ {
		module.builders[id] = nil
	}
	module.evictOldBuilders()
	require.NotContains(t, module.builders, result.PayloadID)

	// Observing the goroutine finish is the point: an evicted builder that merely has its flag set
	// keeps its read view open until whatever it is blocked on gives up on its own.
	select {
	case err := <-released:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("evicted builder was never released")
	}
	require.Eventually(t, func() bool { return entry.builder.Failed() }, 5*time.Second, time.Millisecond)
}

func TestSupersededBuilderKeepsPackingAndStaysRetrievable(t *testing.T) {
	started := make(chan *atomic.Bool, 4)
	module := &ExecModule{
		logger:       log.Root(),
		config:       &chain.Config{},
		semaphore:    semaphore.NewWeighted(1),
		builders:     map[uint64]*builderEntry{},
		bacgroundCtx: t.Context(),
		builderFunc: func(_ context.Context, _ *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
			started <- interrupt
			for !interrupt.Load() {
				time.Sleep(time.Millisecond)
			}
			return &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}, nil
		},
	}

	first, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)
	firstInterrupt := <-started

	second, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x02}})
	require.NoError(t, err)
	require.NotEqual(t, first.PayloadID, second.PayloadID)
	secondInterrupt := <-started

	// The timestamp index moves to the new builder, so nothing reaches the old one by dedup. It is
	// left running: freezing it would answer an id already handed out with a near-empty payload.
	require.Equal(t, second.PayloadID, module.buildersByTimestamp[100])
	require.False(t, firstInterrupt.Load())

	assembled, err := module.GetAssembledBlock(t.Context(), first.PayloadID)
	require.NoError(t, err)
	require.NotNil(t, assembled.Block)

	secondInterrupt.Store(true)
}

func TestCollectedPayloadIsHandedBackToARepeatedRequest(t *testing.T) {
	started := make(chan struct{}, 4)
	module := &ExecModule{
		logger:       log.Root(),
		config:       &chain.Config{},
		semaphore:    semaphore.NewWeighted(1),
		builders:     map[uint64]*builderEntry{},
		bacgroundCtx: t.Context(),
		builderFunc: func(_ context.Context, _ *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
			started <- struct{}{}
			for !interrupt.Load() {
				time.Sleep(time.Millisecond)
			}
			return &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}, nil
		},
	}

	params := func() *builder.Parameters {
		return &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x01}}
	}
	first, err := module.AssembleBlock(t.Context(), params())
	require.NoError(t, err)
	<-started

	assembled, err := module.GetAssembledBlock(t.Context(), first.PayloadID)
	require.NoError(t, err)
	require.NotNil(t, assembled.Block)

	// Collecting stops the builder. A repeated request must still be handed that payload: rebuilding
	// from scratch this late means the next grab takes a near-empty block.
	repeat, err := module.AssembleBlock(t.Context(), params())
	require.NoError(t, err)
	require.Equal(t, first.PayloadID, repeat.PayloadID)
	require.Empty(t, started, "a repeated request must not start a second builder")
}

func TestAssembleBlockDoesNotReuseFailedBuilder(t *testing.T) {
	var failNext atomic.Bool
	failNext.Store(true)
	started := make(chan struct{}, 4)
	module := &ExecModule{
		logger:       log.Root(),
		config:       &chain.Config{},
		semaphore:    semaphore.NewWeighted(1),
		builders:     map[uint64]*builderEntry{},
		bacgroundCtx: t.Context(),
		builderFunc: func(_ context.Context, _ *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
			started <- struct{}{}
			if failNext.Swap(false) {
				return nil, errors.New("build failed")
			}
			for !interrupt.Load() {
				time.Sleep(time.Millisecond)
			}
			return nil, errors.New("builder stopped")
		},
	}

	params := func() *builder.Parameters {
		return &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x01}}
	}
	first, err := module.AssembleBlock(t.Context(), params())
	require.NoError(t, err)
	<-started
	require.Eventually(t, module.builders[first.PayloadID].builder.Failed, time.Second, time.Millisecond)

	// Identical parameters would normally dedup onto the same id. A builder that already died
	// latches its error, so reusing it would spend the slot on a payload that can never arrive.
	second, err := module.AssembleBlock(t.Context(), params())
	require.NoError(t, err)
	require.NotEqual(t, first.PayloadID, second.PayloadID)
	<-started

	_, _ = module.builders[second.PayloadID].builder.Stop(context.Background())
}

func TestGetAssembledBlockDropsFailedBuilder(t *testing.T) {
	module := &ExecModule{
		logger:       log.Root(),
		config:       &chain.Config{},
		semaphore:    semaphore.NewWeighted(1),
		builders:     map[uint64]*builderEntry{},
		bacgroundCtx: t.Context(),
		builderFunc: func(_ context.Context, _ *builder.Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
			return nil, errors.New("build failed")
		},
	}

	result, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)

	_, err = module.GetAssembledBlock(t.Context(), result.PayloadID)
	require.Error(t, err)

	// The error is latched, so leaving the entry in place would keep serving it to every retry.
	require.NotContains(t, module.builders, result.PayloadID)
	require.NotContains(t, module.buildersByTimestamp, uint64(100))
}

func TestGetAssembledBlockKeepsBuilderWhenTheCallerGivesUpMidStop(t *testing.T) {
	interrupted := make(chan struct{})
	release := make(chan struct{})
	module := &ExecModule{
		logger:       log.Root(),
		config:       &chain.Config{},
		semaphore:    semaphore.NewWeighted(1),
		builders:     map[uint64]*builderEntry{},
		bacgroundCtx: t.Context(),
		builderFunc: func(_ context.Context, _ *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
			for !interrupt.Load() {
				time.Sleep(time.Millisecond)
			}
			close(interrupted)
			<-release
			return &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}, nil
		},
	}

	result, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)

	// Cancel while Stop is already waiting, which is the window a caller-side timeout actually
	// lands in. Cancelling beforehand returns at the entry check and exercises none of this.
	ctx, cancel := context.WithCancel(t.Context())
	collected := make(chan error, 1)
	go func() {
		_, collectErr := module.GetAssembledBlock(ctx, result.PayloadID)
		collected <- collectErr
	}()
	<-interrupted
	cancel()
	require.ErrorIs(t, <-collected, context.Canceled)

	// The builder was not dropped, so the payload it goes on to produce is still reachable.
	require.Contains(t, module.builders, result.PayloadID)
	require.Equal(t, result.PayloadID, module.buildersByTimestamp[100])

	close(release)
	assembled, err := module.GetAssembledBlock(t.Context(), result.PayloadID)
	require.NoError(t, err)
	require.NotNil(t, assembled.Block)
}

// mutableTxnProvider stands in for the stateful providers the testing namespace supplies: it hands
// its transactions over once and clears them, from the build goroutine.
type mutableTxnProvider struct {
	txns []types.Transaction
	done atomic.Bool
}

func (m *mutableTxnProvider) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	if !m.done.CompareAndSwap(false, true) {
		return nil, nil
	}
	txns := m.txns
	m.txns = nil
	return txns, nil
}

func TestAssembleBlockNeverReusesABuilderWithACustomProvider(t *testing.T) {
	started := make(chan struct{}, 4)
	module := &ExecModule{
		logger:       log.Root(),
		config:       &chain.Config{},
		semaphore:    semaphore.NewWeighted(1),
		builders:     map[uint64]*builderEntry{},
		bacgroundCtx: t.Context(),
		builderFunc: func(ctx context.Context, params *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
			started <- struct{}{}
			// Keep the provider busy for the whole test, which is when a comparison would read it.
			for !interrupt.Load() && ctx.Err() == nil {
				if params.CustomTxnProvider != nil {
					_, _ = params.CustomTxnProvider.ProvideTxns(ctx)
				}
				time.Sleep(time.Millisecond)
			}
			return nil, errors.New("builder stopped")
		},
	}

	withProvider := func() *builder.Parameters {
		return &builder.Parameters{
			Timestamp:         100,
			ParentHash:        common.Hash{0x01},
			CustomTxnProvider: &mutableTxnProvider{txns: []types.Transaction{}},
		}
	}
	first, err := module.AssembleBlock(t.Context(), withProvider())
	require.NoError(t, err)
	<-started

	// The provider is single-shot and mutates as it runs, so a second request carrying one is not
	// asking for what the first is building, and its fields must never be compared.
	second, err := module.AssembleBlock(t.Context(), withProvider())
	require.NoError(t, err)
	require.NotEqual(t, first.PayloadID, second.PayloadID)
	<-started

	for _, entry := range module.builders {
		entry.builder.Discard()
	}
}

func TestAssembleBlockOwnsParameters(t *testing.T) {
	type observedParameters struct {
		parentRoot common.Hash
		extraData  byte
	}
	readParameters := make(chan struct{})
	observed := make(chan observedParameters, 1)
	module := &ExecModule{
		semaphore:    semaphore.NewWeighted(1),
		config:       &chain.Config{},
		logger:       log.Root(),
		builders:     map[uint64]*builderEntry{},
		bacgroundCtx: t.Context(),
		builderFunc: func(_ context.Context, params *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
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
	_, _ = module.builders[result.PayloadID].builder.Stop(context.Background())
}

func TestAssembleBlockCanceledContextDoesNotSupersedeBuilder(t *testing.T) {
	started := make(chan *atomic.Bool, 1)
	module := &ExecModule{
		semaphore:    semaphore.NewWeighted(1),
		config:       &chain.Config{},
		logger:       log.Root(),
		builders:     map[uint64]*builderEntry{},
		bacgroundCtx: t.Context(),
		builderFunc: func(_ context.Context, _ *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
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
		_, _ = module.builders[result.PayloadID].builder.Stop(context.Background())
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

func TestGetAssembledBlockDropsABuildThatFailedWithAContextError(t *testing.T) {
	module := &ExecModule{
		logger:       log.Root(),
		config:       &chain.Config{},
		semaphore:    semaphore.NewWeighted(1),
		builders:     map[uint64]*builderEntry{},
		bacgroundCtx: t.Context(),
		builderFunc: func(context.Context, *builder.Parameters, *atomic.Bool) (*types.BlockWithReceipts, error) {
			// A transaction provider that gives up reports its own context error, which is a failed
			// build rather than a caller that stopped waiting.
			return nil, fmt.Errorf("issue while waiting for parent block: %w", context.DeadlineExceeded)
		},
	}

	result, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: 100, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)

	_, err = module.GetAssembledBlock(t.Context(), result.PayloadID)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Keeping it would serve that latched error to every later retry of the same slot.
	require.NotContains(t, module.builders, result.PayloadID)
	require.NotContains(t, module.buildersByTimestamp, uint64(100))
}
