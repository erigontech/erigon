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

// newTestModule builds a module whose builders run builderFunc. Every test needs the same fields,
// and getting the config or the context wrong changes the builder's own deadline silently.
func newTestModule(t *testing.T, builderFunc builder.BlockBuilderFunc) *ExecModule {
	t.Helper()
	return &ExecModule{
		logger:              log.Root(),
		config:              &chain.Config{},
		semaphore:           semaphore.NewWeighted(1),
		builders:            map[uint64]*builderEntry{},
		buildersByTimestamp: map[uint64]uint64{},
		backgroundCtx:       t.Context(),
		builderFunc:         builderFunc,
	}
}

// newTestTimestamp is a slot that has not happened yet but is close enough to be one a proposal could
// be waiting for. Far enough ahead that buildDuration gives a budget measured in slots, so the
// max-build-time watchdog cannot fire mid-test, and near enough that the slot still counts as live.
func newTestTimestamp() uint64 {
	return uint64(time.Now().Add(10 * time.Second).Unix())
}

func TestAssembleBlockKeepsBuildersApartByTimestamp(t *testing.T) {
	timestamp := newTestTimestamp()
	type runningBuilder struct {
		id        uint64
		interrupt *atomic.Bool
	}
	started := make(chan runningBuilder, 4)
	module := newTestModule(t, func(_ context.Context, params *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		started <- runningBuilder{id: params.PayloadId, interrupt: interrupt}
		for !interrupt.Load() {
			time.Sleep(time.Millisecond)
		}
		return nil, errors.New("builder stopped")
	})
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

	firstID, first := assemble(timestamp, common.Hash{0x01})
	adjacentID, adjacent := assemble(timestamp+1, common.Hash{0x02})
	require.NotEqual(t, firstID, adjacentID)

	firstDuplicate, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)
	require.Equal(t, firstID, firstDuplicate.PayloadID)

	// Superseding moves only the timestamp index; the old builders keep running. The adjacent
	// timestamp is a different proposal and is untouched throughout.
	secondID, second := assemble(timestamp, common.Hash{0x03})
	require.NotEqual(t, firstID, secondID)
	require.Equal(t, secondID, module.buildersByTimestamp[timestamp])
	require.Equal(t, adjacentID, module.buildersByTimestamp[timestamp+1])

	thirdID, third := assemble(timestamp, common.Hash{0x04})
	require.NotEqual(t, secondID, thirdID)
	require.Equal(t, thirdID, module.buildersByTimestamp[timestamp])

	duplicate, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x04}})
	require.NoError(t, err)
	require.Equal(t, thirdID, duplicate.PayloadID)

	for _, running := range []runningBuilder{first, second, third, adjacent} {
		require.False(t, running.interrupt.Load(), "builder %d must still be packing", running.id)
	}

	// Eviction goes by age but skips what a timestamp still resolves to: first and second have been
	// superseded, third and adjacent have not.
	for id := thirdID + 1; len(module.builders) < engine_helpers.MaxBuilders; id++ {
		module.builders[id] = nil
	}
	module.evictOldBuilders()

	require.NotContains(t, module.builders, firstID)
	require.Eventually(t, first.interrupt.Load, time.Second, time.Millisecond)
	require.Contains(t, module.builders, adjacentID, "the current builder for a timestamp must survive eviction")
	require.Contains(t, module.builders, thirdID)
	require.False(t, adjacent.interrupt.Load())
	require.False(t, third.interrupt.Load())
	require.Equal(t, thirdID, module.buildersByTimestamp[timestamp])
	require.Equal(t, adjacentID, module.buildersByTimestamp[timestamp+1])
}

func TestEvictionReleasesABuilderBlockedOnItsProvider(t *testing.T) {
	timestamp := newTestTimestamp()
	// A transaction provider can wait for most of a slot before returning, and the interrupt flag
	// is not read until it does. Only cancelling the build reaches it.
	entered := make(chan struct{}, 1)
	released := make(chan error, 1)
	module := newTestModule(t, func(ctx context.Context, _ *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		entered <- struct{}{}
		select {
		case <-ctx.Done():
			released <- ctx.Err()
			return nil, ctx.Err()
		case <-time.After(time.Minute):
			released <- errors.New("provider was never released")
			return nil, errors.New("provider was never released")
		}
	})

	blocked, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)
	<-entered

	// Superseded, so eviction is allowed to take it: what a timestamp still resolves to is exempt.
	_, err = module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x02}})
	require.NoError(t, err)
	<-entered

	entry := module.builders[blocked.PayloadID]
	require.NotNil(t, entry)
	for id := uint64(len(module.builders)) + 100; len(module.builders) < engine_helpers.MaxBuilders; id++ {
		module.builders[id] = nil
	}
	module.evictOldBuilders()
	require.NotContains(t, module.builders, blocked.PayloadID)

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
	timestamp := newTestTimestamp()
	started := make(chan *atomic.Bool, 4)
	module := newTestModule(t, func(_ context.Context, _ *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		started <- interrupt
		for !interrupt.Load() {
			time.Sleep(time.Millisecond)
		}
		return &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}, nil
	})

	first, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)
	firstInterrupt := <-started

	second, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x02}})
	require.NoError(t, err)
	require.NotEqual(t, first.PayloadID, second.PayloadID)
	secondInterrupt := <-started

	// Superseding moves only the index; the old builder keeps running and its id stays retrievable.
	require.Equal(t, second.PayloadID, module.buildersByTimestamp[timestamp])
	require.False(t, firstInterrupt.Load())

	assembled, err := module.GetAssembledBlock(t.Context(), first.PayloadID)
	require.NoError(t, err)
	require.NotNil(t, assembled.Block)

	secondInterrupt.Store(true)
}

func TestCollectedPayloadIsHandedBackToARepeatedRequest(t *testing.T) {
	timestamp := newTestTimestamp()
	started := make(chan struct{}, 4)
	module := newTestModule(t, func(_ context.Context, _ *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		started <- struct{}{}
		for !interrupt.Load() {
			time.Sleep(time.Millisecond)
		}
		return &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}, nil
	})

	params := func() *builder.Parameters {
		return &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x01}}
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
	timestamp := newTestTimestamp()
	var failNext atomic.Bool
	failNext.Store(true)
	started := make(chan struct{}, 4)
	module := newTestModule(t, func(_ context.Context, _ *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		started <- struct{}{}
		if failNext.Swap(false) {
			return nil, errors.New("build failed")
		}
		for !interrupt.Load() {
			time.Sleep(time.Millisecond)
		}
		return nil, errors.New("builder stopped")
	})

	params := func() *builder.Parameters {
		return &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x01}}
	}
	first, err := module.AssembleBlock(t.Context(), params())
	require.NoError(t, err)
	<-started
	require.Eventually(t, module.builders[first.PayloadID].builder.Failed, time.Second, time.Millisecond)

	// Identical parameters would normally dedup onto the same id, but a failed builder latches its
	// error and has to be passed over.
	second, err := module.AssembleBlock(t.Context(), params())
	require.NoError(t, err)
	require.NotEqual(t, first.PayloadID, second.PayloadID)
	<-started

	_, _ = module.builders[second.PayloadID].builder.Stop(context.Background())
}

func TestGetAssembledBlockDropsFailedBuilder(t *testing.T) {
	timestamp := newTestTimestamp()
	module := newTestModule(t, func(_ context.Context, _ *builder.Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		return nil, errors.New("build failed")
	})

	result, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)

	_, err = module.GetAssembledBlock(t.Context(), result.PayloadID)
	require.Error(t, err)

	// The error is latched, so leaving the entry in place would keep serving it to every retry.
	require.NotContains(t, module.builders, result.PayloadID)
	require.NotContains(t, module.buildersByTimestamp, timestamp)
}

func TestGetAssembledBlockKeepsBuilderWhenTheCallerGivesUpMidStop(t *testing.T) {
	timestamp := newTestTimestamp()
	interrupted := make(chan struct{})
	release := make(chan struct{})
	module := newTestModule(t, func(_ context.Context, _ *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		for !interrupt.Load() {
			time.Sleep(time.Millisecond)
		}
		close(interrupted)
		<-release
		return &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}, nil
	})

	result, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x01}})
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
	require.Equal(t, result.PayloadID, module.buildersByTimestamp[timestamp])

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
	module := newTestModule(t, func(ctx context.Context, params *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		started <- struct{}{}
		// Keep the provider busy for the whole test, which is when a comparison would read it.
		for !interrupt.Load() && ctx.Err() == nil {
			if params.CustomTxnProvider != nil {
				_, _ = params.CustomTxnProvider.ProvideTxns(ctx)
			}
			time.Sleep(time.Millisecond)
		}
		return nil, errors.New("builder stopped")
	})

	timestamp := newTestTimestamp()
	withProvider := func() *builder.Parameters {
		return &builder.Parameters{
			Timestamp:         timestamp,
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
	module := newTestModule(t, func(_ context.Context, params *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		<-readParameters
		observed <- observedParameters{parentRoot: *params.ParentBeaconBlockRoot, extraData: params.ExtraData[0]}
		for !interrupt.Load() {
			time.Sleep(time.Millisecond)
		}
		return nil, errors.New("builder stopped")
	})
	timestamp := newTestTimestamp()
	root := common.Hash{0xaa}
	params := &builder.Parameters{
		Timestamp:             timestamp,
		ParentHash:            common.Hash{0x01},
		ParentBeaconBlockRoot: &root,
		ExtraData:             []byte{0xbb},
	}
	result, err := module.AssembleBlock(t.Context(), params)
	require.NoError(t, err)
	require.False(t, result.Busy)
	require.Zero(t, params.PayloadId, "the caller's parameters are not the module's to write to")

	root[0] = 0xcc
	params.ExtraData[0] = 0xdd
	close(readParameters)
	require.Equal(t, observedParameters{parentRoot: common.Hash{0xaa}, extraData: 0xbb}, <-observed)

	duplicate, err := module.AssembleBlock(t.Context(), &builder.Parameters{
		Timestamp:             timestamp,
		ParentHash:            common.Hash{0x01},
		ParentBeaconBlockRoot: &common.Hash{0xaa},
		ExtraData:             []byte{0xbb},
	})
	require.NoError(t, err)
	require.Equal(t, result.PayloadID, duplicate.PayloadID)
	_, _ = module.builders[result.PayloadID].builder.Stop(context.Background())
}

func TestAssembleBlockCanceledContextDoesNotSupersedeBuilder(t *testing.T) {
	timestamp := newTestTimestamp()
	started := make(chan *atomic.Bool, 1)
	module := newTestModule(t, func(_ context.Context, _ *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		started <- interrupt
		for !interrupt.Load() {
			time.Sleep(time.Millisecond)
		}
		return nil, errors.New("builder stopped")
	})
	result, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)
	interrupt := <-started
	t.Cleanup(func() {
		_, _ = module.builders[result.PayloadID].builder.Stop(context.Background())
	})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = module.AssembleBlock(ctx, &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x02}})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, interrupt.Load())
	require.Equal(t, result.PayloadID, module.buildersByTimestamp[timestamp])
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

func TestStopGraceDuration(t *testing.T) {
	// The grace has to fit a heavy transaction plus the packing tail and finalization, while still
	// bounding how long a stuck build can hold its read view past its budget.
	require.Equal(t, 6*time.Second, stopGraceDuration(12))
	require.Equal(t, 2500*time.Millisecond, stopGraceDuration(5))
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
	timestamp := newTestTimestamp()
	module := newTestModule(t, func(context.Context, *builder.Parameters, *atomic.Bool) (*types.BlockWithReceipts, error) {
		// A transaction provider that gives up reports its own context error, which is a failed
		// build rather than a caller that stopped waiting.
		return nil, fmt.Errorf("issue while waiting for parent block: %w", context.DeadlineExceeded)
	})

	result, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)

	_, err = module.GetAssembledBlock(t.Context(), result.PayloadID)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Keeping it would serve that latched error to every later retry of the same slot.
	require.NotContains(t, module.builders, result.PayloadID)
	require.NotContains(t, module.buildersByTimestamp, timestamp)
}

func TestGetAssembledBlockSaysWhenAPayloadIdIsUnknown(t *testing.T) {
	module := newTestModule(t, func(context.Context, *builder.Parameters, *atomic.Bool) (*types.BlockWithReceipts, error) {
		return nil, errors.New("builder stopped")
	})

	// An id with no builder behind it can never produce anything. Reporting it as an ordinary empty
	// result leaves a caller polling it for the rest of the slot.
	assembled, err := module.GetAssembledBlock(t.Context(), 404)
	require.NoError(t, err)
	require.True(t, assembled.Unknown)
	require.Nil(t, assembled.Block)
}

func TestEvictionSparesTheBuilderATimestampStillPointsAt(t *testing.T) {
	timestamp := newTestTimestamp()
	started := make(chan struct{}, 1)
	module := newTestModule(t, func(_ context.Context, _ *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		started <- struct{}{}
		for !interrupt.Load() {
			time.Sleep(time.Millisecond)
		}
		return nil, errors.New("builder stopped")
	})

	current, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)
	<-started

	// It is the oldest by id, so eviction would take it first, and it is also what a proposal for
	// that timestamp is waiting on. Age says nothing about that.
	for id := current.PayloadID + 1; len(module.builders) < engine_helpers.MaxBuilders+1; id++ {
		module.builders[id] = nil
	}
	module.evictOldBuilders()

	require.Contains(t, module.builders, current.PayloadID)
	require.Equal(t, current.PayloadID, module.buildersByTimestamp[timestamp])
	require.False(t, module.builders[current.PayloadID].builder.Failed())

	module.builders[current.PayloadID].builder.Discard()
}

func TestAssembleBlockDoesNotReuseADiscardedBuilder(t *testing.T) {
	timestamp := newTestTimestamp()
	started := make(chan struct{}, 2)
	module := newTestModule(t, func(ctx context.Context, _ *builder.Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		started <- struct{}{}
		<-ctx.Done()
		// The goroutine outliving the discard is the point: the builder must read as gone at once,
		// not only once its work notices the cancellation.
		time.Sleep(200 * time.Millisecond)
		return nil, ctx.Err()
	})

	params := func() *builder.Parameters {
		return &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x01}}
	}
	first, err := module.AssembleBlock(t.Context(), params())
	require.NoError(t, err)
	<-started

	module.builders[first.PayloadID].builder.Discard()

	second, err := module.AssembleBlock(t.Context(), params())
	require.NoError(t, err)
	require.NotEqual(t, first.PayloadID, second.PayloadID)
	<-started
	module.builders[second.PayloadID].builder.Discard()
}

func TestGetAssembledBlockReportsADiscardedBuilderAsUnknown(t *testing.T) {
	timestamp := newTestTimestamp()
	started := make(chan struct{}, 1)
	module := newTestModule(t, func(ctx context.Context, _ *builder.Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		started <- struct{}{}
		<-ctx.Done()
		time.Sleep(200 * time.Millisecond)
		return nil, ctx.Err()
	})

	result, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)
	<-started
	module.builders[result.PayloadID].builder.Discard()

	assembled, err := module.GetAssembledBlock(t.Context(), result.PayloadID)
	require.NoError(t, err)
	require.True(t, assembled.Unknown)
	require.NotContains(t, module.builders, result.PayloadID)
	require.NotContains(t, module.buildersByTimestamp, timestamp)
}

func TestGetAssembledBlockReportsDiscardDuringStopAsUnknown(t *testing.T) {
	timestamp := newTestTimestamp()
	stopObserved := make(chan struct{})
	module := newTestModule(t, func(ctx context.Context, _ *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		for !interrupt.Load() {
			time.Sleep(time.Millisecond)
		}
		close(stopObserved)
		<-ctx.Done()
		return nil, ctx.Err()
	})

	assembled, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)

	type response struct {
		result AssembledBlockResult
		err    error
	}
	collected := make(chan response, 1)
	go func() {
		result, err := module.GetAssembledBlock(t.Context(), assembled.PayloadID)
		collected <- response{result: result, err: err}
	}()
	<-stopObserved
	module.builders[assembled.PayloadID].builder.Discard()

	result := <-collected
	require.NoError(t, result.err)
	require.True(t, result.result.Unknown)
	require.NotContains(t, module.builders, assembled.PayloadID)
	require.NotContains(t, module.buildersByTimestamp, timestamp)
}

func TestEvictionTakesAFailedCurrentBuilderBeforeALiveSupersededOne(t *testing.T) {
	timestamp := newTestTimestamp()
	started := make(chan struct{}, 4)
	module := newTestModule(t, func(_ context.Context, params *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		started <- struct{}{}
		if params.ParentHash == (common.Hash{0xff}) {
			return nil, errors.New("build failed")
		}
		for !interrupt.Load() {
			time.Sleep(time.Millisecond)
		}
		return &types.BlockWithReceipts{Block: types.NewBlock(&types.Header{}, nil, nil, nil, nil)}, nil
	})
	t.Cleanup(func() {
		for _, entry := range module.builders {
			if entry != nil && entry.builder != nil {
				entry.builder.Discard()
			}
		}
	})

	failed, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp, ParentHash: common.Hash{0xff}})
	require.NoError(t, err)
	<-started
	require.Eventually(t, module.builders[failed.PayloadID].builder.Failed, time.Second, time.Millisecond)

	superseded, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp + 1, ParentHash: common.Hash{0x01}})
	require.NoError(t, err)
	<-started
	_, err = module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: timestamp + 1, ParentHash: common.Hash{0x02}})
	require.NoError(t, err)
	<-started

	for id := uint64(1000); len(module.builders) < engine_helpers.MaxBuilders; id++ {
		module.builders[id] = nil
	}
	module.evictOldBuilders()

	require.NotContains(t, module.builders, failed.PayloadID)
	require.Contains(t, module.builders, superseded.PayloadID)
}

func TestAssembleBlockRefusesAfterShutdown(t *testing.T) {
	module := newTestModule(t, func(ctx context.Context, _ *builder.Parameters, _ *atomic.Bool) (*types.BlockWithReceipts, error) {
		return nil, ctx.Err()
	})
	shutdownCtx, cancel := context.WithCancel(t.Context())
	module.backgroundCtx = shutdownCtx
	cancel()

	_, err := module.AssembleBlock(t.Context(), &builder.Parameters{Timestamp: newTestTimestamp(), ParentHash: common.Hash{0x01}})
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, module.builders)
}

func TestEvictionKeepsTheBuilderCacheBounded(t *testing.T) {
	module := newTestModule(t, func(context.Context, *builder.Parameters, *atomic.Bool) (*types.BlockWithReceipts, error) {
		return nil, errors.New("builder stopped")
	})

	// Ordinary traffic is one builder per slot, so every entry is the one its own timestamp
	// resolves to. If being indexed were enough to protect an entry, nothing would ever be evicted
	// and both maps would grow for the life of the process.
	past := uint64(time.Now().Add(-time.Hour).Unix())
	for i := range uint64(engine_helpers.MaxBuilders + 8) {
		_, err := module.AssembleBlock(t.Context(), &builder.Parameters{
			Timestamp:  past + i,
			ParentHash: common.Hash{byte(i), byte(i >> 8)},
		})
		require.NoError(t, err)
	}

	require.LessOrEqual(t, len(module.builders), engine_helpers.MaxBuilders)
	require.LessOrEqual(t, len(module.buildersByTimestamp), engine_helpers.MaxBuilders)
}
