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

package engineapi

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

// busyUntilModule reports the execution module as not ready for busyFor, the way the module's
// semaphore stays held while a previous forkchoice update's flush, commit and prune run in the
// background. The busy interval starts at the first Ready call rather than at construction, so
// slow test setup cannot use it up before the request under test arrives.
type busyUntilModule struct {
	*stubExecutionModule
	busyFor time.Duration
	once    sync.Once
	readyAt time.Time
}

func (m *busyUntilModule) Ready(context.Context) (bool, error) {
	m.once.Do(func() { m.readyAt = time.Now().Add(m.busyFor) })
	return time.Now().After(m.readyAt), nil
}

// TestForkchoiceUpdatedWithAttributesWaitsOutPostForkchoiceWork proves a forkchoiceUpdated
// carrying payload attributes for an already-known head does not come back SYNCING without a
// payload id just because the execution module is briefly busy with the previous head's
// background flush/commit/prune.
func TestForkchoiceUpdatedWithAttributesWaitsOutPostForkchoiceWork(t *testing.T) {
	t.Parallel()

	header := makeParentHeader(1000)
	headHash := header.Hash()
	headNumber := header.Number.Uint64()

	var assembleCalls int
	stub := &stubExecutionModule{
		getHeaderFunc: getHeaderReturning(headHash, header),
		headerNumberFunc: func(context.Context, common.Hash) (*uint64, error) {
			return &headNumber, nil
		},
		// The module's recorded fork choice is still the previous head, so this request does not
		// take the repeated-forkchoice shortcut.
		getForkChoiceFunc: func(context.Context) (execmodule.ForkChoiceState, error) {
			return execmodule.ForkChoiceState{HeadHash: common.Hash{0x99}}, nil
		},
		updateForkChoiceFunc: func(context.Context, common.Hash, common.Hash, common.Hash) (execmodule.ForkChoiceResult, error) {
			return execmodule.ForkChoiceResult{Status: execmodule.ExecutionStatusSuccess, LatestValidHash: headHash}, nil
		},
		assembleBlockFunc: func(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
			assembleCalls++
			return execmodule.AssembleBlockResult{PayloadID: 7}, nil
		},
	}
	// Longer than the readiness check's current half-second budget, far shorter than a slot.
	module := &busyUntilModule{stubExecutionModule: stub, busyFor: 600 * time.Millisecond}

	cfg := preCancunChainConfig()
	ctx := context.Background()
	downloader := engine_block_downloader.NewEngineBlockDownloader(ctx, log.New(), module, nil, nil, cfg, ethconfig.Sync{}, nil)
	srv := NewEngineServer(log.New(), cfg, module, downloader, false, false, true, true, nil, nil, 0, 0)

	resp, err := srv.forkchoiceUpdated(ctx, &engine_types.ForkChoiceState{HeadHash: headHash}, &engine_types.PayloadAttributes{
		Timestamp:             hexutil.Uint64(header.Time + 12),
		PrevRandao:            common.Hash{0xaa},
		SuggestedFeeRecipient: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Withdrawals:           []*types.Withdrawal{},
	}, clparams.CapellaVersion)
	require.NoError(t, err)
	require.Equal(t, engine_types.ValidStatus, resp.PayloadStatus.Status, "a known, valid head must not be reported as SYNCING while the module finishes background work")
	require.NotNil(t, resp.PayloadId, "the payload build request must not be dropped")
	require.Equal(t, 1, assembleCalls)
}

// TestForkchoiceUpdatedWithoutAttributesStillAnswersSyncingQuickly pins the other side of the
// longer wait: a plain head update loses nothing by answering SYNCING, so it keeps the short
// readiness budget instead of holding the consensus client for a slot.
func TestForkchoiceUpdatedWithoutAttributesStillAnswersSyncingQuickly(t *testing.T) {
	t.Parallel()

	header := makeParentHeader(1000)
	headHash := header.Hash()
	headNumber := header.Number.Uint64()

	stub := &stubExecutionModule{
		getHeaderFunc: getHeaderReturning(headHash, header),
		headerNumberFunc: func(context.Context, common.Hash) (*uint64, error) {
			return &headNumber, nil
		},
		getForkChoiceFunc: func(context.Context) (execmodule.ForkChoiceState, error) {
			return execmodule.ForkChoiceState{HeadHash: common.Hash{0x99}}, nil
		},
	}
	module := &busyUntilModule{stubExecutionModule: stub, busyFor: 5 * time.Second}

	cfg := preCancunChainConfig()
	ctx := context.Background()
	downloader := engine_block_downloader.NewEngineBlockDownloader(ctx, log.New(), module, nil, nil, cfg, ethconfig.Sync{}, nil)
	srv := NewEngineServer(log.New(), cfg, module, downloader, false, false, true, true, nil, nil, 0, 0)

	start := time.Now()
	resp, err := srv.forkchoiceUpdated(ctx, &engine_types.ForkChoiceState{HeadHash: headHash}, nil, clparams.CapellaVersion)
	require.NoError(t, err)
	require.Equal(t, engine_types.SyncingStatus, resp.PayloadStatus.Status)
	require.Less(t, time.Since(start), 2*time.Second)
}

// TestAttributesReadinessWaitCapsLongSlotChains pins the chain-agnostic formula: a chain's own
// slot time is used as-is unless it exceeds maxAttributesReadinessWait, in which case the cap
// applies instead.
func TestAttributesReadinessWaitCapsLongSlotChains(t *testing.T) {
	t.Parallel()

	require.Equal(t, 5*time.Second, attributesReadinessWait(5), "Gnosis/Chiado slot time is under the cap and must be used unchanged")
	require.Equal(t, 6*time.Second, attributesReadinessWait(6), "a slot time exactly at the cap must be used unchanged")
	require.Equal(t, maxAttributesReadinessWait, attributesReadinessWait(12), "mainnet/Hoodi slot time exceeds the cap and must be capped")
}

// TestWaitForResponseReturnsPromptlyOnContextCancellation proves waitForResponse stops polling as
// soon as its context is done, instead of continuing to poll until maxWait elapses.
func TestWaitForResponseReturnsPromptlyOnContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	busy, err := waitForResponse(ctx, time.Minute, func() (bool, error) {
		return true, nil
	})
	require.NoError(t, err)
	require.True(t, busy)
	require.Less(t, time.Since(start), time.Second)
}

// TestWaitForResponseBoundsTotalWaitByElapsedTime proves maxWait is a wall-clock budget that
// includes the callback's own time, starting with the first call, rather than a count of polls.
func TestWaitForResponseBoundsTotalWaitByElapsedTime(t *testing.T) {
	t.Parallel()

	start := time.Now()
	_, err := waitForResponse(context.Background(), 100*time.Millisecond, func() (bool, error) {
		time.Sleep(100 * time.Millisecond)
		return true, nil
	})
	require.NoError(t, err)
	require.Less(t, time.Since(start), 160*time.Millisecond, "the budget must cover callback time, including the first call")
}

// snapshotsNotReadyModule mimics ExecModule.Ready while snapshots are unavailable: each call blocks
// for up to a second, or until its context ends, and then reports not ready.
type snapshotsNotReadyModule struct {
	*stubExecutionModule
}

func (m *snapshotsNotReadyModule) Ready(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
	}
	return false, nil
}

// TestReadinessWaitIsBoundedWhenReadyBlocks proves the readiness wait answers SYNCING within its
// budget even when every Ready call blocks, instead of the budget covering only the polling sleeps.
func TestReadinessWaitIsBoundedWhenReadyBlocks(t *testing.T) {
	t.Parallel()

	header := makeParentHeader(1000)
	headHash := header.Hash()
	headNumber := header.Number.Uint64()

	stub := &stubExecutionModule{
		getHeaderFunc: getHeaderReturning(headHash, header),
		headerNumberFunc: func(context.Context, common.Hash) (*uint64, error) {
			return &headNumber, nil
		},
		getForkChoiceFunc: func(context.Context) (execmodule.ForkChoiceState, error) {
			return execmodule.ForkChoiceState{HeadHash: common.Hash{0x99}}, nil
		},
	}
	module := &snapshotsNotReadyModule{stubExecutionModule: stub}

	cfg := preCancunChainConfig()
	ctx := context.Background()
	downloader := engine_block_downloader.NewEngineBlockDownloader(ctx, log.New(), module, nil, nil, cfg, ethconfig.Sync{}, nil)
	srv := NewEngineServer(log.New(), cfg, module, downloader, false, false, true, true, nil, nil, 0, 0)

	start := time.Now()
	status, err := srv.getQuickPayloadStatusIfPossible(ctx, headHash, 0, common.Hash{}, &engine_types.ForkChoiceState{HeadHash: headHash}, false, 200*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, engine_types.SyncingStatus, status.Status)
	require.Less(t, time.Since(start), 500*time.Millisecond, "a blocking Ready must not stretch the wait past its budget")
}

// TestWaitForResponseDoesNotCallBackAfterDeadline proves no callback starts once the budget is
// spent, even when less than one poll interval remained after the first call.
func TestWaitForResponseDoesNotCallBackAfterDeadline(t *testing.T) {
	t.Parallel()

	calls := 0
	start := time.Now()
	busy, err := waitForResponse(context.Background(), 5*time.Millisecond, func() (bool, error) {
		calls++
		if calls > 1 {
			time.Sleep(time.Second)
		}
		return true, nil
	})
	require.NoError(t, err)
	require.True(t, busy)
	require.Equal(t, 1, calls, "a callback must not start after the deadline")
	require.Less(t, time.Since(start), 100*time.Millisecond)
}

// TestWaitForResponseDoesNotCallBackAfterCancellation proves no callback starts once the context
// is done, even when a poll tick is ready at the same moment. select picks randomly among ready
// cases, so the scenario is repeated to make a missing check fail reliably.
func TestWaitForResponseDoesNotCallBackAfterCancellation(t *testing.T) {
	t.Parallel()

	for range 20 {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		busy, err := waitForResponse(ctx, time.Minute, func() (bool, error) {
			calls++
			if calls == 2 {
				cancel()
				time.Sleep(20 * time.Millisecond) // lets the next tick become ready too
			}
			return true, nil
		})
		cancel()
		require.NoError(t, err)
		require.True(t, busy)
		require.Equal(t, 2, calls, "a callback must not start after cancellation")
	}
}
