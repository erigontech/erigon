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

// TestForkchoiceUpdatedWithAttributesWaitsOutPostForkchoiceWork reproduces #24371: a
// forkchoiceUpdated carrying payload attributes for an already-known head must not come back
// SYNCING without a payload id just because the module is briefly busy with the previous head's
// background flush/commit/prune - the same request would get a payload id half a second later.
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
