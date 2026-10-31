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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

type initialHeaderRecorder struct {
	p2p.Fetcher
	requested chan common.Hash
}

func (f *initialHeaderRecorder) FetchHeadersBackwards(_ context.Context, hash common.Hash, _ uint64, _ *p2p.PeerId, _ ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Header], error) {
	select {
	case f.requested <- hash:
	default:
	}
	return p2p.FetcherResponse[[]*types.Header]{}, errors.New("unknown header")
}

func newMissingSegmentServer(t *testing.T, missingHash common.Hash) (*EngineServer, *engine_block_downloader.EngineBlockDownloader, *initialHeaderRecorder, *types.Header) {
	return newForkchoiceResultServer(t, execmodule.ForkChoiceResult{Status: execmodule.ExecutionStatusMissingSegment, MissingHash: missingHash})
}

func newForkchoiceResultServer(t *testing.T, result execmodule.ForkChoiceResult) (*EngineServer, *engine_block_downloader.EngineBlockDownloader, *initialHeaderRecorder, *types.Header) {
	head := makeParentHeader(1000)
	headNumber := head.Number.Uint64()
	stub := &stubExecutionModule{
		getHeaderFunc: getHeaderReturning(head.Hash(), head),
		headerNumberFunc: func(context.Context, common.Hash) (*uint64, error) {
			return &headNumber, nil
		},
		updateForkChoiceFunc: func(context.Context, common.Hash, common.Hash, common.Hash) (execmodule.ForkChoiceResult, error) {
			return result, nil
		},
	}

	logger := log.New()
	fetcher := &initialHeaderRecorder{requested: make(chan common.Hash, 1)}
	peerTracker := p2p.NewPeerTracker(logger, nil)
	peerTracker.PeerConnected(p2p.PeerIdFromUint64(1))
	bbd := p2p.NewBackwardBlockDownloader(logger, fetcher, peerTracker, t.TempDir())

	cfg := preCancunChainConfig()
	downloader := engine_block_downloader.NewEngineBlockDownloader(t.Context(), logger, stub, nil, nil, cfg, ethconfig.Sync{}, bbd)
	srv := NewEngineServer(logger, cfg, stub, downloader, false, false, false, true, nil, nil, 0, 0)
	return srv, downloader, fetcher, head
}

func TestForkchoiceMissingSegmentDownloadsMissingHash(t *testing.T) {
	t.Parallel()

	missingHash := common.Hash{0x42}
	srv, _, fetcher, head := newMissingSegmentServer(t, missingHash)

	status, err := srv.HandleForkChoice(t.Context(), "test", &engine_types.ForkChoiceState{HeadHash: head.Hash()})
	require.NoError(t, err)
	require.Equal(t, engine_types.SyncingStatus, status.Status)

	select {
	case got := <-fetcher.requested:
		require.Equal(t, missingHash, got)
	case <-time.After(5 * time.Second):
		t.Fatal("no backward download started for the missing hash")
	}
}

func TestForkchoiceMissingSegmentOfBadBlockIsInvalid(t *testing.T) {
	t.Parallel()

	missingHash := common.Hash{0x42}
	lastValidHash := common.Hash{0x41}
	srv, downloader, fetcher, head := newMissingSegmentServer(t, missingHash)
	downloader.ReportBadHeader(missingHash, lastValidHash, "invalid state root")

	status, err := srv.HandleForkChoice(t.Context(), "test", &engine_types.ForkChoiceState{HeadHash: head.Hash()})
	require.NoError(t, err)
	require.Equal(t, engine_types.InvalidStatus, status.Status)
	require.Equal(t, lastValidHash, *status.LatestValidHash)
	require.Equal(t, engine_block_downloader.Idle, downloader.Status())
	require.Empty(t, fetcher.requested)
}

func TestForkchoiceBadBlockReturnsLatestValidHash(t *testing.T) {
	t.Parallel()

	parentHash := common.Hash{0x41}
	srv, downloader, _, head := newForkchoiceResultServer(t, execmodule.ForkChoiceResult{
		Status:          execmodule.ExecutionStatusBadBlock,
		LatestValidHash: parentHash,
		ValidationError: "invalid state root",
	})

	status, err := srv.HandleForkChoice(t.Context(), "test", &engine_types.ForkChoiceState{HeadHash: head.Hash()})
	require.NoError(t, err)
	require.Equal(t, engine_types.InvalidStatus, status.Status)
	require.NotNil(t, status.LatestValidHash)
	require.Equal(t, parentHash, *status.LatestValidHash)
	require.EqualError(t, status.ValidationError.Error(), "invalid state root")

	bad, lastValid, validationErr := downloader.IsBadHeader(head.Hash())
	require.True(t, bad)
	require.Equal(t, parentHash, lastValid)
	require.Equal(t, "invalid state root", validationErr)
}
