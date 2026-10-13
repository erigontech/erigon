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
	requested chan common.Hash
}

func (f *initialHeaderRecorder) FetchHeaders(context.Context, uint64, uint64, *p2p.PeerId, ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Header], error) {
	return p2p.FetcherResponse[[]*types.Header]{}, errors.New("not implemented")
}

func (f *initialHeaderRecorder) FetchHeadersBackwards(_ context.Context, hash common.Hash, _ uint64, _ *p2p.PeerId, _ ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Header], error) {
	select {
	case f.requested <- hash:
	default:
	}
	return p2p.FetcherResponse[[]*types.Header]{}, errors.New("unknown header")
}

func (f *initialHeaderRecorder) FetchBodies(context.Context, []*types.Header, *p2p.PeerId, ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Body], error) {
	return p2p.FetcherResponse[[]*types.Body]{}, errors.New("not implemented")
}

func (f *initialHeaderRecorder) FetchBlocksBackwardsByHash(context.Context, common.Hash, uint64, *p2p.PeerId, ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Block], error) {
	return p2p.FetcherResponse[[]*types.Block]{}, errors.New("not implemented")
}

func TestForkchoiceMissingSegmentDownloadsMissingHash(t *testing.T) {
	t.Parallel()

	head := makeParentHeader(1000)
	headNumber := head.Number.Uint64()
	missingHash := common.Hash{0x42}

	stub := &stubExecutionModule{
		getHeaderFunc: getHeaderReturning(head.Hash(), head),
		headerNumberFunc: func(context.Context, common.Hash) (*uint64, error) {
			return &headNumber, nil
		},
		updateForkChoiceFunc: func(context.Context, common.Hash, common.Hash, common.Hash) (execmodule.ForkChoiceResult, error) {
			return execmodule.ForkChoiceResult{Status: execmodule.ExecutionStatusMissingSegment, MissingHash: missingHash}, nil
		},
	}

	logger := log.New()
	fetcher := &initialHeaderRecorder{requested: make(chan common.Hash, 1)}
	peerTracker := p2p.NewPeerTracker(logger, nil)
	peerTracker.PeerConnected(p2p.PeerIdFromUint64(1))
	bbd := p2p.NewBackwardBlockDownloader(logger, fetcher, &p2p.PeerPenalizer{}, peerTracker, t.TempDir())

	cfg := preCancunChainConfig()
	downloader := engine_block_downloader.NewEngineBlockDownloader(t.Context(), logger, stub, nil, nil, cfg, ethconfig.Sync{}, bbd)
	srv := NewEngineServer(logger, cfg, stub, downloader, false, false, false, true, nil, nil, 0, 0)

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
