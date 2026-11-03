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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

type chainFetcher struct {
	p2p.Fetcher
	blocks map[common.Hash]*types.Block
}

func (f *chainFetcher) FetchHeadersBackwards(_ context.Context, hash common.Hash, amount uint64, _ *p2p.PeerId, _ ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Header], error) {
	var headers []*types.Header
	for b := f.blocks[hash]; b != nil && uint64(len(headers)) < amount; b = f.blocks[b.ParentHash()] {
		headers = append(headers, b.Header())
	}
	if len(headers) == 0 {
		return p2p.FetcherResponse[[]*types.Header]{}, errors.New("unknown header")
	}
	slices.Reverse(headers)
	return p2p.FetcherResponse[[]*types.Header]{Data: headers}, nil
}

func (f *chainFetcher) FetchHeaders(context.Context, uint64, uint64, *p2p.PeerId, ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Header], error) {
	return p2p.FetcherResponse[[]*types.Header]{}, errors.New("unknown header")
}

func (f *chainFetcher) FetchBodies(_ context.Context, headers []*types.Header, _ *p2p.PeerId, _ ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Body], error) {
	bodies := make([]*types.Body, len(headers))
	for i, h := range headers {
		bodies[i] = f.blocks[h.Hash()].Body()
	}
	return p2p.FetcherResponse[[]*types.Body]{Data: bodies}, nil
}

func newExecLagEngineServer(t *testing.T, loopBlockLimit uint, lag int) (*EngineServer, *blockgen.ChainPack) {
	t.Helper()
	m, chainPack := execmoduletester.NewWithExecLag(t, lag, 2, execmoduletester.WithLoopBlockLimit(loopBlockLimit))

	fetcher := &chainFetcher{blocks: make(map[common.Hash]*types.Block, len(chainPack.Blocks))}
	for _, b := range chainPack.Blocks {
		fetcher.blocks[b.Hash()] = b
	}
	peerTracker := p2p.NewPeerTracker(m.Log, nil)
	peerTracker.PeerConnected(p2p.PeerIdFromUint64(1))
	bbd := p2p.NewBackwardBlockDownloader(m.Log, fetcher, peerTracker, t.TempDir())
	syncCfg := m.Cfg().Sync
	downloader := engine_block_downloader.NewEngineBlockDownloader(t.Context(), m.Log, m.ExecModule, m.BlockReader, m.DB, m.ChainConfig, syncCfg, bbd)
	srv := NewEngineServer(m.Log, m.ChainConfig, m.ExecModule, downloader, false, false, false, true, nil, nil, 0, syncCfg.MaxReorgDepth)
	return srv, chainPack
}

func TestNewPayloadBeyondLoopBlockLimitIsAccepted(t *testing.T) {
	const lag = 12
	srv, chainPack := newExecLagEngineServer(t, 5, lag)

	status, err := srv.HandleNewPayload(t.Context(), "test", chainPack.Blocks[1+lag])
	require.NoError(t, err)
	require.Equal(t, engine_types.AcceptedStatus, status.Status)
	require.Nil(t, status.LatestValidHash)
}

func TestNewPayloadWithDownloadedParentBeyondLoopBlockLimitIsAccepted(t *testing.T) {
	const lag = 12
	srv, chainPack := newExecLagEngineServer(t, 5, lag)

	status, err := srv.HandleNewPayload(t.Context(), "test", chainPack.Blocks[2+lag])
	require.NoError(t, err)
	require.Equal(t, engine_types.AcceptedStatus, status.Status)
	require.Nil(t, status.LatestValidHash)
}
