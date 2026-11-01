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
	"math/big"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
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

func (f *chainFetcher) FetchBodies(_ context.Context, headers []*types.Header, _ *p2p.PeerId, _ ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Body], error) {
	bodies := make([]*types.Body, len(headers))
	for i, h := range headers {
		bodies[i] = f.blocks[h.Hash()].Body()
	}
	return p2p.FetcherResponse[[]*types.Body]{Data: bodies}, nil
}

func newExecLagEngineServer(t *testing.T, loopBlockLimit uint, lag int) (*EngineServer, *blockgen.ChainPack) {
	t.Helper()
	ctx := t.Context()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	addr := crypto.PubkeyToAddress(key.PublicKey)
	genesis := &types.Genesis{Config: chain.AllProtocolChanges, Alloc: types.GenesisAlloc{addr: {Balance: big.NewInt(1e18)}}}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key), execmoduletester.WithLoopBlockLimit(loopBlockLimit))
	chainPack, err := m.GenerateChain(lag+3, func(i int, b *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(uint64(i), addr, uint256.NewInt(1), 50000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil), *types.LatestSignerForChainID(nil), key)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertValidateAndUfc1By1(ctx, chainPack.Blocks[:1]))

	lagging := chainPack.Blocks[1 : 1+lag]
	_, err = m.InsertBlocks(ctx, lagging)
	require.NoError(t, err)
	head := lagging[len(lagging)-1].NumberU64()
	require.NoError(t, m.DB.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		maxTxNum, err := rawdbv3.TxNums.Max(ctx, tx, 1)
		if err != nil {
			return err
		}
		for _, b := range lagging {
			maxTxNum += uint64(len(b.Transactions())) + 2
			if err := rawdbv3.TxNums.Append(tx, b.NumberU64(), maxTxNum); err != nil {
				return err
			}
			if err := rawdb.WriteCanonicalHash(tx, b.Hash(), b.NumberU64()); err != nil {
				return err
			}
		}
		for _, stage := range []stages.SyncStage{stages.Headers, stages.Bodies, stages.Senders} {
			if err := stages.SaveStageProgress(tx, stage, head); err != nil {
				return err
			}
		}
		return nil
	}))

	fetcher := &chainFetcher{blocks: make(map[common.Hash]*types.Block, len(chainPack.Blocks))}
	for _, b := range chainPack.Blocks {
		fetcher.blocks[b.Hash()] = b
	}
	peerTracker := p2p.NewPeerTracker(m.Log, nil)
	peerTracker.PeerConnected(p2p.PeerIdFromUint64(1))
	bbd := p2p.NewBackwardBlockDownloader(m.Log, fetcher, &p2p.PeerPenalizer{}, peerTracker, t.TempDir())
	syncCfg := m.Cfg().Sync
	downloader := engine_block_downloader.NewEngineBlockDownloader(ctx, m.Log, m.ExecModule, m.BlockReader, m.DB, m.ChainConfig, syncCfg, bbd)
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
