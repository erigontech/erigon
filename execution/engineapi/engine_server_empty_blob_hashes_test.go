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
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// wireChainFetcher round-trips the bodies through RLP, as a body from a peer is decoded.
type wireChainFetcher struct{ chainFetcher }

func (f *wireChainFetcher) FetchBodies(ctx context.Context, headers []*types.Header, peerId *p2p.PeerId, opts ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Body], error) {
	res, err := f.chainFetcher.FetchBodies(ctx, headers, peerId, opts...)
	if err != nil {
		return res, err
	}
	for i, body := range res.Data {
		enc, err := rlp.EncodeToBytes(body)
		if err != nil {
			return p2p.FetcherResponse[[]*types.Body]{}, err
		}
		decoded := new(types.Body)
		if err := rlp.DecodeBytes(enc, decoded); err != nil {
			return p2p.FetcherResponse[[]*types.Body]{}, err
		}
		res.Data[i] = decoded
	}
	return res, nil
}

type emptyBlobHashesChain struct {
	srv                      *EngineServer
	valid, invalid, invalid2 *types.Block
}

// newEmptyBlobHashesChain executes valid on top of genesis. invalid carries a blob
// transaction without blob hashes, and invalid2 is its child. A peer serves all three.
func newEmptyBlobHashesChain(t *testing.T) emptyBlobHashesChain {
	t.Helper()
	cfg := chain.TestChainOsakaConfig
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(cfg), execmoduletester.WithLoopBlockLimit(100))
	chainPack, err := m.GenerateChain(3, func(int, *blockgen.BlockGen) {})
	require.NoError(t, err)
	valid := chainPack.Blocks[0]
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), []*types.Block{valid}))

	txn, err := types.SignTx(&types.BlobTx{
		DynamicFeeTransaction: *types.NewEIP1559Transaction(
			*cfg.ChainID, 0, common.Address{1}, uint256.NewInt(0), 21_000,
			uint256.NewInt(1), uint256.NewInt(1e12), nil,
		),
		MaxFeePerBlobGas: *uint256.NewInt(1e12),
	}, *types.LatestSignerForChainID(cfg.ChainID), m.Key)
	require.NoError(t, err)
	invalid := types.NewBlock(chainPack.Headers[1], types.Transactions{txn}, nil, nil, chainPack.Blocks[1].Withdrawals(), nil)
	header2 := types.CopyHeader(chainPack.Headers[2])
	header2.ParentHash = invalid.Hash()
	invalid2 := types.NewBlock(header2, nil, nil, nil, chainPack.Blocks[2].Withdrawals(), nil)

	fetcher := &wireChainFetcher{chainFetcher{blocks: map[common.Hash]*types.Block{
		valid.Hash(): valid, invalid.Hash(): invalid, invalid2.Hash(): invalid2,
	}}}
	peerTracker := p2p.NewPeerTracker(m.Log, nil)
	peerTracker.PeerConnected(p2p.PeerIdFromUint64(1))
	bbd := p2p.NewBackwardBlockDownloader(m.Log, fetcher, peerTracker, t.TempDir())
	syncCfg := m.Cfg().Sync
	downloader := engine_block_downloader.NewEngineBlockDownloader(t.Context(), m.Log, m.ExecModule, m.BlockReader, m.DB, m.ChainConfig, syncCfg, bbd)
	srv := NewEngineServer(m.Log, m.ChainConfig, m.ExecModule, downloader, false, false, false, true, nil, nil, 0, syncCfg.MaxReorgDepth)
	return emptyBlobHashesChain{srv: srv, valid: valid, invalid: invalid, invalid2: invalid2}
}

func sendNewPayload(t *testing.T, srv *EngineServer, block *types.Block) *engine_types.PayloadStatus {
	t.Helper()
	payload, err := engine_types.ExecutionPayloadFromBlock(block)
	require.NoError(t, err)
	status, err := srv.NewPayloadV4(t.Context(), payload, []common.Hash{}, block.Header().ParentBeaconBlockRoot, []hexutil.Bytes{})
	require.NoError(t, err)
	return status
}

func requireEmptyBlobHashesInvalid(t *testing.T, status *engine_types.PayloadStatus, latestValid common.Hash) {
	t.Helper()
	require.Equal(t, engine_types.InvalidStatus, status.Status, "validation error: %v", status.ValidationError)
	require.Equal(t, &latestValid, status.LatestValidHash)
	require.NotNil(t, status.ValidationError)
	require.ErrorContains(t, status.ValidationError.Error(), types.ErrBlobTxnEmptyBlobs.Error())
}

// The parent of the new payload is downloaded from a peer, and both blocks are then
// invalid, with the last valid block as the latest valid hash.
func TestNewPayloadMarksDownloadedEmptyBlobHashesChainInvalid(t *testing.T) {
	c := newEmptyBlobHashesChain(t)

	status := sendNewPayload(t, c.srv, c.invalid2)
	require.Contains(t, []engine_types.EngineStatus{engine_types.SyncingStatus, engine_types.InvalidStatus}, status.Status)
	require.NotEqual(t, engine_block_downloader.Syncing, c.srv.blockDownloader.Status())

	requireEmptyBlobHashesInvalid(t, sendNewPayload(t, c.srv, c.invalid2), c.valid.Hash())
	requireEmptyBlobHashesInvalid(t, sendNewPayload(t, c.srv, c.invalid), c.valid.Hash())
}

// The block is invalid with its parent as the latest valid hash, also when it is sent
// again, and so is its child.
func TestNewPayloadRejectsEmptyBlobHashesAtExecution(t *testing.T) {
	c := newEmptyBlobHashesChain(t)

	requireEmptyBlobHashesInvalid(t, sendNewPayload(t, c.srv, c.invalid), c.valid.Hash())
	requireEmptyBlobHashesInvalid(t, sendNewPayload(t, c.srv, c.invalid), c.valid.Hash())
	requireEmptyBlobHashesInvalid(t, sendNewPayload(t, c.srv, c.invalid2), c.valid.Hash())
}

func TestForkchoiceMarksDownloadedEmptyBlobHashesChainInvalid(t *testing.T) {
	c := newEmptyBlobHashesChain(t)
	fcu := func() *engine_types.PayloadStatus {
		resp, err := c.srv.ForkchoiceUpdatedV3(t.Context(), &engine_types.ForkChoiceState{HeadHash: c.invalid2.Hash()}, nil)
		require.NoError(t, err)
		return resp.PayloadStatus
	}

	require.Contains(t, []engine_types.EngineStatus{engine_types.SyncingStatus, engine_types.InvalidStatus}, fcu().Status)
	require.Eventually(t, func() bool {
		return c.srv.blockDownloader.Status() != engine_block_downloader.Syncing
	}, 30*time.Second, 10*time.Millisecond)

	requireEmptyBlobHashesInvalid(t, fcu(), c.valid.Hash())
	requireEmptyBlobHashesInvalid(t, sendNewPayload(t, c.srv, c.invalid), c.valid.Hash())
}
