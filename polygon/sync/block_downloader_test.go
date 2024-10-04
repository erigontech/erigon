// Copyright 2024 The Erigon Authors
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

package sync

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/p2p"
	"github.com/erigontech/erigon/turbo/testlog"
)

func newBlockDownloaderTest(t *testing.T) *blockDownloaderTest {
	return newBlockDownloaderTestWithOpts(t, blockDownloaderTestOpts{})
}

func newBlockDownloaderTestWithOpts(t *testing.T, opts blockDownloaderTestOpts) *blockDownloaderTest {
	ctrl := gomock.NewController(t)
	waypointReader := NewMockwaypointReader(ctrl)
	p2pService := p2p.NewMockService(ctrl)
	p2pService.EXPECT().MaxPeers().Return(100).Times(1)
	logger := testlog.Logger(t, log.LvlDebug)
	checkpointVerifier := opts.getOrCreateDefaultCheckpointVerifier()
	milestoneVerifier := opts.getOrCreateDefaultMilestoneVerifier()
	blocksVerifier := opts.getOrCreateDefaultBlocksVerifier()
	store := NewMockStore(ctrl)
	headerDownloader := newBlockDownloader(
		logger,
		p2pService,
		waypointReader,
		checkpointVerifier,
		milestoneVerifier,
		blocksVerifier,
		store,
		time.Millisecond,
		opts.getOrCreateDefaultMaxWorkers(),
		opts.getOrCreateDefaultBlockLimit(),
	)
	return &blockDownloaderTest{
		waypointReader:  waypointReader,
		p2pService:      p2pService,
		blockDownloader: headerDownloader,
		store:           store,
	}
}

type blockDownloaderTestOpts struct {
	checkpointVerifier WaypointHeadersVerifier
	milestoneVerifier  WaypointHeadersVerifier
	blocksVerifier     BlocksVerifier
	maxWorkers         int
	blockLimit         uint
}

func (opts blockDownloaderTestOpts) getOrCreateDefaultCheckpointVerifier() WaypointHeadersVerifier {
	if opts.checkpointVerifier == nil {
		return func(_ heimdall.Waypoint, _ []*types.Header) error {
			return nil
		}
	}

	return opts.checkpointVerifier
}

func (opts blockDownloaderTestOpts) getOrCreateDefaultMilestoneVerifier() WaypointHeadersVerifier {
	if opts.milestoneVerifier == nil {
		return func(_ heimdall.Waypoint, _ []*types.Header) error {
			return nil
		}
	}

	return opts.milestoneVerifier
}

func (opts blockDownloaderTestOpts) getOrCreateDefaultBlocksVerifier() BlocksVerifier {
	if opts.blocksVerifier == nil {
		return func(_ []*types.Block) error {
			return nil
		}
	}

	return opts.blocksVerifier
}

func (opts blockDownloaderTestOpts) getOrCreateDefaultMaxWorkers() int {
	if opts.maxWorkers == 0 {
		return math.MaxInt
	}

	return opts.maxWorkers
}

func (opts blockDownloaderTestOpts) getOrCreateDefaultBlockLimit() uint {
	return opts.blockLimit // default to 0 if not set
}

type blockDownloaderTest struct {
	waypointReader  *MockwaypointReader
	p2pService      *p2p.MockService
	blockDownloader *blockDownloader
	store           *MockStore
}

func (hdt blockDownloaderTest) fakePeers(count int) []*p2p.PeerId {
	peers := make([]*p2p.PeerId, count)
	for i := range peers {
		peers[i] = p2p.PeerIdFromUint64(uint64(i) + 1)
	}

	return peers
}

func (hdt blockDownloaderTest) fakeCheckpoints(count int) heimdall.Checkpoints {
	checkpoints := make(heimdall.Checkpoints, count)
	for i := range checkpoints {
		start := i*1024 + 1
		end := start + 1023
		checkpoints[i] = &heimdall.Checkpoint{
			Fields: heimdall.WaypointFields{
				StartBlock: big.NewInt(int64(start)),
				EndBlock:   big.NewInt(int64(end)),
				RootHash:   common.BytesToHash([]byte(fmt.Sprintf("0x%d", i+1))),
			},
		}
	}

	return checkpoints
}

func (hdt blockDownloaderTest) fakeMilestones(count int) heimdall.Milestones {
	milestones := make(heimdall.Milestones, count)
	for i := range milestones {
		start := i*12 + 1
		end := start + 11
		milestones[i] = &heimdall.Milestone{
			Fields: heimdall.WaypointFields{
				StartBlock: big.NewInt(int64(start)),
				EndBlock:   big.NewInt(int64(end)),
				RootHash:   common.BytesToHash([]byte(fmt.Sprintf("0x%d", i+1))),
			},
		}
	}

	return milestones
}

type fetchHeadersMock func(
	ctx context.Context,
	start uint64,
	end uint64,
	peerId *p2p.PeerId,
	opts ...p2p.FetcherOption,
) (p2p.FetcherResponse[[]*types.Header], error)

func (hdt blockDownloaderTest) defaultFetchHeadersMock() fetchHeadersMock {
	// p2p.Service.FetchHeaders interface is using [start, end) so we stick to that
	return func(
		ctx context.Context,
		start uint64,
		end uint64,
		peerId *p2p.PeerId,
		opts ...p2p.FetcherOption,
	) (p2p.FetcherResponse[[]*types.Header], error) {
		if start >= end {
			err := fmt.Errorf("unexpected start >= end in test: start=%d, end=%d", start, end)
			return p2p.FetcherResponse[[]*types.Header]{}, err
		}

		res := make([]*types.Header, end-start)
		size := 0
		for num := start; num < end; num++ {
			header := &types.Header{
				Number: new(big.Int).SetUint64(num),
			}
			res[num-start] = header
			size += header.EncodingSize()
		}

		return p2p.FetcherResponse[[]*types.Header]{Data: res, TotalSize: size}, nil
	}
}

type fetchBodiesMock func(
	ctx context.Context,
	headers []*types.Header,
	peerId *p2p.PeerId,
	opts ...p2p.FetcherOption,
) (p2p.FetcherResponse[[]*types.Body], error)

func (hdt blockDownloaderTest) defaultFetchBodiesMock() fetchBodiesMock {
	return func(
		ctx context.Context,
		headers []*types.Header,
		peerId *p2p.PeerId,
		opts ...p2p.FetcherOption,
	) (p2p.FetcherResponse[[]*types.Body], error) {
		bodies := make([]*types.Body, len(headers))
		size := 0

		for i := range headers {
			body := &types.Body{
				Transactions: []types.Transaction{
					types.NewEIP1559Transaction(
						*uint256.NewInt(1),
						1,
						common.BigToAddress(big.NewInt(123)),
						uint256.NewInt(55),
						0,
						uint256.NewInt(666),
						uint256.NewInt(777),
						uint256.NewInt(888),
						nil,
					),
				},
			}
			bodies[i] = body
			size += body.EncodingSize()
		}

		return p2p.FetcherResponse[[]*types.Body]{Data: bodies, TotalSize: size}, nil
	}
}

func (hdt blockDownloaderTest) defaultInsertBlocksMock(capture *[]*types.Block) func(context.Context, []*types.Block) error {
	return func(ctx context.Context, blocks []*types.Block) error {
		*capture = append(*capture, blocks...)
		return nil
	}
}

func TestBlockDownloaderDownloadBlocksUsingMilestones(t *testing.T) {
	test := newBlockDownloaderTest(t)
	test.waypointReader.EXPECT().
		MilestonesFromBlock(gomock.Any(), gomock.Any()).
		Return(test.fakeMilestones(4), nil).
		Times(1)
	test.p2pService.EXPECT().
		ListPeersMayHaveBlockNum(gomock.Any()).
		Return(test.fakePeers(8)).
		Times(1)
	test.p2pService.EXPECT().
		FetchHeaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchHeadersMock()).
		Times(4)
	test.p2pService.EXPECT().
		FetchBodies(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchBodiesMock()).
		Times(4)
	var blocks []*types.Block
	test.store.EXPECT().
		InsertBlocks(gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultInsertBlocksMock(&blocks)).
		Times(1)

	tip, err := test.blockDownloader.DownloadBlocksUsingMilestones(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, blocks, 48) // 4 milestones x 12 blocks each
	// check blocks are written in order
	require.Equal(t, uint64(1), blocks[0].Header().Number.Uint64())
	require.Equal(t, uint64(2), blocks[1].Header().Number.Uint64())
	require.Equal(t, uint64(3), blocks[2].Header().Number.Uint64())
	require.Equal(t, uint64(4), blocks[3].Header().Number.Uint64())
	require.Equal(t, uint64(48), blocks[47].Header().Number.Uint64())
	require.Equal(t, blocks[len(blocks)-1].Header(), tip)
}

func TestBlockDownloaderDownloadBlocksUsingMilestonesWhenStartIsBeforeFirstMilestone(t *testing.T) {
	test := newBlockDownloaderTest(t)
	// we skip milestone 1, only use 2,3,4 (3 milestones in total) with start=1
	fakeMilestones := test.fakeMilestones(4)[1:]
	test.waypointReader.EXPECT().
		MilestonesFromBlock(gomock.Any(), gomock.Any()).
		Return(fakeMilestones, nil).
		Times(1)
	test.p2pService.EXPECT().
		ListPeersMayHaveBlockNum(gomock.Any()).
		Return(test.fakePeers(6)).
		Times(1)
	test.p2pService.EXPECT().
		FetchHeaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchHeadersMock()).
		Times(3)
	test.p2pService.EXPECT().
		FetchBodies(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchBodiesMock()).
		Times(3)
	var blocks []*types.Block
	test.store.EXPECT().
		InsertBlocks(gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultInsertBlocksMock(&blocks)).
		Times(1)

	tip, err := test.blockDownloader.DownloadBlocksUsingMilestones(context.Background(), 1)
	require.NoError(t, err)
	// 3 milestones x 12 blocks each + 12 because we override milestones[0].StartBlock=1 to fill the gap
	require.Len(t, blocks, 48)
	// check blocks are written in order
	require.Equal(t, uint64(1), blocks[0].Header().Number.Uint64())
	require.Equal(t, uint64(16), blocks[15].Header().Number.Uint64())
	require.Equal(t, uint64(48), blocks[47].Header().Number.Uint64())
	require.Equal(t, blocks[len(blocks)-1].Header(), tip)
}

func TestBlockDownloaderDownloadBlocksUsingCheckpoints(t *testing.T) {
	test := newBlockDownloaderTest(t)
	test.waypointReader.EXPECT().
		CheckpointsFromBlock(gomock.Any(), gomock.Any()).
		Return(test.fakeCheckpoints(8), nil).
		Times(1)
	test.p2pService.EXPECT().
		ListPeersMayHaveBlockNum(gomock.Any()).
		Return(test.fakePeers(2)).
		Times(4)
	test.p2pService.EXPECT().
		FetchHeaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchHeadersMock()).
		Times(8)
	test.p2pService.EXPECT().
		FetchBodies(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchBodiesMock()).
		Times(8)
	var blocks []*types.Block
	test.store.EXPECT().
		InsertBlocks(gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultInsertBlocksMock(&blocks)).
		Times(4)

	tip, err := test.blockDownloader.DownloadBlocksUsingCheckpoints(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, blocks, 8192) // 8 checkpoints x 1024 blocks each
	// check blocks are written in order
	require.Equal(t, uint64(1), blocks[0].Header().Number.Uint64())
	require.Equal(t, uint64(2), blocks[1].Header().Number.Uint64())
	require.Equal(t, uint64(3), blocks[2].Header().Number.Uint64())
	require.Equal(t, uint64(4), blocks[3].Header().Number.Uint64())
	require.Equal(t, uint64(5), blocks[4].Header().Number.Uint64())
	require.Equal(t, uint64(6), blocks[5].Header().Number.Uint64())
	require.Equal(t, uint64(7), blocks[6].Header().Number.Uint64())
	require.Equal(t, uint64(8), blocks[7].Header().Number.Uint64())
	require.Equal(t, uint64(8192), blocks[8191].Header().Number.Uint64())
	require.Equal(t, blocks[len(blocks)-1].Header(), tip)
}

func TestBlockDownloaderDownloadBlocksUsingCheckpointsWhenStartIsInMiddleOfCheckpointRange(t *testing.T) {
	test := newBlockDownloaderTest(t)
	test.waypointReader.EXPECT().
		CheckpointsFromBlock(gomock.Any(), gomock.Any()).
		Return(test.fakeCheckpoints(2), nil).
		Times(1)
	test.p2pService.EXPECT().
		ListPeersMayHaveBlockNum(gomock.Any()).
		Return(test.fakePeers(2)).
		Times(1)
	test.p2pService.EXPECT().
		FetchHeaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchHeadersMock()).
		Times(2)
	test.p2pService.EXPECT().
		FetchBodies(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchBodiesMock()).
		Times(2)
	var blocks []*types.Block
	test.store.EXPECT().
		InsertBlocks(gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultInsertBlocksMock(&blocks)).
		Times(1)

	tip, err := test.blockDownloader.DownloadBlocksUsingCheckpoints(context.Background(), 513)
	require.NoError(t, err)
	require.Len(t, blocks, 1536) // [513,1024] = 512 blocks + 1024 blocks from 2nd checkpoint
	// check blocks are written in order
	require.Equal(t, uint64(513), blocks[0].Header().Number.Uint64())
	require.Equal(t, uint64(1024), blocks[511].Header().Number.Uint64())
	require.Equal(t, uint64(1025), blocks[512].Header().Number.Uint64())
	require.Equal(t, uint64(2048), blocks[1535].Header().Number.Uint64())
	require.Equal(t, blocks[len(blocks)-1].Header(), tip)
}

func TestBlockDownloaderDownloadBlocksWhenInvalidHeadersThenPenalizePeerAndReDownload(t *testing.T) {
	var firstTimeInvalidReturned bool
	firstTimeInvalidReturnedPtr := &firstTimeInvalidReturned
	test := newBlockDownloaderTestWithOpts(t, blockDownloaderTestOpts{
		checkpointVerifier: func(waypoint heimdall.Waypoint, headers []*types.Header) error {
			// 1025 is start of 2nd checkpoint
			if waypoint.StartBlock().Cmp(new(big.Int).SetUint64(1025)) == 0 && !*firstTimeInvalidReturnedPtr {
				*firstTimeInvalidReturnedPtr = true
				return errors.New("invalid checkpoint")
			}
			return nil
		},
	})
	test.waypointReader.EXPECT().
		CheckpointsFromBlock(gomock.Any(), gomock.Any()).
		Return(test.fakeCheckpoints(6), nil).
		Times(1)
	test.p2pService.EXPECT().
		FetchHeaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchHeadersMock()).
		// request checkpoints 1,2,3 in parallel (we have 3 peers)
		// -> verifications for checkpoint 2 headers fails, checkpoints 1 and 3 pass verifications
		// requests 2,3 in parallel (now we have only 2 peers)
		// -> checkpoint 3 blocks are cached, checkpoint 2 is re-requested from another peer and passes verifications
		// requests 4,5 in parallel
		// request 6
		// in total 6 requests + 1 request for re-requesting checkpoint 2 headers
		// total = 7 (note this also tests blocks caching works)
		Times(7)
	test.p2pService.EXPECT().
		FetchBodies(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchBodiesMock()).
		// 1 less than FetchHeaders since checkpoint 2 from peer 2 did not pass headers verification
		Times(6)
	fakePeers := test.fakePeers(3)
	gomock.InOrder(
		test.p2pService.EXPECT().
			ListPeersMayHaveBlockNum(gomock.Any()).
			Return(fakePeers). // 3 initially
			Times(1),
		test.p2pService.EXPECT().
			ListPeersMayHaveBlockNum(gomock.Any()).
			Return([]*p2p.PeerId{fakePeers[0], fakePeers[2]}). // but then peer 2 gets penalized
			Times(3),
	)
	test.p2pService.EXPECT().
		Penalize(gomock.Any(), gomock.Eq(p2p.PeerIdFromUint64(2))).
		Times(1)
	var blocksBatch1, blocksBatch2 []*types.Block
	gomock.InOrder(
		test.store.EXPECT().
			InsertBlocks(gomock.Any(), gomock.Any()).
			DoAndReturn(test.defaultInsertBlocksMock(&blocksBatch1)).
			Times(1),
		test.store.EXPECT().
			InsertBlocks(gomock.Any(), gomock.Any()).
			DoAndReturn(test.defaultInsertBlocksMock(&blocksBatch2)).
			Times(3),
	)

	_, err := test.blockDownloader.DownloadBlocksUsingCheckpoints(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, blocksBatch1, 1024)
	require.Len(t, blocksBatch2, 5120)
}

func TestBlockDownloaderDownloadBlocksWhenZeroPeersTriesAgain(t *testing.T) {
	test := newBlockDownloaderTest(t)
	test.waypointReader.EXPECT().
		CheckpointsFromBlock(gomock.Any(), gomock.Any()).
		Return(test.fakeCheckpoints(8), nil).
		Times(1)
	test.p2pService.EXPECT().
		FetchHeaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchHeadersMock()).
		Times(8)
	test.p2pService.EXPECT().
		FetchBodies(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchBodiesMock()).
		Times(8)
	var blocks []*types.Block
	test.store.EXPECT().
		InsertBlocks(gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultInsertBlocksMock(&blocks)).
		Times(4)
	gomock.InOrder(
		// first time, no peers at all
		test.p2pService.EXPECT().
			ListPeersMayHaveBlockNum(gomock.Any()).
			Return(nil).
			Times(1),
		// second time, 2 peers that we can use
		test.p2pService.EXPECT().
			ListPeersMayHaveBlockNum(gomock.Any()).
			Return(test.fakePeers(2)).
			Times(4),
	)

	tip, err := test.blockDownloader.DownloadBlocksUsingCheckpoints(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, blocks, 8192)
	require.Equal(t, blocks[len(blocks)-1].Header(), tip)
}

func TestBlockDownloaderDownloadBlocksWhenInvalidBodiesThenPenalizePeerAndReDownload(t *testing.T) {
	var firstTimeInvalidReturned bool
	firstTimeInvalidReturnedPtr := &firstTimeInvalidReturned
	test := newBlockDownloaderTestWithOpts(t, blockDownloaderTestOpts{
		blocksVerifier: func(blocks []*types.Block) error {
			// 1025 is beginning of 2nd checkpoint
			if blocks[0].NumberU64() == 1025 && !*firstTimeInvalidReturnedPtr {
				*firstTimeInvalidReturnedPtr = true
				return errors.New("invalid block body")
			}
			return nil
		},
	})
	test.waypointReader.EXPECT().
		CheckpointsFromBlock(gomock.Any(), gomock.Any()).
		Return(test.fakeCheckpoints(6), nil).
		Times(1)
	test.p2pService.EXPECT().
		FetchHeaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchHeadersMock()).
		// request checkpoints 1,2,3 in parallel (we have 3 peers)
		// -> verifications for checkpoint 2 bodies fails, checkpoints 1 and 3 pass verifications
		// requests 2,3 in parallel (now we have only 2 peers)
		// -> checkpoint 3 blocks are cached, checkpoint 2 is re-requested from another peer and passes verifications
		// requests 4,5 in parallel
		// request 6
		// in total 6 requests + 1 request for re-requesting checkpoint 2 headers + bodies
		// total = 7 (note this also tests blocks caching works)
		Times(7)
	test.p2pService.EXPECT().
		FetchBodies(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchBodiesMock()).
		// same explanation as above for FetchHeaders.Times(7)
		Times(7)
	fakePeers := test.fakePeers(3)
	gomock.InOrder(
		test.p2pService.EXPECT().
			ListPeersMayHaveBlockNum(gomock.Any()).
			Return(fakePeers). // 3 initially
			Times(1),
		test.p2pService.EXPECT().
			ListPeersMayHaveBlockNum(gomock.Any()).
			Return([]*p2p.PeerId{fakePeers[0], fakePeers[2]}). // but then peer 2 gets penalized
			Times(3),
	)
	test.p2pService.EXPECT().
		Penalize(gomock.Any(), gomock.Eq(p2p.PeerIdFromUint64(2))).
		Times(1)
	var blocksBatch1, blocksBatch2 []*types.Block
	gomock.InOrder(
		test.store.EXPECT().
			InsertBlocks(gomock.Any(), gomock.Any()).
			DoAndReturn(test.defaultInsertBlocksMock(&blocksBatch1)).
			Times(1),
		test.store.EXPECT().
			InsertBlocks(gomock.Any(), gomock.Any()).
			DoAndReturn(test.defaultInsertBlocksMock(&blocksBatch2)).
			Times(3),
	)

	_, err := test.blockDownloader.DownloadBlocksUsingCheckpoints(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, blocksBatch1, 1024)
	require.Len(t, blocksBatch2, 5120)
}

func TestBlockDownloaderDownloadBlocksWhenMissingBodiesThenPenalizePeerAndReDownload(t *testing.T) {
	test := newBlockDownloaderTestWithOpts(t, blockDownloaderTestOpts{})
	test.waypointReader.EXPECT().
		CheckpointsFromBlock(gomock.Any(), gomock.Any()).
		Return(test.fakeCheckpoints(6), nil).
		Times(1)
	test.p2pService.EXPECT().
		FetchBodies(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(
			func(
				ctx context.Context,
				headers []*types.Header,
				peerId *p2p.PeerId,
				opts ...p2p.FetcherOption,
			) (p2p.FetcherResponse[[]*types.Body], error) {
				if peerId.Equal(p2p.PeerIdFromUint64(2)) {
					return p2p.FetcherResponse[[]*types.Body]{}, p2p.NewErrMissingBodies(headers)
				}

				return test.defaultFetchBodiesMock()(ctx, headers, peerId)
			}).
		// request checkpoints 1,2,3 in parallel (we have 3 peers)
		// -> peer 2 returns missing bodies error, checkpoints 1 and 3 fetch succeeds
		// requests 2,3 in parallel (now we have only 2 peers)
		// -> checkpoint 3 blocks are cached, checkpoint 2 is re-requested from another peer and passes verifications
		// requests 4,5 in parallel
		// request 6
		// in total 6 requests + 1 request for re-requesting checkpoint 2 headers + bodies
		// total = 7 (note this also tests blocks caching works)
		Times(7)
	test.p2pService.EXPECT().
		FetchHeaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchHeadersMock()).
		// same explanation as above for FetchBodies.Times(7)
		Times(7)
	fakePeers := test.fakePeers(3)
	gomock.InOrder(
		test.p2pService.EXPECT().
			ListPeersMayHaveBlockNum(gomock.Any()).
			Return(fakePeers). // 3 initially
			Times(1),
		test.p2pService.EXPECT().
			ListPeersMayHaveBlockNum(gomock.Any()).
			Return([]*p2p.PeerId{fakePeers[0], fakePeers[2]}). // but then peer 2 gets penalized
			Times(3),
	)
	test.p2pService.EXPECT().
		Penalize(gomock.Any(), gomock.Eq(p2p.PeerIdFromUint64(2))).
		Times(1)
	var blocksBatch1, blocksBatch2 []*types.Block
	gomock.InOrder(
		test.store.EXPECT().
			InsertBlocks(gomock.Any(), gomock.Any()).
			DoAndReturn(test.defaultInsertBlocksMock(&blocksBatch1)).
			Times(1),
		test.store.EXPECT().
			InsertBlocks(gomock.Any(), gomock.Any()).
			DoAndReturn(test.defaultInsertBlocksMock(&blocksBatch2)).
			Times(3),
	)

	_, err := test.blockDownloader.DownloadBlocksUsingCheckpoints(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, blocksBatch1, 1024)
	require.Len(t, blocksBatch2, 5120)
}

func TestBlockDownloaderDownloadBlocksRespectsMaxWorkers(t *testing.T) {
	test := newBlockDownloaderTestWithOpts(t, blockDownloaderTestOpts{
		maxWorkers: 1,
	})
	test.waypointReader.EXPECT().
		CheckpointsFromBlock(gomock.Any(), gomock.Any()).
		Return(test.fakeCheckpoints(2), nil).
		Times(1)
	test.p2pService.EXPECT().
		ListPeersMayHaveBlockNum(gomock.Any()).
		Return(test.fakePeers(100)).
		Times(2)
	test.p2pService.EXPECT().
		FetchHeaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchHeadersMock()).
		Times(2)
	test.p2pService.EXPECT().
		FetchBodies(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchBodiesMock()).
		Times(2)
	var blocksBatch1, blocksBatch2 []*types.Block
	gomock.InOrder(
		test.store.EXPECT().
			InsertBlocks(gomock.Any(), gomock.Any()).
			DoAndReturn(test.defaultInsertBlocksMock(&blocksBatch1)).
			Times(1),
		test.store.EXPECT().
			InsertBlocks(gomock.Any(), gomock.Any()).
			DoAndReturn(test.defaultInsertBlocksMock(&blocksBatch2)).
			Times(1),
	)

	// max 1 worker
	// 100 peers
	// 2 waypoints
	// the downloader should fetch the 2 waypoints in 2 separate batches
	_, err := test.blockDownloader.DownloadBlocksUsingCheckpoints(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, blocksBatch1, 1024)
	require.Len(t, blocksBatch2, 1024)
}

func TestBlockDownloaderDownloadBlocksRespectsBlockLimit(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		blockLimit            uint
		numCheckpoints        int
		wantNumBlockFetches   int
		wantNumInsertedBlocks int
	}{
		{
			// limit 5000 blocks
			// 100 peers
			// 2 checkpoints x 1024 blocks each
			// the downloader should fetch 2048 blocks
			name:                  "checkpoint blocks less than limit",
			blockLimit:            5000,
			numCheckpoints:        2,
			wantNumBlockFetches:   2,
			wantNumInsertedBlocks: 2048,
		},
		{
			// limit 5000 blocks
			// 100 peers
			// 10 checkpoints x 1024 blocks each
			// the downloader should fetch 5120 blocks (we allow an extra checkpoint to pass through)
			name:                  "allow an extra checkpoint",
			blockLimit:            5000,
			numCheckpoints:        10,
			wantNumBlockFetches:   5,
			wantNumInsertedBlocks: 5120,
		},
		{
			// limit 10000 blocks
			// 100 peers
			// 10 checkpoints x 1024 blocks each
			// the downloader should fetch 10240 blocks (we allow an extra checkpoint to pass through)
			name:                  "allow an extra checkpoint (when it is last)",
			blockLimit:            10000,
			numCheckpoints:        10,
			wantNumBlockFetches:   10,
			wantNumInsertedBlocks: 10240,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			test := newBlockDownloaderTestWithOpts(t, blockDownloaderTestOpts{
				blockLimit: tc.blockLimit,
			})
			test.waypointReader.EXPECT().
				CheckpointsFromBlock(gomock.Any(), gomock.Any()).
				Return(test.fakeCheckpoints(tc.numCheckpoints), nil).
				Times(1)
			test.p2pService.EXPECT().
				ListPeersMayHaveBlockNum(gomock.Any()).
				Return(test.fakePeers(100)).
				Times(1)
			test.p2pService.EXPECT().
				FetchHeaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(test.defaultFetchHeadersMock()).
				Times(tc.wantNumBlockFetches)
			test.p2pService.EXPECT().
				FetchBodies(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(test.defaultFetchBodiesMock()).
				Times(tc.wantNumBlockFetches)
			var insertedBlocks []*types.Block
			test.store.EXPECT().
				InsertBlocks(gomock.Any(), gomock.Any()).
				DoAndReturn(test.defaultInsertBlocksMock(&insertedBlocks)).
				Times(1)

			_, err := test.blockDownloader.DownloadBlocksUsingCheckpoints(context.Background(), 1)
			require.NoError(t, err)
			require.Len(t, insertedBlocks, tc.wantNumInsertedBlocks)
		})
	}
}
