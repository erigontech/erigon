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
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/p2p"
	"github.com/ledgerwatch/erigon/turbo/testlog"
)

func newBlockDownloaderTest(t *testing.T) *blockDownloaderTest {
	return newBlockDownloaderTestWithOpts(t, blockDownloaderTestOpts{})
}

func newBlockDownloaderTestWithOpts(t *testing.T, opts blockDownloaderTestOpts) *blockDownloaderTest {
	ctrl := gomock.NewController(t)
	heimdallService := heimdall.NewMockHeimdallNoStore(ctrl)
	p2pService := p2p.NewMockService(ctrl)
	p2pService.EXPECT().MaxPeers().Return(100).Times(1)
	logger := testlog.Logger(t, log.LvlDebug)
	headersVerifier := opts.getOrCreateDefaultHeadersVerifier()
	blocksVerifier := opts.getOrCreateDefaultBlocksVerifier()
	storage := NewMockStorage(ctrl)
	headerDownloader := newBlockDownloader(
		logger,
		p2pService,
		heimdallService,
		headersVerifier,
		blocksVerifier,
		storage,
		time.Millisecond,
		opts.getOrCreateDefaultMaxWorkers(),
	)
	return &blockDownloaderTest{
		heimdall:        heimdallService,
		p2pService:      p2pService,
		blockDownloader: headerDownloader,
		storage:         storage,
	}
}

type blockDownloaderTestOpts struct {
	headersVerifier AccumulatedHeadersVerifier
	blocksVerifier  BlocksVerifier
	maxWorkers      int
}

func (opts blockDownloaderTestOpts) getOrCreateDefaultHeadersVerifier() AccumulatedHeadersVerifier {
	if opts.headersVerifier == nil {
		return func(_ heimdall.Waypoint, _ []*types.Header) error {
			return nil
		}
	}

	return opts.headersVerifier
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

type blockDownloaderTest struct {
	heimdall        *heimdall.MockHeimdallNoStore
	p2pService      *p2p.MockService
	blockDownloader *blockDownloader
	storage         *MockStorage
}

func (hdt blockDownloaderTest) fakePeers(count int) []*p2p.PeerId {
	peers := make([]*p2p.PeerId, count)
	for i := range peers {
		peers[i] = p2p.PeerIdFromUint64(uint64(i) + 1)
	}

	return peers
}

func (hdt blockDownloaderTest) fakeCheckpoints(count int) heimdall.Waypoints {
	checkpoints := make(heimdall.Waypoints, count)
	for i := range checkpoints {
		num := i + 1
		checkpoints[i] = &heimdall.Checkpoint{
			Fields: heimdall.WaypointFields{
				StartBlock: big.NewInt(int64(num)),
				EndBlock:   big.NewInt(int64(num)),
				RootHash:   common.BytesToHash([]byte(fmt.Sprintf("0x%d", num))),
			},
		}
	}

	return checkpoints
}

func (hdt blockDownloaderTest) fakeMilestones(count int) heimdall.Waypoints {
	milestones := make(heimdall.Waypoints, count)
	for i := range milestones {
		num := i + 1
		milestones[i] = &heimdall.Milestone{
			Fields: heimdall.WaypointFields{
				StartBlock: big.NewInt(int64(num)),
				EndBlock:   big.NewInt(int64(num)),
				RootHash:   common.BytesToHash([]byte(fmt.Sprintf("0x%d", num))),
			},
		}
	}

	return milestones
}

type fetchHeadersMock func(ctx context.Context, start uint64, end uint64, peerId *p2p.PeerId) (p2p.FetcherResponse[[]*types.Header], error)

func (hdt blockDownloaderTest) defaultFetchHeadersMock() fetchHeadersMock {
	// p2p.Service.FetchHeaders interface is using [start, end) so we stick to that
	return func(ctx context.Context, start uint64, end uint64, _ *p2p.PeerId) (p2p.FetcherResponse[[]*types.Header], error) {
		if start >= end {
			return p2p.FetcherResponse[[]*types.Header]{Data: nil, TotalSize: 0}, fmt.Errorf("unexpected start >= end in test: start=%d, end=%d", start, end)
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

type fetchBodiesMock func(context.Context, []*types.Header, *p2p.PeerId) (p2p.FetcherResponse[[]*types.Body], error)

func (hdt blockDownloaderTest) defaultFetchBodiesMock() fetchBodiesMock {
	return func(ctx context.Context, headers []*types.Header, _ *p2p.PeerId) (p2p.FetcherResponse[[]*types.Body], error) {
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
	test.heimdall.EXPECT().
		FetchMilestonesFromBlock(gomock.Any(), gomock.Any()).
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
	test.storage.EXPECT().
		InsertBlocks(gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultInsertBlocksMock(&blocks)).
		Times(1)

	tip, err := test.blockDownloader.DownloadBlocksUsingMilestones(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, blocks, 4)
	// check blocks are written in order
	require.Equal(t, uint64(1), blocks[0].Header().Number.Uint64())
	require.Equal(t, uint64(2), blocks[1].Header().Number.Uint64())
	require.Equal(t, uint64(3), blocks[2].Header().Number.Uint64())
	require.Equal(t, uint64(4), blocks[3].Header().Number.Uint64())
	require.Equal(t, blocks[len(blocks)-1].Header(), tip)
}

func TestBlockDownloaderDownloadBlocksUsingCheckpoints(t *testing.T) {
	test := newBlockDownloaderTest(t)
	test.heimdall.EXPECT().
		FetchCheckpointsFromBlock(gomock.Any(), gomock.Any()).
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
	test.storage.EXPECT().
		InsertBlocks(gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultInsertBlocksMock(&blocks)).
		Times(4)

	tip, err := test.blockDownloader.DownloadBlocksUsingCheckpoints(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, blocks, 8)
	// check blocks are written in order
	require.Equal(t, uint64(1), blocks[0].Header().Number.Uint64())
	require.Equal(t, uint64(2), blocks[1].Header().Number.Uint64())
	require.Equal(t, uint64(3), blocks[2].Header().Number.Uint64())
	require.Equal(t, uint64(4), blocks[3].Header().Number.Uint64())
	require.Equal(t, uint64(5), blocks[4].Header().Number.Uint64())
	require.Equal(t, uint64(6), blocks[5].Header().Number.Uint64())
	require.Equal(t, uint64(7), blocks[6].Header().Number.Uint64())
	require.Equal(t, uint64(8), blocks[7].Header().Number.Uint64())
	require.Equal(t, blocks[len(blocks)-1].Header(), tip)
}

func TestBlockDownloaderDownloadBlocksWhenInvalidHeadersThenPenalizePeerAndReDownload(t *testing.T) {
	var firstTimeInvalidReturned bool
	firstTimeInvalidReturnedPtr := &firstTimeInvalidReturned
	test := newBlockDownloaderTestWithOpts(t, blockDownloaderTestOpts{
		headersVerifier: func(waypoint heimdall.Waypoint, headers []*types.Header) error {
			if waypoint.StartBlock().Cmp(new(big.Int).SetUint64(2)) == 0 && !*firstTimeInvalidReturnedPtr {
				*firstTimeInvalidReturnedPtr = true
				return errors.New("invalid checkpoint")
			}
			return nil
		},
	})
	test.heimdall.EXPECT().
		FetchCheckpointsFromBlock(gomock.Any(), gomock.Any()).
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
		test.storage.EXPECT().
			InsertBlocks(gomock.Any(), gomock.Any()).
			DoAndReturn(test.defaultInsertBlocksMock(&blocksBatch1)).
			Times(1),
		test.storage.EXPECT().
			InsertBlocks(gomock.Any(), gomock.Any()).
			DoAndReturn(test.defaultInsertBlocksMock(&blocksBatch2)).
			Times(3),
	)

	_, err := test.blockDownloader.DownloadBlocksUsingCheckpoints(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, blocksBatch1, 1)
	require.Len(t, blocksBatch2, 5)
}

func TestBlockDownloaderDownloadBlocksWhenZeroPeersTriesAgain(t *testing.T) {
	test := newBlockDownloaderTest(t)
	test.heimdall.EXPECT().
		FetchCheckpointsFromBlock(gomock.Any(), gomock.Any()).
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
	test.storage.EXPECT().
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
	require.Len(t, blocks, 8)
	require.Equal(t, blocks[len(blocks)-1].Header(), tip)
}

func TestBlockDownloaderDownloadBlocksWhenInvalidBodiesThenPenalizePeerAndReDownload(t *testing.T) {
	var firstTimeInvalidReturned bool
	firstTimeInvalidReturnedPtr := &firstTimeInvalidReturned
	test := newBlockDownloaderTestWithOpts(t, blockDownloaderTestOpts{
		blocksVerifier: func(blocks []*types.Block) error {
			if blocks[0].NumberU64() == 2 && !*firstTimeInvalidReturnedPtr {
				*firstTimeInvalidReturnedPtr = true
				return errors.New("invalid block body")
			}
			return nil
		},
	})
	test.heimdall.EXPECT().
		FetchCheckpointsFromBlock(gomock.Any(), gomock.Any()).
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
		test.storage.EXPECT().
			InsertBlocks(gomock.Any(), gomock.Any()).
			DoAndReturn(test.defaultInsertBlocksMock(&blocksBatch1)).
			Times(1),
		test.storage.EXPECT().
			InsertBlocks(gomock.Any(), gomock.Any()).
			DoAndReturn(test.defaultInsertBlocksMock(&blocksBatch2)).
			Times(3),
	)

	_, err := test.blockDownloader.DownloadBlocksUsingCheckpoints(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, blocksBatch1, 1)
	require.Len(t, blocksBatch2, 5)
}

func TestBlockDownloaderDownloadBlocksWhenMissingBodiesThenPenalizePeerAndReDownload(t *testing.T) {
	test := newBlockDownloaderTestWithOpts(t, blockDownloaderTestOpts{})
	test.heimdall.EXPECT().
		FetchCheckpointsFromBlock(gomock.Any(), gomock.Any()).
		Return(test.fakeCheckpoints(6), nil).
		Times(1)
	test.p2pService.EXPECT().
		FetchBodies(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, headers []*types.Header, peerId *p2p.PeerId) (p2p.FetcherResponse[[]*types.Body], error) {
			if peerId.Equal(p2p.PeerIdFromUint64(2)) {
				return p2p.FetcherResponse[[]*types.Body]{Data: nil, TotalSize: 0}, p2p.NewErrMissingBodies(headers)
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
		test.storage.EXPECT().
			InsertBlocks(gomock.Any(), gomock.Any()).
			DoAndReturn(test.defaultInsertBlocksMock(&blocksBatch1)).
			Times(1),
		test.storage.EXPECT().
			InsertBlocks(gomock.Any(), gomock.Any()).
			DoAndReturn(test.defaultInsertBlocksMock(&blocksBatch2)).
			Times(3),
	)

	_, err := test.blockDownloader.DownloadBlocksUsingCheckpoints(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, blocksBatch1, 1)
	require.Len(t, blocksBatch2, 5)
}

func TestBlockDownloaderDownloadBlocksRespectsMaxWorkers(t *testing.T) {
	test := newBlockDownloaderTestWithOpts(t, blockDownloaderTestOpts{
		maxWorkers: 1,
	})
	test.heimdall.EXPECT().
		FetchCheckpointsFromBlock(gomock.Any(), gomock.Any()).
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
		test.storage.EXPECT().
			InsertBlocks(gomock.Any(), gomock.Any()).
			DoAndReturn(test.defaultInsertBlocksMock(&blocksBatch1)).
			Times(1),
		test.storage.EXPECT().
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
	require.Len(t, blocksBatch1, 1)
	require.Len(t, blocksBatch2, 1)
}
