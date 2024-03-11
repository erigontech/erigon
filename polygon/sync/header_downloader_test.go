package sync

import (
	"context"
	"errors"
	"fmt"
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

func newHeaderDownloaderTest(t *testing.T) *headerDownloaderTest {
	return newHeaderDownloaderTestWithOpts(t, headerDownloaderTestOpts{})
}

func newHeaderDownloaderTestWithOpts(t *testing.T, opts headerDownloaderTestOpts) *headerDownloaderTest {
	ctrl := gomock.NewController(t)
	heimdallService := heimdall.NewMockHeimdallNoStore(ctrl)
	p2pService := p2p.NewMockService(ctrl)
	p2pService.EXPECT().MaxPeers().Return(100).Times(1)
	logger := testlog.Logger(t, log.LvlDebug)
	headersVerifier := opts.getOrCreateDefaultHeadersVerifier()
	bodiesVerifier := opts.getOrCreateDefaultBodiesVerifier()
	storage := NewMockStorage(ctrl)
	headerDownloader := newHeaderDownloader(
		logger,
		p2pService,
		heimdallService,
		headersVerifier,
		bodiesVerifier,
		storage,
		time.Millisecond,
	)
	return &headerDownloaderTest{
		heimdall:         heimdallService,
		p2pService:       p2pService,
		headerDownloader: headerDownloader,
		storage:          storage,
	}
}

type headerDownloaderTestOpts struct {
	headersVerifier AccumulatedHeadersVerifier
	bodiesVerifier  BodiesVerifier
}

func (opts headerDownloaderTestOpts) getOrCreateDefaultHeadersVerifier() AccumulatedHeadersVerifier {
	if opts.headersVerifier == nil {
		return func(_ heimdall.Waypoint, _ []*types.Header) error {
			return nil
		}
	}

	return opts.headersVerifier
}

func (opts headerDownloaderTestOpts) getOrCreateDefaultBodiesVerifier() BodiesVerifier {
	if opts.bodiesVerifier == nil {
		return func(_ []*types.Header, _ []*types.Body) error {
			return nil
		}
	}

	return opts.bodiesVerifier
}

type headerDownloaderTest struct {
	heimdall         *heimdall.MockHeimdallNoStore
	p2pService       *p2p.MockService
	headerDownloader HeaderDownloader
	storage          *MockStorage
}

func (hdt headerDownloaderTest) fakePeers(count int) []*p2p.PeerId {
	peers := make([]*p2p.PeerId, count)
	for i := range peers {
		peers[i] = p2p.PeerIdFromUint64(uint64(i) + 1)
	}

	return peers
}

func (hdt headerDownloaderTest) fakeCheckpoints(count int) heimdall.Waypoints {
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

func (hdt headerDownloaderTest) fakeMilestones(count int) heimdall.Waypoints {
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

type fetchHeadersMock func(ctx context.Context, start uint64, end uint64, peerId *p2p.PeerId) ([]*types.Header, error)

func (hdt headerDownloaderTest) defaultFetchHeadersMock() fetchHeadersMock {
	// p2p.Service.FetchHeaders interface is using [start, end) so we stick to that
	return func(ctx context.Context, start uint64, end uint64, _ *p2p.PeerId) ([]*types.Header, error) {
		if start >= end {
			return nil, fmt.Errorf("unexpected start >= end in test: start=%d, end=%d", start, end)
		}

		res := make([]*types.Header, end-start)
		for num := start; num < end; num++ {
			res[num-start] = &types.Header{
				Number: new(big.Int).SetUint64(num),
			}
		}

		return res, nil
	}
}

type fetchBodiesMock func(context.Context, []*types.Header, *p2p.PeerId) ([]*types.Body, error)

func (hdt headerDownloaderTest) defaultFetchBodiesMock() fetchBodiesMock {
	return func(ctx context.Context, headers []*types.Header, _ *p2p.PeerId) ([]*types.Body, error) {
		bodies := make([]*types.Body, len(headers))
		for i := range headers {
			bodies[i] = &types.Body{
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
		}

		return bodies, nil
	}
}

func (hdt headerDownloaderTest) defaultInsertBlocksMock(capture *[]*types.Block) func(context.Context, []*types.Block) error {
	return func(ctx context.Context, blocks []*types.Block) error {
		*capture = append(*capture, blocks...)
		return nil
	}
}

func TestDownloadBlocksUsingMilestones(t *testing.T) {
	test := newHeaderDownloaderTest(t)
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
	test.storage.EXPECT().
		Flush(gomock.Any()).
		Return(nil).
		Times(1)

	lastHeader, err := test.headerDownloader.DownloadUsingMilestones(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, blocks, 4)
	// check blocks are written in order
	require.Equal(t, uint64(1), blocks[0].Header().Number.Uint64())
	require.Equal(t, uint64(2), blocks[1].Header().Number.Uint64())
	require.Equal(t, uint64(3), blocks[2].Header().Number.Uint64())
	require.Equal(t, uint64(4), blocks[3].Header().Number.Uint64())
	require.Equal(t, blocks[len(blocks)-1].Header(), lastHeader)
}

func TestDownloadBlocksUsingCheckpoints(t *testing.T) {
	test := newHeaderDownloaderTest(t)
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
	test.storage.EXPECT().
		Flush(gomock.Any()).
		Return(nil).
		Times(4)

	lastHeader, err := test.headerDownloader.DownloadUsingCheckpoints(context.Background(), 1)
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
	require.Equal(t, blocks[len(blocks)-1].Header(), lastHeader)
}

func TestDownloadBlocksWhenInvalidHeadersThenPenalizePeerAndReDownload(t *testing.T) {
	var firstTimeInvalidReturned bool
	firstTimeInvalidReturnedPtr := &firstTimeInvalidReturned
	test := newHeaderDownloaderTestWithOpts(t, headerDownloaderTestOpts{
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
		ListPeersMayHaveBlockNum(gomock.Any()).
		Return(test.fakePeers(3)).
		Times(3)
	test.p2pService.EXPECT().
		FetchHeaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchHeadersMock()).
		// request 1,2,3 in parallel
		// -> 2 fails
		// requests 2,3,4 in parallel
		// 3 is cached
		// requests 5,6 in parallel
		// in total 6 requests + 1 request for re-requesting checkpoint 2
		// total = 7 (note this also tests caching works)
		Times(7)
	test.p2pService.EXPECT().
		FetchBodies(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(test.defaultFetchBodiesMock()).
		Times(6)
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
			Times(2),
	)
	test.storage.EXPECT().
		Flush(gomock.Any()).
		Return(nil).
		Times(3)

	_, err := test.headerDownloader.DownloadUsingCheckpoints(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, blocksBatch1, 1)
	require.Len(t, blocksBatch2, 5)
}

func TestDownloadBlocksWhenZeroPeersTriesAgain(t *testing.T) {
	test := newHeaderDownloaderTest(t)
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
	test.storage.EXPECT().
		Flush(gomock.Any()).
		Return(nil).
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

	lastHeader, err := test.headerDownloader.DownloadUsingCheckpoints(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, blocks, 8)
	require.Equal(t, blocks[len(blocks)-1].Header(), lastHeader)
}
