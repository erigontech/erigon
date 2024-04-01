package sync

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/p2p"
)

const (
	notEnoughPeersBackOffDuration = time.Minute

	// conservative over-estimation: 1 MB block size x 1024 blocks per waypoint
	blockDownloaderEstimatedRamPerWorker = estimate.EstimatedRamPerWorker(1 * datasize.GB)
)

type BlockDownloader interface {
	DownloadBlocksUsingCheckpoints(ctx context.Context, start uint64) (tip *types.Header, err error)
	DownloadBlocksUsingMilestones(ctx context.Context, start uint64) (tip *types.Header, err error)
}

func NewBlockDownloader(
	logger log.Logger,
	p2pService p2p.Service,
	heimdall heimdall.HeimdallNoStore,
	headersVerifier AccumulatedHeadersVerifier,
	blocksVerifier BlocksVerifier,
	storage Storage,
) BlockDownloader {
	return newBlockDownloader(
		logger,
		p2pService,
		heimdall,
		headersVerifier,
		blocksVerifier,
		storage,
		notEnoughPeersBackOffDuration,
		blockDownloaderEstimatedRamPerWorker.WorkersByRAMOnly(),
	)
}

func newBlockDownloader(
	logger log.Logger,
	p2pService p2p.Service,
	heimdall heimdall.HeimdallNoStore,
	headersVerifier AccumulatedHeadersVerifier,
	blocksVerifier BlocksVerifier,
	storage Storage,
	notEnoughPeersBackOffDuration time.Duration,
	maxWorkers int,
) *blockDownloader {
	return &blockDownloader{
		logger:                        logger,
		p2pService:                    p2pService,
		heimdall:                      heimdall,
		headersVerifier:               headersVerifier,
		blocksVerifier:                blocksVerifier,
		storage:                       storage,
		notEnoughPeersBackOffDuration: notEnoughPeersBackOffDuration,
		maxWorkers:                    maxWorkers,
	}
}

type blockDownloader struct {
	logger                        log.Logger
	p2pService                    p2p.Service
	heimdall                      heimdall.HeimdallNoStore
	headersVerifier               AccumulatedHeadersVerifier
	blocksVerifier                BlocksVerifier
	storage                       Storage
	notEnoughPeersBackOffDuration time.Duration
	maxWorkers                    int
}

func (d *blockDownloader) DownloadBlocksUsingCheckpoints(ctx context.Context, start uint64) (*types.Header, error) {
	waypoints, err := d.heimdall.FetchCheckpointsFromBlock(ctx, start)
	if err != nil {
		return nil, err
	}

	return d.downloadBlocksUsingWaypoints(ctx, waypoints)
}

func (d *blockDownloader) DownloadBlocksUsingMilestones(ctx context.Context, start uint64) (*types.Header, error) {
	waypoints, err := d.heimdall.FetchMilestonesFromBlock(ctx, start)
	if err != nil {
		return nil, err
	}

	return d.downloadBlocksUsingWaypoints(ctx, waypoints)
}

func (d *blockDownloader) downloadBlocksUsingWaypoints(ctx context.Context, waypoints heimdall.Waypoints) (*types.Header, error) {
	if len(waypoints) == 0 {
		return nil, nil
	}

	// waypoint rootHash->[blocks part of waypoint]
	waypointBlocksMemo, err := lru.New[common.Hash, []*types.Block](d.p2pService.MaxPeers())
	if err != nil {
		return nil, err
	}

	progressLogTimer := time.NewTimer(30 * time.Second)
	defer progressLogTimer.Stop()

	var lastBlock *types.Block
	lastBlockNum := waypoints[len(waypoints)-1].EndBlock().Uint64()
	for len(waypoints) > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// carry-on
		}

		endBlockNum := waypoints[len(waypoints)-1].EndBlock().Uint64()
		peers := d.p2pService.ListPeersMayHaveBlockNum(endBlockNum)
		if len(peers) == 0 {
			d.logger.Warn(
				syncLogPrefix("can't use any peers to download blocks, will try again in a bit"),
				"start", waypoints[0].StartBlock(),
				"end", endBlockNum,
				"sleepSeconds", d.notEnoughPeersBackOffDuration.Seconds(),
			)

			time.Sleep(d.notEnoughPeersBackOffDuration)
			continue
		}

		numWorkers := cmp.Min(cmp.Min(d.maxWorkers, len(peers)), len(waypoints))
		waypointsBatch := waypoints[:numWorkers]

		select {
		case <-progressLogTimer.C:
			d.logger.Info(
				syncLogPrefix("downloading blocks progress"),
				"waypointsBatchLength", len(waypointsBatch),
				"startBlockNum", waypointsBatch[0].StartBlock(),
				"endBlockNum", waypointsBatch[len(waypointsBatch)-1].EndBlock(),
				"kind", reflect.TypeOf(waypointsBatch[0]),
				"peerCount", len(peers),
				"maxWorkers", d.maxWorkers,
			)
		default:
			// carry on
		}

		blockBatches := make([][]*types.Block, len(waypointsBatch))
		maxWaypointLength := float64(0)
		wg := sync.WaitGroup{}
		for i, waypoint := range waypointsBatch {
			maxWaypointLength = math.Max(float64(waypoint.Length()), maxWaypointLength)
			wg.Add(1)
			go func(i int, waypoint heimdall.Waypoint, peerId *p2p.PeerId) {
				defer wg.Done()

				if blocks, ok := waypointBlocksMemo.Get(waypoint.RootHash()); ok {
					blockBatches[i] = blocks
					return
				}

				blocks, err := d.fetchVerifiedBlocks(ctx, waypoint, peerId)
				if err != nil {
					d.logger.Debug(
						syncLogPrefix("issue downloading waypoint blocks - will try again"),
						"err", err,
						"start", waypoint.StartBlock(),
						"end", waypoint.EndBlock(),
						"rootHash", waypoint.RootHash(),
						"kind", reflect.TypeOf(waypoint),
						"peerId", peerId,
					)

					return
				}

				waypointBlocksMemo.Add(waypoint.RootHash(), blocks)
				blockBatches[i] = blocks
			}(i, waypoint, peers[i])
		}

		wg.Wait()
		blocks := make([]*types.Block, 0, int(maxWaypointLength)*len(waypointsBatch))
		gapIndex := -1
		for i, blockBatch := range blockBatches {
			if len(blockBatch) == 0 {
				d.logger.Debug(
					syncLogPrefix("no blocks - will try again"),
					"start", waypointsBatch[i].StartBlock(),
					"end", waypointsBatch[i].EndBlock(),
					"rootHash", waypointsBatch[i].RootHash(),
					"kind", reflect.TypeOf(waypointsBatch[i]),
				)

				gapIndex = i
				break
			}

			blocks = append(blocks, blockBatch...)
		}

		if gapIndex >= 0 {
			waypoints = waypoints[gapIndex:]
		} else {
			waypoints = waypoints[len(waypointsBatch):]
		}

		if len(blocks) > 0 {
			if err := d.storage.InsertBlocks(ctx, blocks); err != nil {
				return nil, err
			}

			flushStartTime := time.Now()
			if err := d.storage.Flush(ctx); err != nil {
				return nil, err
			}

			d.logger.Debug(
				syncLogPrefix("stored blocks"),
				"len", len(blocks),
				"duration", time.Since(flushStartTime),
			)
		}

		if (endBlockNum == lastBlockNum) && (len(blocks) > 0) {
			lastBlock = blocks[len(blocks)-1]
		}
	}

	return lastBlock.Header(), nil
}

func (d *blockDownloader) fetchVerifiedBlocks(
	ctx context.Context,
	waypoint heimdall.Waypoint,
	peerId *p2p.PeerId,
) ([]*types.Block, error) {
	// 1. Fetch headers in waypoint from a peer
	start := waypoint.StartBlock().Uint64()
	end := waypoint.EndBlock().Uint64() + 1 // waypoint end is inclusive, fetch headers is [start, end)
	headers, err := d.p2pService.FetchHeaders(ctx, start, end, peerId)
	if err != nil {
		return nil, err
	}

	// 2. Verify headers match waypoint root hash
	if err = d.headersVerifier(waypoint, headers); err != nil {
		d.logger.Debug(syncLogPrefix("penalizing peer - invalid headers"), "peerId", peerId, "err", err)

		if penalizeErr := d.p2pService.Penalize(ctx, peerId); penalizeErr != nil {
			err = fmt.Errorf("%w: %w", penalizeErr, err)
		}

		return nil, err
	}

	// 3. Fetch bodies for the verified waypoint headers
	bodies, err := d.p2pService.FetchBodies(ctx, headers, peerId)
	if err != nil {
		if errors.Is(err, &p2p.ErrMissingBodies{}) {
			d.logger.Debug(syncLogPrefix("penalizing peer - missing bodies"), "peerId", peerId, "err", err)

			if penalizeErr := d.p2pService.Penalize(ctx, peerId); penalizeErr != nil {
				err = fmt.Errorf("%w: %w", penalizeErr, err)
			}
		}

		return nil, err
	}

	// 4. Assemble blocks
	blocks := make([]*types.Block, len(headers))
	for i, header := range headers {
		blocks[i] = types.NewBlockFromNetwork(header, bodies[i])
	}

	// 5. Verify blocks
	if err = d.blocksVerifier(blocks); err != nil {
		d.logger.Debug(syncLogPrefix("penalizing peer - invalid blocks"), "peerId", peerId, "err", err)

		if penalizeErr := d.p2pService.Penalize(ctx, peerId); penalizeErr != nil {
			err = fmt.Errorf("%w: %w", penalizeErr, err)
		}

		return nil, err
	}

	return blocks, nil
}
