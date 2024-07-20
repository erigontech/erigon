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
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/p2p"
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
	heimdall heimdallWaypointsFetcher,
	checkpointVerifier WaypointHeadersVerifier,
	milestoneVerifier WaypointHeadersVerifier,
	blocksVerifier BlocksVerifier,
	store Store,
	blockLimit uint,
) BlockDownloader {
	return newBlockDownloader(
		logger,
		p2pService,
		heimdall,
		checkpointVerifier,
		milestoneVerifier,
		blocksVerifier,
		store,
		notEnoughPeersBackOffDuration,
		blockDownloaderEstimatedRamPerWorker.WorkersByRAMOnly(),
		blockLimit,
	)
}

func newBlockDownloader(
	logger log.Logger,
	p2pService p2p.Service,
	heimdall heimdallWaypointsFetcher,
	checkpointVerifier WaypointHeadersVerifier,
	milestoneVerifier WaypointHeadersVerifier,
	blocksVerifier BlocksVerifier,
	store Store,
	notEnoughPeersBackOffDuration time.Duration,
	maxWorkers int,
	blockLimit uint,
) *blockDownloader {
	return &blockDownloader{
		logger:                        logger,
		p2pService:                    p2pService,
		heimdall:                      heimdall,
		checkpointVerifier:            checkpointVerifier,
		milestoneVerifier:             milestoneVerifier,
		blocksVerifier:                blocksVerifier,
		store:                         store,
		notEnoughPeersBackOffDuration: notEnoughPeersBackOffDuration,
		maxWorkers:                    maxWorkers,
		blockLimit:                    blockLimit,
	}
}

type blockDownloader struct {
	logger                        log.Logger
	p2pService                    p2p.Service
	heimdall                      heimdallWaypointsFetcher
	checkpointVerifier            WaypointHeadersVerifier
	milestoneVerifier             WaypointHeadersVerifier
	blocksVerifier                BlocksVerifier
	store                         Store
	notEnoughPeersBackOffDuration time.Duration
	maxWorkers                    int
	blockLimit                    uint
}

func (d *blockDownloader) DownloadBlocksUsingCheckpoints(ctx context.Context, start uint64) (*types.Header, error) {
	waypoints, err := d.heimdall.FetchCheckpointsFromBlock(ctx, start)
	if err != nil {
		return nil, err
	}

	return d.downloadBlocksUsingWaypoints(ctx, waypoints, d.checkpointVerifier)
}

func (d *blockDownloader) DownloadBlocksUsingMilestones(ctx context.Context, start uint64) (*types.Header, error) {
	waypoints, err := d.heimdall.FetchMilestonesFromBlock(ctx, start)
	if err != nil {
		return nil, err
	}

	return d.downloadBlocksUsingWaypoints(ctx, waypoints, d.milestoneVerifier)
}

func (d *blockDownloader) downloadBlocksUsingWaypoints(
	ctx context.Context,
	waypoints heimdall.Waypoints,
	verifier WaypointHeadersVerifier,
) (*types.Header, error) {
	if len(waypoints) == 0 {
		return nil, nil
	}

	waypoints = d.limitWaypoints(waypoints)

	d.logger.Debug(
		syncLogPrefix("downloading blocks using waypoints"),
		"waypointsLen", len(waypoints),
		"start", waypoints[0].StartBlock().Uint64(),
		"end", waypoints[len(waypoints)-1].EndBlock().Uint64(),
		"kind", reflect.TypeOf(waypoints[0]),
		"blockLimit", d.blockLimit,
	)

	// waypoint rootHash->[blocks part of waypoint]
	waypointBlocksMemo, err := lru.New[common.Hash, []*types.Block](d.p2pService.MaxPeers())
	if err != nil {
		return nil, err
	}

	progressLogTicker := time.NewTicker(30 * time.Second)
	defer progressLogTicker.Stop()

	var lastBlock *types.Block
	batchFetchStartTime := time.Now()
	fetchStartTime := time.Now()
	var blockCount, blocksTotalSize atomic.Uint64

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

			if err := common.Sleep(ctx, d.notEnoughPeersBackOffDuration); err != nil {
				return nil, err
			}

			continue
		}

		numWorkers := min(d.maxWorkers, len(peers), len(waypoints))
		waypointsBatch := waypoints[:numWorkers]

		select {
		case <-progressLogTicker.C:
			d.logger.Info(
				syncLogPrefix("downloading blocks progress"),
				"waypointsBatchLength", len(waypointsBatch),
				"startBlockNum", waypointsBatch[0].StartBlock(),
				"endBlockNum", waypointsBatch[len(waypointsBatch)-1].EndBlock(),
				"kind", reflect.TypeOf(waypointsBatch[0]),
				"peerCount", len(peers),
				"maxWorkers", d.maxWorkers,
				"blk/s", fmt.Sprintf("%.2f", float64(blockCount.Load())/time.Since(fetchStartTime).Seconds()),
				"bytes/s", fmt.Sprintf("%s", common.ByteCount(uint64(float64(blocksTotalSize.Load())/time.Since(fetchStartTime).Seconds()))),
			)

			blockCount.Store(0)
			blocksTotalSize.Store(0)
			fetchStartTime = time.Now()

		default:
			// carry on
		}

		blockBatches := make([][]*types.Block, len(waypointsBatch))
		maxWaypointLength := uint64(0)
		wg := sync.WaitGroup{}
		for i, waypoint := range waypointsBatch {
			maxWaypointLength = max(waypoint.Length(), maxWaypointLength)
			wg.Add(1)
			go func(i int, waypoint heimdall.Waypoint, peerId *p2p.PeerId) {
				defer wg.Done()

				if blocks, ok := waypointBlocksMemo.Get(waypoint.RootHash()); ok {
					blockBatches[i] = blocks
					return
				}

				blocks, totalSize, err := d.fetchVerifiedBlocks(ctx, waypoint, peerId, verifier)
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

				blocksTotalSize.Add(uint64(totalSize))
				blockCount.Add(uint64(len(blocks)))

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

			if blockBatch[0].Number().Uint64() == 0 {
				// we do not want to insert block 0 (genesis)
				blockBatch = blockBatch[1:]
			}

			blocks = append(blocks, blockBatch...)
		}

		if gapIndex >= 0 {
			waypoints = waypoints[gapIndex:]
		} else {
			waypoints = waypoints[len(waypointsBatch):]
		}

		if len(blocks) == 0 {
			continue
		}

		d.logger.Debug(syncLogPrefix("fetched blocks"), "len", len(blocks), "duration", time.Since(batchFetchStartTime))

		batchFetchStartTime = time.Now() // reset for next time

		if err := d.store.InsertBlocks(ctx, blocks); err != nil {
			return nil, err
		}

		lastBlock = blocks[len(blocks)-1]
	}

	d.logger.Debug(syncLogPrefix("finished downloading blocks using waypoints"))

	return lastBlock.Header(), nil
}

func (d *blockDownloader) fetchVerifiedBlocks(
	ctx context.Context,
	waypoint heimdall.Waypoint,
	peerId *p2p.PeerId,
	verifier WaypointHeadersVerifier,
) ([]*types.Block, int, error) {
	// 1. Fetch headers in waypoint from a peer
	start := waypoint.StartBlock().Uint64()
	end := waypoint.EndBlock().Uint64() + 1 // waypoint end is inclusive, fetch headers is [start, end)
	headers, err := d.p2pService.FetchHeaders(ctx, start, end, peerId)
	if err != nil {
		return nil, 0, err
	}

	// 2. Verify headers match waypoint root hash
	if err = verifier(waypoint, headers.Data); err != nil {
		d.logger.Debug(syncLogPrefix("penalizing peer - invalid headers"), "peerId", peerId, "err", err)

		if penalizeErr := d.p2pService.Penalize(ctx, peerId); penalizeErr != nil {
			err = fmt.Errorf("%w: %w", penalizeErr, err)
		}

		return nil, 0, err
	}

	// 3. Fetch bodies for the verified waypoint headers
	bodies, err := d.p2pService.FetchBodies(ctx, headers.Data, peerId)
	if err != nil {
		if errors.Is(err, &p2p.ErrMissingBodies{}) {
			d.logger.Debug(syncLogPrefix("penalizing peer - missing bodies"), "peerId", peerId, "err", err)

			if penalizeErr := d.p2pService.Penalize(ctx, peerId); penalizeErr != nil {
				err = fmt.Errorf("%w: %w", penalizeErr, err)
			}
		}

		return nil, 0, err
	}

	// 4. Assemble blocks
	blocks := make([]*types.Block, len(headers.Data))
	for i, header := range headers.Data {
		blocks[i] = types.NewBlockFromNetwork(header, bodies.Data[i])
	}

	// 5. Verify blocks
	if err = d.blocksVerifier(blocks); err != nil {
		d.logger.Debug(syncLogPrefix("penalizing peer - invalid blocks"), "peerId", peerId, "err", err)

		if penalizeErr := d.p2pService.Penalize(ctx, peerId); penalizeErr != nil {
			err = fmt.Errorf("%w: %w", penalizeErr, err)
		}

		return nil, 0, err
	}

	return blocks, headers.TotalSize + bodies.TotalSize, nil
}

func (d *blockDownloader) limitWaypoints(waypoints []heimdall.Waypoint) []heimdall.Waypoint {
	if d.blockLimit == 0 {
		return waypoints
	}

	startBlockNum := waypoints[0].StartBlock().Uint64()
	for i, waypoint := range waypoints {
		// we allow a bit of surplus to overflow above the block limit at waypoint boundary
		if waypoint.EndBlock().Uint64()-startBlockNum < uint64(d.blockLimit) {
			continue
		}

		waypoints = waypoints[:i+1]
		break
	}

	return waypoints
}
