package sync

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/p2p"
)

const (
	blockDownloaderLogPrefix      = "BlockDownloader"
	notEnoughPeersBackOffDuration = time.Minute
)

type BlockDownloader interface {
	DownloadBlocksUsingCheckpoints(ctx context.Context, start uint64) (latest *types.Header, err error)
	DownloadBlocksUsingMilestones(ctx context.Context, start uint64) (latest *types.Header, err error)
}

func NewBlockDownloader(
	logger log.Logger,
	p2pService p2p.Service,
	heimdall heimdall.HeimdallNoStore,
	headersVerifier AccumulatedHeadersVerifier,
	bodiesVerifier BodiesVerifier,
	storage Storage,
) BlockDownloader {
	return newBlockDownloader(
		logger,
		p2pService,
		heimdall,
		headersVerifier,
		bodiesVerifier,
		storage,
		notEnoughPeersBackOffDuration,
	)
}

func newBlockDownloader(
	logger log.Logger,
	p2pService p2p.Service,
	heimdall heimdall.HeimdallNoStore,
	headersVerifier AccumulatedHeadersVerifier,
	bodiesVerifier BodiesVerifier,
	storage Storage,
	notEnoughPeersBackOffDuration time.Duration,
) *blockDownloader {
	return &blockDownloader{
		logger:                        logger,
		p2pService:                    p2pService,
		heimdall:                      heimdall,
		headersVerifier:               headersVerifier,
		bodiesVerifier:                bodiesVerifier,
		storage:                       storage,
		notEnoughPeersBackOffDuration: notEnoughPeersBackOffDuration,
	}
}

type blockDownloader struct {
	logger                        log.Logger
	p2pService                    p2p.Service
	heimdall                      heimdall.HeimdallNoStore
	headersVerifier               AccumulatedHeadersVerifier
	bodiesVerifier                BodiesVerifier
	storage                       Storage
	notEnoughPeersBackOffDuration time.Duration
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

	var lastBlock *types.Block
	lastBlockNum := waypoints[len(waypoints)-1].EndBlock().Uint64()
	for len(waypoints) > 0 {
		endBlockNum := waypoints[len(waypoints)-1].EndBlock().Uint64()
		peers := d.p2pService.ListPeersMayHaveBlockNum(endBlockNum)
		if len(peers) == 0 {
			d.logger.Warn(
				fmt.Sprintf("[%s] can't use any peers to sync, will try again", blockDownloaderLogPrefix),
				"start", waypoints[0].StartBlock(),
				"end", endBlockNum,
				"sleepSeconds", d.notEnoughPeersBackOffDuration.Seconds(),
			)

			time.Sleep(d.notEnoughPeersBackOffDuration)
			continue
		}

		peerCount := len(peers)
		waypointsBatch := waypoints
		if len(waypointsBatch) > peerCount {
			waypointsBatch = waypointsBatch[:peerCount]
		}

		d.logger.Info(
			fmt.Sprintf("[%s] downloading blocks", blockDownloaderLogPrefix),
			"waypointsBatchLength", len(waypointsBatch),
			"startBlockNum", waypointsBatch[0].StartBlock(),
			"endBlockNum", waypointsBatch[len(waypointsBatch)-1].EndBlock(),
			"kind", reflect.TypeOf(waypointsBatch[0]),
			"peerCount", peerCount,
		)

		//
		// TODO (for discussion and subsequent PRs)
		//      we may 1) need ETL or 2) limit level of parallelism to fit in RAM reasonably
		//      if we have 50 peers => that is 50 goroutine parallelism
		//      => 50 checkpoints => 1024 blocks each in the worst case
		//      => 512 KB per block in the worst case => 25 GB needed in the worst case
		//      Option 1) introduces additional ETL IO but leverages higher throughput by downloading
		//                from more peers in parallel
		//      Option 2) would not introduce additional IO however would utilise less throughput since
		//                it will be downloading from say 16 peers in parallel instead of from say 60
		//                (maybe 16 peers in parallel is still quite performant?)
		//
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

				headers, err := d.fetchVerifiedHeaders(ctx, waypoint, peerId)
				if err != nil {
					d.logger.Debug(
						fmt.Sprintf("[%s] issue downloading headers - will try again", blockDownloaderLogPrefix),
						"err", err,
						"start", waypoint.StartBlock(),
						"end", waypoint.EndBlock(),
						"rootHash", waypoint.RootHash(),
						"kind", reflect.TypeOf(waypoint),
						"peerId", peerId,
					)

					return
				}

				bodies, err := d.fetchVerifiedBodies(ctx, headers, peerId)
				if err != nil {
					d.logger.Debug(
						fmt.Sprintf("[%s] issue downloading bodies - will try again", blockDownloaderLogPrefix),
						"err", err,
						"start", waypoint.StartBlock(),
						"end", waypoint.EndBlock(),
						"rootHash", waypoint.RootHash(),
						"kind", reflect.TypeOf(waypoint),
						"peerId", peerId,
					)

					return
				}

				blocks := make([]*types.Block, len(headers))
				for i, header := range headers {
					body := bodies[i]
					blocks[i] = types.NewBlock(header, body.Transactions, body.Uncles, nil, body.Withdrawals)
				}

				waypointBlocksMemo.Add(waypoint.RootHash(), blocks)
				blockBatches[i] = blocks
			}(i, waypoint, peers[i])
		}

		wg.Wait()
		blocks := make([]*types.Block, 0, int(maxWaypointLength)*peerCount)
		gapIndex := -1
		for i, blockBatch := range blockBatches {
			if len(blockBatch) == 0 {
				d.logger.Debug(
					fmt.Sprintf("[%s] no blocks - will try again", blockDownloaderLogPrefix),
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

		if err := d.storage.InsertBlocks(ctx, blocks); err != nil {
			return nil, err
		}

		flushStartTime := time.Now()
		if err := d.storage.Flush(ctx); err != nil {
			return nil, err
		}

		d.logger.Debug(
			fmt.Sprintf("[%s] stored blocks", blockDownloaderLogPrefix),
			"len", len(blocks),
			"duration", time.Since(flushStartTime),
		)

		if (endBlockNum == lastBlockNum) && (len(blocks) > 0) {
			lastBlock = blocks[len(blocks)-1]
		}
	}

	return lastBlock.Header(), nil
}

func (d *blockDownloader) fetchVerifiedHeaders(
	ctx context.Context,
	waypoint heimdall.Waypoint,
	peerId *p2p.PeerId,
) ([]*types.Header, error) {
	start := waypoint.StartBlock().Uint64()
	end := waypoint.EndBlock().Uint64() + 1 // waypoint end is inclusive, fetch headers is [start, end)
	headers, err := d.p2pService.FetchHeaders(ctx, start, end, peerId)
	if err != nil {
		return nil, err
	}

	if err := d.headersVerifier(waypoint, headers); err != nil {
		d.logger.Debug(
			fmt.Sprintf("[%s] penalizing peer - bad headers received for waypoint", blockDownloaderLogPrefix),
			"start", waypoint.StartBlock(),
			"end", waypoint.EndBlock(),
			"rootHash", waypoint.RootHash(),
			"kind", reflect.TypeOf(waypoint),
			"peerId", peerId,
		)

		if penalizeErr := d.p2pService.Penalize(ctx, peerId); penalizeErr != nil {
			err = fmt.Errorf("%w: %w", penalizeErr, err)
		}

		return nil, err
	}

	return headers, nil
}

func (d *blockDownloader) fetchVerifiedBodies(
	ctx context.Context,
	headers []*types.Header,
	peerId *p2p.PeerId,
) ([]*types.Body, error) {
	bodies, err := d.p2pService.FetchBodies(ctx, headers, peerId)
	if err != nil {
		if errors.Is(err, &p2p.ErrMissingBodies{}) {
			d.logger.Debug(
				fmt.Sprintf("[%s] penalizing peer - missing bodies for headers", blockDownloaderLogPrefix),
				"start", headers[0].Number.Uint64(),
				"end", headers[len(headers)-1].Number.Uint64(),
				"peerId", peerId,
			)

			if penalizeErr := d.p2pService.Penalize(ctx, peerId); penalizeErr != nil {
				err = fmt.Errorf("%w: %w", penalizeErr, err)
			}
		}

		return nil, err
	}

	if err := d.bodiesVerifier(headers, bodies); err != nil {
		d.logger.Debug(
			fmt.Sprintf("[%s] penalizing peer - bad bodies received for headers", blockDownloaderLogPrefix),
			"start", headers[0].Number.Uint64(),
			"end", headers[len(headers)-1].Number.Uint64(),
			"peerId", peerId,
		)

		if penalizeErr := d.p2pService.Penalize(ctx, peerId); penalizeErr != nil {
			err = fmt.Errorf("%w: %w", penalizeErr, err)
		}

		return nil, err
	}

	return bodies, nil
}
