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
	headerDownloaderLogPrefix     = "HeaderDownloader"
	notEnoughPeersBackOffDuration = time.Minute
)

type HeaderDownloader interface {
	DownloadUsingCheckpoints(ctx context.Context, start uint64) (*types.Header, error)
	DownloadUsingMilestones(ctx context.Context, start uint64) (*types.Header, error)
}

func NewHeaderDownloader(
	logger log.Logger,
	p2pService p2p.Service,
	heimdall heimdall.HeimdallNoStore,
	headersVerifier AccumulatedHeadersVerifier,
	bodiesVerifier BodiesVerifier,
	storage Storage,
) HeaderDownloader {
	return newHeaderDownloader(
		logger,
		p2pService,
		heimdall,
		headersVerifier,
		bodiesVerifier,
		storage,
		notEnoughPeersBackOffDuration,
	)
}

func newHeaderDownloader(
	logger log.Logger,
	p2pService p2p.Service,
	heimdall heimdall.HeimdallNoStore,
	headersVerifier AccumulatedHeadersVerifier,
	bodiesVerifier BodiesVerifier,
	storage Storage,
	notEnoughPeersBackOffDuration time.Duration,
) HeaderDownloader {
	return &headerDownloader{
		logger:                        logger,
		p2pService:                    p2pService,
		heimdall:                      heimdall,
		headersVerifier:               headersVerifier,
		bodiesVerifier:                bodiesVerifier,
		storage:                       storage,
		notEnoughPeersBackOffDuration: notEnoughPeersBackOffDuration,
	}
}

//
// TODO rename to block downloader? revise and update naming in logs, tests, etc.
//

type headerDownloader struct {
	logger                        log.Logger
	p2pService                    p2p.Service
	heimdall                      heimdall.HeimdallNoStore
	headersVerifier               AccumulatedHeadersVerifier
	bodiesVerifier                BodiesVerifier
	storage                       Storage
	notEnoughPeersBackOffDuration time.Duration
}

func (hd *headerDownloader) DownloadUsingCheckpoints(ctx context.Context, start uint64) (*types.Header, error) {
	waypoints, err := hd.heimdall.FetchCheckpointsFromBlock(ctx, start)
	if err != nil {
		return nil, err
	}

	return hd.downloadUsingWaypoints(ctx, waypoints)
}

func (hd *headerDownloader) DownloadUsingMilestones(ctx context.Context, start uint64) (*types.Header, error) {
	waypoints, err := hd.heimdall.FetchMilestonesFromBlock(ctx, start)
	if err != nil {
		return nil, err
	}

	return hd.downloadUsingWaypoints(ctx, waypoints)
}

func (hd *headerDownloader) downloadUsingWaypoints(ctx context.Context, waypoints heimdall.Waypoints) (*types.Header, error) {
	if len(waypoints) == 0 {
		return nil, nil
	}

	// waypoint rootHash->[blocks part of waypoint]
	waypointBlocksMemo, err := lru.New[common.Hash, []*types.Block](hd.p2pService.MaxPeers())
	if err != nil {
		return nil, err
	}

	var lastBlock *types.Block
	lastBlockNum := waypoints[len(waypoints)-1].EndBlock().Uint64()
	for len(waypoints) > 0 {
		endBlockNum := waypoints[len(waypoints)-1].EndBlock().Uint64()
		peers := hd.p2pService.ListPeersMayHaveBlockNum(endBlockNum)
		if len(peers) == 0 {
			hd.logger.Warn(
				fmt.Sprintf("[%s] can't use any peers to sync, will try again", headerDownloaderLogPrefix),
				"start", waypoints[0].StartBlock(),
				"end", endBlockNum,
				"sleepSeconds", hd.notEnoughPeersBackOffDuration.Seconds(),
			)

			time.Sleep(hd.notEnoughPeersBackOffDuration)
			continue
		}

		peerCount := len(peers)
		waypointsBatch := waypoints
		if len(waypointsBatch) > peerCount {
			waypointsBatch = waypointsBatch[:peerCount]
		}

		hd.logger.Info(
			fmt.Sprintf("[%s] downloading headers", headerDownloaderLogPrefix),
			"waypointsBatchLength", len(waypointsBatch),
			"startBlockNum", waypointsBatch[0].StartBlock(),
			"endBlockNum", waypointsBatch[len(waypointsBatch)-1].EndBlock(),
			"kind", reflect.TypeOf(waypointsBatch[0]),
			"peerCount", peerCount,
		)

		//
		// TODO we may 1) need ETL or 2) limit level of parallelism to fit in RAM reasonably
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

				headers, err := hd.fetchVerifiedHeaders(ctx, waypoint, peerId)
				if err != nil {
					hd.logger.Debug(
						fmt.Sprintf("[%s] issue downloading headers - will try again", headerDownloaderLogPrefix),
						"err", err,
						"start", waypoint.StartBlock(),
						"end", waypoint.EndBlock(),
						"rootHash", waypoint.RootHash(),
						"kind", reflect.TypeOf(waypoint),
						"peerId", peerId,
					)

					return
				}

				bodies, err := hd.fetchVerifiedBodies(ctx, headers, peerId)
				if err != nil {
					hd.logger.Debug(
						fmt.Sprintf("[%s] issue downloading bodies - will try again", headerDownloaderLogPrefix),
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
				hd.logger.Debug(
					fmt.Sprintf("[%s] no blocks, will try again", headerDownloaderLogPrefix),
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

		if err := hd.storage.InsertBlocks(ctx, blocks); err != nil {
			return nil, err
		}

		flushStartTime := time.Now()
		if err := hd.storage.Flush(ctx); err != nil {
			return nil, err
		}

		hd.logger.Debug(
			fmt.Sprintf("[%s] stored blocks", headerDownloaderLogPrefix),
			"len", len(blocks),
			"duration", time.Since(flushStartTime),
		)

		if (endBlockNum == lastBlockNum) && (len(blocks) > 0) {
			lastBlock = blocks[len(blocks)-1]
		}
	}

	return lastBlock.Header(), nil
}

func (hd *headerDownloader) fetchVerifiedHeaders(
	ctx context.Context,
	waypoint heimdall.Waypoint,
	peerId *p2p.PeerId,
) ([]*types.Header, error) {
	start := waypoint.StartBlock().Uint64()
	end := waypoint.EndBlock().Uint64() + 1 // waypoint end is inclusive, fetch headers is [start, end)
	headers, err := hd.p2pService.FetchHeaders(ctx, start, end, peerId)
	if err != nil {
		return nil, err
	}

	if err := hd.headersVerifier(waypoint, headers); err != nil {
		hd.logger.Debug(
			fmt.Sprintf(
				"[%s] bad headers received from peer for waypoint - penalizing",
				headerDownloaderLogPrefix,
			),
			"start", waypoint.StartBlock(),
			"end", waypoint.EndBlock(),
			"rootHash", waypoint.RootHash(),
			"kind", reflect.TypeOf(waypoint),
			"peerId", peerId,
		)

		if penalizeErr := hd.p2pService.Penalize(ctx, peerId); penalizeErr != nil {
			hd.logger.Error(
				fmt.Sprintf("[%s] failed to penalize peer", headerDownloaderLogPrefix),
				"peerId", peerId,
				"err", penalizeErr,
			)

			err = fmt.Errorf("%w: %w", penalizeErr, err)
		}

		return nil, err
	}

	return headers, nil
}

func (hd *headerDownloader) fetchVerifiedBodies(
	ctx context.Context,
	headers []*types.Header,
	peerId *p2p.PeerId,
) ([]*types.Body, error) {
	bodies, err := hd.p2pService.FetchBodies(ctx, headers, peerId)
	if err != nil {
		if errors.Is(err, &p2p.ErrMissingBodies{}) {
			hd.logger.Debug("penalizing peer", "peerId", peerId, "err", err)
			penalizeErr := hd.p2pService.Penalize(ctx, peerId)
			if penalizeErr != nil {
				err = fmt.Errorf("%w: %w", penalizeErr, err)
			}
		}

		return nil, err
	}

	if err := hd.bodiesVerifier(headers, bodies); err != nil {
		hd.logger.Debug("penalizing peer", "peerId", peerId, "err", err)
		penalizeErr := hd.p2pService.Penalize(ctx, peerId)
		if penalizeErr != nil {
			err = fmt.Errorf("%w: %w", penalizeErr, err)
		}

		return nil, err
	}

	return bodies, nil
}
