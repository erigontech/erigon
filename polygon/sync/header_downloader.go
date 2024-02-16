package sync

import (
	"context"
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

func NewHeaderDownloader(
	logger log.Logger,
	p2pService p2p.Service,
	heimdall heimdall.Heimdall,
	verify AccumulatedHeadersVerifier,
) *HeaderDownloader {
	return newHeaderDownloader(logger, p2pService, heimdall, verify, notEnoughPeersBackOffDuration)
}

func newHeaderDownloader(
	logger log.Logger,
	p2pService p2p.Service,
	heimdall heimdall.Heimdall,
	verify AccumulatedHeadersVerifier,
	notEnoughPeersBackOffDuration time.Duration,
) *HeaderDownloader {
	return &HeaderDownloader{
		logger:                        logger,
		p2pService:                    p2pService,
		heimdall:                      heimdall,
		verify:                        verify,
		notEnoughPeersBackOffDuration: notEnoughPeersBackOffDuration,
	}
}

type HeaderDownloader struct {
	logger                        log.Logger
	p2pService                    p2p.Service
	heimdall                      heimdall.Heimdall
	verify                        AccumulatedHeadersVerifier
	notEnoughPeersBackOffDuration time.Duration
}

func (hd *HeaderDownloader) DownloadUsingCheckpoints(ctx context.Context, store CheckpointStore, start uint64) error {
	checkpoints, err := hd.heimdall.FetchCheckpointsFromBlock(ctx, store, start)
	if err != nil {
		return err
	}

	err = hd.downloadUsingWaypoints(ctx, store, checkpoints)
	if err != nil {
		return err
	}

	return nil
}

func (hd *HeaderDownloader) DownloadUsingMilestones(ctx context.Context, store MilestoneStore, start uint64) error {
	milestones, err := hd.heimdall.FetchMilestonesFromBlock(ctx, store, start)
	if err != nil {
		return err
	}

	err = hd.downloadUsingWaypoints(ctx, store, milestones)
	if err != nil {
		return err
	}

	return nil
}

func (hd *HeaderDownloader) downloadUsingWaypoints(ctx context.Context, store HeaderStore, waypoints heimdall.Waypoints) error {
	// waypoint rootHash->[headers part of waypoint]
	waypointHeadersMemo, err := lru.New[common.Hash, []*types.Header](hd.p2pService.MaxPeers())
	if err != nil {
		return err
	}

	for len(waypoints) > 0 {
		endBlockNum := waypoints[len(waypoints)-1].EndBlock()
		peers := hd.p2pService.ListPeersMayHaveBlockNum(endBlockNum.Uint64())
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

		headerBatches := make([][]*types.Header, len(waypointsBatch))
		maxWaypointLength := float64(0)
		wg := sync.WaitGroup{}
		for i, waypoint := range waypointsBatch {
			maxWaypointLength = math.Max(float64(waypoint.Length()), maxWaypointLength)
			wg.Add(1)
			go func(i int, waypoint heimdall.Waypoint, peerId p2p.PeerId) {
				defer wg.Done()

				if headers, ok := waypointHeadersMemo.Get(waypoint.RootHash()); ok {
					headerBatches[i] = headers
					return
				}

				start, end := waypoint.StartBlock().Uint64(), waypoint.EndBlock().Uint64()
				headers, err := hd.p2pService.DownloadHeaders(ctx, start, end, peerId)
				if err != nil {
					hd.logger.Debug(
						fmt.Sprintf("[%s] issue downloading headers, will try again", headerDownloaderLogPrefix),
						"err", err,
						"start", waypoint.StartBlock(),
						"end", waypoint.EndBlock(),
						"rootHash", waypoint.RootHash(),
						"kind", reflect.TypeOf(waypoint),
						"peerId", peerId,
					)
					return
				}

				if err := hd.verify(waypoint, headers); err != nil {
					hd.logger.Debug(
						fmt.Sprintf(
							"[%s] bad headers received from peer for waypoint - penalizing and will try again",
							headerDownloaderLogPrefix,
						),
						"start", waypoint.StartBlock(),
						"end", waypoint.EndBlock(),
						"rootHash", waypoint.RootHash(),
						"kind", reflect.TypeOf(waypoint),
						"peerId", peerId,
					)

					if err := hd.p2pService.Penalize(ctx, peerId); err != nil {
						hd.logger.Error(
							fmt.Sprintf("[%s] failed to penalize peer", headerDownloaderLogPrefix),
							"peerId", peerId,
							"err", err,
						)
					}

					return
				}

				waypointHeadersMemo.Add(waypoint.RootHash(), headers)
				headerBatches[i] = headers
			}(i, waypoint, peers[i])
		}

		wg.Wait()
		headers := make([]*types.Header, 0, int(maxWaypointLength)*peerCount)
		gapIndex := -1
		for i, headerBatch := range headerBatches {
			if len(headerBatch) == 0 {
				hd.logger.Debug(
					fmt.Sprintf("[%s] no headers, will try again", headerDownloaderLogPrefix),
					"start", waypointsBatch[i].StartBlock(),
					"end", waypointsBatch[i].EndBlock(),
					"rootHash", waypointsBatch[i].RootHash(),
					"kind", reflect.TypeOf(waypointsBatch[i]),
				)

				gapIndex = i
				break
			}

			headers = append(headers, headerBatch...)
		}

		if gapIndex >= 0 {
			waypoints = waypoints[gapIndex:]
		} else {
			waypoints = waypoints[len(waypointsBatch):]
		}

		dbWriteStartTime := time.Now()
		if err := store.PutHeaders(headers); err != nil {
			return err
		}

		hd.logger.Debug(
			fmt.Sprintf("[%s] wrote headers to db", headerDownloaderLogPrefix),
			"numHeaders", len(headers),
			"time", time.Since(dbWriteStartTime),
		)
	}

	return nil
}
