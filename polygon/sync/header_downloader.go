package sync

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
)

const headerDownloaderLogPrefix = "HeaderDownloader"

func NewHeaderDownloader(logger log.Logger, sentry Sentry, heimdall heimdall.Heimdall, verify AccumulatedHeadersVerifier) *HeaderDownloader {
	return &HeaderDownloader{
		logger:   logger,
		sentry:   sentry,
		heimdall: heimdall,
		verify:   verify,
	}
}

type HeaderDownloader struct {
	logger   log.Logger
	sentry   Sentry
	heimdall heimdall.Heimdall
	verify   AccumulatedHeadersVerifier
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
	waypointHeadersMemo, err := lru.New[common.Hash, []*types.Header](hd.sentry.MaxPeers())
	if err != nil {
		return err
	}

	for len(waypoints) > 0 {
		allPeers := hd.sentry.PeersWithBlockNumInfo()
		if len(allPeers) == 0 {
			hd.logger.Warn(fmt.Sprintf("[%s] zero peers, will try again", headerDownloaderLogPrefix))
			continue
		}

		sort.Sort(allPeers) // sort by block num in asc order
		peers := hd.choosePeers(allPeers, waypoints)
		if len(peers) == 0 {
			hd.logger.Warn(
				fmt.Sprintf("[%s] can't use any peers to sync, will try again", headerDownloaderLogPrefix),
				"start", waypoints[0].StartBlock(),
				"end", waypoints[len(waypoints)-1].EndBlock(),
				"minPeerBlockNum", allPeers[0].BlockNum,
				"minPeerID", allPeers[0].ID,
			)
			continue
		}

		peerCount := len(peers)
		waypointsBatch := waypoints[:peerCount]
		hd.logger.Info(
			fmt.Sprintf("[%s] downloading headers", headerDownloaderLogPrefix),
			"start", waypointsBatch[0].StartBlock(),
			"end", waypointsBatch[len(waypointsBatch)-1].EndBlock(),
			"kind", reflect.TypeOf(waypointsBatch[0]),
			"peerCount", peerCount,
		)

		headerBatches := make([][]*types.Header, len(waypointsBatch))
		maxWaypointLength := float64(0)
		wg := sync.WaitGroup{}
		for i, waypoint := range waypointsBatch {
			maxWaypointLength = math.Max(float64(waypoint.Length()), maxWaypointLength)
			wg.Add(1)
			go func(i int, waypoint heimdall.Waypoint, peerID string) {
				defer wg.Done()

				if headers, ok := waypointHeadersMemo.Get(waypoint.RootHash()); ok {
					headerBatches[i] = headers
					return
				}

				headers, err := hd.sentry.DownloadHeaders(ctx, waypoint.StartBlock(), waypoint.EndBlock(), peerID)
				if err != nil {
					hd.logger.Debug(
						fmt.Sprintf("[%s] issue downloading headers, will try again", headerDownloaderLogPrefix),
						"err", err,
						"start", waypoint.StartBlock(),
						"end", waypoint.EndBlock(),
						"rootHash", waypoint.RootHash(),
						"kind", reflect.TypeOf(waypoint),
						"peerID", peerID,
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
						"peerID", peerID,
					)

					hd.sentry.Penalize(peerID)
					return
				}

				waypointHeadersMemo.Add(waypoint.RootHash(), headers)
				headerBatches[i] = headers
			}(i, waypoint, peers[i].ID)
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

// choosePeers assumes peers are sorted in ascending order based on block num
func (hd *HeaderDownloader) choosePeers(peers PeersWithBlockNumInfo, waypoints heimdall.Waypoints) PeersWithBlockNumInfo {
	var peersIdx int
	chosenPeers := make(PeersWithBlockNumInfo, 0, len(peers))
	for _, waypoint := range waypoints {
		if peersIdx >= len(peers) {
			break
		}

		peer := peers[peersIdx]
		if peer.BlockNum.Cmp(waypoint.EndBlock()) > -1 {
			chosenPeers = append(chosenPeers, peer)
		}

		peersIdx++
	}

	return chosenPeers
}
