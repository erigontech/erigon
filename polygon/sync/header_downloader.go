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
	statePointHeadersMemo, err := lru.New[common.Hash, []*types.Header](sentry.MaxPeers())
	if err != nil {
		panic(err)
	}

	return &HeaderDownloader{
		logger:                logger,
		sentry:                sentry,
		heimdall:              heimdall,
		verify:                verify,
		statePointHeadersMemo: statePointHeadersMemo,
	}
}

type HeaderDownloader struct {
	logger                log.Logger
	sentry                Sentry
	heimdall              heimdall.Heimdall
	verify                AccumulatedHeadersVerifier
	statePointHeadersMemo *lru.Cache[common.Hash, []*types.Header] // statePoint.rootHash->[headers part of state point]
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

func (hd *HeaderDownloader) downloadUsingWaypoints(ctx context.Context, store HeaderStore, hashAccumulators heimdall.Waypoints) error {
	for len(hashAccumulators) > 0 {
		allPeers := hd.sentry.PeersWithBlockNumInfo()
		if len(allPeers) == 0 {
			hd.logger.Warn(fmt.Sprintf("[%s] zero peers, will try again", headerDownloaderLogPrefix))
			continue
		}

		sort.Sort(allPeers) // sort by block num in asc order
		peers := hd.choosePeers(allPeers, hashAccumulators)
		if len(peers) == 0 {
			hd.logger.Warn(
				fmt.Sprintf("[%s] can't use any peers to sync, will try again", headerDownloaderLogPrefix),
				"start", hashAccumulators[0].StartBlock(),
				"end", hashAccumulators[len(hashAccumulators)-1].EndBlock(),
				"minPeerBlockNum", allPeers[0].BlockNum,
				"minPeerID", allPeers[0].ID,
			)
			continue
		}

		peerCount := len(peers)
		statePointsBatch := hashAccumulators[:peerCount]
		hd.logger.Info(
			fmt.Sprintf("[%s] downloading headers", headerDownloaderLogPrefix),
			"start", statePointsBatch[0].StartBlock(),
			"end", statePointsBatch[len(statePointsBatch)-1].EndBlock(),
			"kind", reflect.TypeOf(statePointsBatch[0]),
			"peerCount", peerCount,
		)

		headerBatches := make([][]*types.Header, len(statePointsBatch))
		maxStatePointLength := float64(0)
		wg := sync.WaitGroup{}
		for i, point := range statePointsBatch {
			maxStatePointLength = math.Max(float64(point.Length()), maxStatePointLength)
			wg.Add(1)
			go func(i int, statePoint heimdall.Waypoint, peerID string) {
				defer wg.Done()

				if headers, ok := hd.statePointHeadersMemo.Get(statePoint.RootHash()); ok {
					headerBatches[i] = headers
					return
				}

				headers, err := hd.sentry.DownloadHeaders(ctx, statePoint.StartBlock(), statePoint.EndBlock(), peerID)
				if err != nil {
					hd.logger.Debug(
						fmt.Sprintf("[%s] issue downloading headers, will try again", headerDownloaderLogPrefix),
						"err", err,
						"start", statePoint.StartBlock(),
						"end", statePoint.EndBlock(),
						"rootHash", statePoint.RootHash(),
						"kind", reflect.TypeOf(statePoint),
						"peerID", peerID,
					)
					return
				}

				if err := hd.verify(statePoint, headers); err != nil {
					hd.logger.Debug(
						fmt.Sprintf(
							"[%s] bad headers received from peer for state point - penalizing and will try again",
							headerDownloaderLogPrefix,
						),
						"start", statePoint.StartBlock(),
						"end", statePoint.EndBlock(),
						"rootHash", statePoint.RootHash(),
						"kind", reflect.TypeOf(statePoint),
						"peerID", peerID,
					)

					hd.sentry.Penalize(peerID)
					return
				}

				hd.statePointHeadersMemo.Add(statePoint.RootHash(), headers)
				headerBatches[i] = headers
			}(i, point, peers[i].ID)
		}

		wg.Wait()
		headers := make([]*types.Header, 0, int(maxStatePointLength)*peerCount)
		gapIndex := -1
		for i, headerBatch := range headerBatches {
			if len(headerBatch) == 0 {
				hd.logger.Debug(
					fmt.Sprintf("[%s] no headers, will try again", headerDownloaderLogPrefix),
					"start", statePointsBatch[i].StartBlock(),
					"end", statePointsBatch[i].EndBlock(),
					"rootHash", statePointsBatch[i].RootHash(),
					"kind", reflect.TypeOf(statePointsBatch[i]),
				)

				gapIndex = i
				break
			}

			headers = append(headers, headerBatch...)
		}

		if gapIndex >= 0 {
			hashAccumulators = hashAccumulators[gapIndex:]
		} else {
			hashAccumulators = hashAccumulators[len(statePointsBatch):]
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
func (hd *HeaderDownloader) choosePeers(peers PeersWithBlockNumInfo, hashAccumulators heimdall.Waypoints) PeersWithBlockNumInfo {
	var peersIdx int
	chosenPeers := make(PeersWithBlockNumInfo, 0, len(peers))
	for _, statePoint := range hashAccumulators {
		if peersIdx >= len(peers) {
			break
		}

		peer := peers[peersIdx]
		if peer.BlockNum.Cmp(statePoint.EndBlock()) > -1 {
			chosenPeers = append(chosenPeers, peer)
		}

		peersIdx++
	}

	return chosenPeers
}
