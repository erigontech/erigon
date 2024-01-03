package sync

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/sync/peerinfo"
)

const headerDownloaderLogPrefix = "HeaderDownloader"

func NewHeaderDownloader(logger log.Logger, sentry Sentry, db DB, heimdall Heimdall, verify HeaderVerifier) *HeaderDownloader {
	statePointHeadersMemo, err := lru.New[common.Hash, []*types.Header](sentry.MaxPeers())
	if err != nil {
		panic(err)
	}

	return &HeaderDownloader{
		logger:                logger,
		sentry:                sentry,
		db:                    db,
		heimdall:              heimdall,
		verify:                verify,
		statePointHeadersMemo: statePointHeadersMemo,
	}
}

type HeaderDownloader struct {
	logger                log.Logger
	sentry                Sentry
	db                    DB
	heimdall              Heimdall
	verify                HeaderVerifier
	statePointHeadersMemo *lru.Cache[common.Hash, []*types.Header] // statePoint.rootHash->[headers part of state point]
}

func (hd *HeaderDownloader) DownloadUsingCheckpoints(ctx context.Context, start uint64) error {
	checkpoints, err := hd.heimdall.FetchCheckpoints(ctx, start)
	if err != nil {
		return err
	}

	err = hd.downloadUsingStatePoints(ctx, statePointsFromCheckpoints(checkpoints))
	if err != nil {
		return err
	}

	return nil
}

func (hd *HeaderDownloader) DownloadUsingMilestones(ctx context.Context, start uint64) error {
	milestones, err := hd.heimdall.FetchMilestones(ctx, start)
	if err != nil {
		return err
	}

	err = hd.downloadUsingStatePoints(ctx, statePointsFromMilestones(milestones))
	if err != nil {
		return err
	}

	return nil
}

func (hd *HeaderDownloader) downloadUsingStatePoints(ctx context.Context, statePoints statePoints) error {
	for len(statePoints) > 0 {
		allPeers := hd.sentry.PeersWithBlockNumInfo()
		if len(allPeers) == 0 {
			hd.logger.Warn(fmt.Sprintf("[%s] zero peers, will try again", headerDownloaderLogPrefix))
			continue
		}

		sort.Sort(allPeers) // sort by block num in asc order
		peers := hd.choosePeers(allPeers, statePoints)
		if len(peers) == 0 {
			hd.logger.Warn(
				fmt.Sprintf("[%s] can't use any peers to sync, will try again", headerDownloaderLogPrefix),
				"start", statePoints[0].startBlock,
				"end", statePoints[len(statePoints)-1].endBlock,
				"minPeerBlockNum", allPeers[0].BlockNum,
				"minPeerID", allPeers[0].ID,
			)
			continue
		}

		peerCount := len(peers)
		statePointsBatch := statePoints[:peerCount]
		hd.logger.Info(
			fmt.Sprintf("[%s] downloading headers", headerDownloaderLogPrefix),
			"start", statePointsBatch[0].startBlock,
			"end", statePointsBatch[len(statePointsBatch)-1].endBlock,
			"kind", statePointsBatch[0].kind,
			"peerCount", peerCount,
		)

		headerBatches := make([][]*types.Header, len(statePointsBatch))
		maxStatePointLength := float64(0)
		wg := sync.WaitGroup{}
		for i, point := range statePointsBatch {
			maxStatePointLength = math.Max(float64(point.length()), maxStatePointLength)
			wg.Add(1)
			go func(i int, statePoint *statePoint, peerID string) {
				defer wg.Done()

				if headers, ok := hd.statePointHeadersMemo.Get(statePoint.rootHash); ok {
					headerBatches[i] = headers
					return
				}

				headers, err := hd.sentry.DownloadHeaders(ctx, statePoint.startBlock, statePoint.endBlock, peerID)
				if err != nil {
					hd.logger.Debug(
						fmt.Sprintf("[%s] issue downloading headers, will try again", headerDownloaderLogPrefix),
						"err", err,
						"start", statePoint.startBlock,
						"end", statePoint.endBlock,
						"rootHash", statePoint.rootHash,
						"kind", statePoint.kind,
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
						"start", statePoint.startBlock,
						"end", statePoint.endBlock,
						"rootHash", statePoint.rootHash,
						"kind", statePoint.kind,
						"peerID", peerID,
					)

					hd.sentry.Penalize(peerID)
					return
				}

				hd.statePointHeadersMemo.Add(statePoint.rootHash, headers)
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
					"start", statePointsBatch[i].startBlock,
					"end", statePointsBatch[i].endBlock,
					"rootHash", statePointsBatch[i].rootHash,
					"kind", statePointsBatch[i].kind,
				)

				gapIndex = i
				break
			}

			headers = append(headers, headerBatch...)
		}

		if gapIndex >= 0 {
			statePoints = statePoints[gapIndex:]
		} else {
			statePoints = statePoints[len(statePointsBatch):]
		}

		dbWriteStartTime := time.Now()
		if err := hd.db.WriteHeaders(headers); err != nil {
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
func (hd *HeaderDownloader) choosePeers(peers peerinfo.PeersWithBlockNumInfo, statePoints statePoints) peerinfo.PeersWithBlockNumInfo {
	var peersIdx int
	chosenPeers := make(peerinfo.PeersWithBlockNumInfo, 0, len(peers))
	for _, statePoint := range statePoints {
		if peersIdx >= len(peers) {
			break
		}

		peer := peers[peersIdx]
		if peer.BlockNum.Cmp(statePoint.endBlock) > -1 {
			chosenPeers = append(chosenPeers, peer)
		}

		peersIdx++
	}

	return chosenPeers
}
