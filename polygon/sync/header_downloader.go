package sync

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/common"
	erigonlibtypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/core/types"
)

const (
	minPeers                  = 10
	minBlockDecreaseQuotient  = 4
	headerDownloaderLogPrefix = "[HeaderDownloader]"
)

func NewHeaderDownloader(logger log.Logger, sentry Sentry, db DB, heimdall Heimdall, verifier HeaderVerifier) *HeaderDownloader {
	return &HeaderDownloader{
		logger:                logger,
		sentry:                sentry,
		db:                    db,
		heimdall:              heimdall,
		verifier:              verifier,
		statePointHeadersMemo: map[common.Hash][]*types.Header{},
	}
}

type HeaderDownloader struct {
	logger                log.Logger
	sentry                Sentry
	db                    DB
	heimdall              Heimdall
	verifier              HeaderVerifier
	statePointHeadersMemo map[common.Hash][]*types.Header // statePoint.rootHash->[headers part of state point]
}

func (bd *HeaderDownloader) DownloadUsingCheckpoints(ctx context.Context, start uint64) error {
	checkpoints, err := bd.heimdall.FetchCheckpoints(ctx, start)
	if err != nil {
		return err
	}

	err = bd.downloadUsingStatePoints(ctx, statePointsFromCheckpoints(checkpoints))
	if err != nil {
		return err
	}

	return nil
}

func (bd *HeaderDownloader) DownloadUsingMilestones(ctx context.Context, start uint64) error {
	milestones, err := bd.heimdall.FetchMilestones(ctx, start)
	if err != nil {
		return err
	}

	err = bd.downloadUsingStatePoints(ctx, statePointsFromMilestones(milestones))
	if err != nil {
		return err
	}

	return nil
}

func (bd *HeaderDownloader) downloadUsingStatePoints(ctx context.Context, statePoints statePoints) error {
	for len(statePoints) > 0 {
		lastStatePoint := statePoints[len(statePoints)-1]
		peers, err := bd.findEnoughPeersWithMinBlock(lastStatePoint.endBlock)
		if err != nil {
			return err
		}

		peerCount := len(peers)
		statePointsBatch := statePoints[:peerCount]
		bd.logger.Info(
			fmt.Sprintf("[%s] downloading headers", headerDownloaderLogPrefix),
			"start", statePointsBatch[0].startBlock,
			"end", statePointsBatch[len(statePointsBatch)-1].endBlock,
			"kind", statePointsBatch[0].kind,
			"peerCount", peerCount,
		)

		headerBatches := make([][]*types.Header, len(statePointsBatch))
		maxStatePointLength := float64(0)
		g, ctx := errgroup.WithContext(ctx)
		for i, statePoint := range statePointsBatch {
			maxStatePointLength = math.Max(float64(statePoint.length()), maxStatePointLength)
			// local copy for inputs since used in async closure
			i, statePoint, peerID := i, statePoint, peers[i].Id
			g.Go(func() error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				if headers, ok := bd.statePointHeadersMemo[statePoint.rootHash]; ok {
					headerBatches[i] = headers
					return nil
				}

				headers, err := bd.sentry.DownloadHeaders(ctx, statePoint.startBlock, statePoint.endBlock, peerID)
				if err != nil {
					return err
				}

				if err := bd.verifier.Verify(statePoint, headers); err != nil {
					bd.logger.Debug(
						fmt.Sprintf(
							"[%s] bad headers received from peer for state point - penalizing",
							headerDownloaderLogPrefix,
						),
						"start", statePoint.startBlock,
						"end", statePoint.endBlock,
						"rootHash", statePoint.rootHash,
						"kind", statePoint.kind,
						"peerID", peerID,
					)

					bd.sentry.Penalize(peerID)
					// no error since we handle gaps outside of this goroutine at a later point
					// and cache downloaded but unsaved headers
					return nil
				}

				bd.statePointHeadersMemo[statePoint.rootHash] = headers
				headerBatches[i] = headers
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return err
		}

		headers := make([]*types.Header, 0, int(maxStatePointLength)*peerCount)
		gapIndex := -1
		for i, headerBatch := range headerBatches {
			if len(headerBatch) == 0 {
				bd.logger.Debug(
					fmt.Sprintf("[%s] gap detected, auto recovery will try a different peer", headerDownloaderLogPrefix),
					"start", statePointsBatch[i].startBlock,
					"end", statePointsBatch[i].endBlock,
					"rootHash", statePointsBatch[i].rootHash,
					"kind", statePointsBatch[i].kind,
				)

				gapIndex = i
				break
			}

			headers = append(headers, headerBatch...)
			delete(bd.statePointHeadersMemo, statePointsBatch[i].rootHash)
		}

		if gapIndex >= 0 {
			statePoints = statePoints[:gapIndex]
		} else {
			statePoints = statePoints[:len(statePointsBatch)]
		}

		dbWriteStartTime := time.Now()
		if err := bd.db.WriteHeaders(headers); err != nil {
			return err
		}

		bd.logger.Debug(
			fmt.Sprintf("[%s] wrote headers to db", headerDownloaderLogPrefix),
			"numHeaders", len(headers),
			"time", time.Since(dbWriteStartTime),
		)
	}

	return nil
}

func (bd *HeaderDownloader) findEnoughPeersWithMinBlock(num uint64) ([]*erigonlibtypes.PeerInfo, error) {
	peers := bd.sentry.PeersWithMinBlock(num)
	if len(peers) >= minPeers {
		return peers, nil
	}

	if num < minBlockDecreaseQuotient {
		return nil, fmt.Errorf(
			"could not find enough peers with min block: minPeers=%d, blockNum=%d, decreaseQuotient=%d",
			minPeers,
			num,
			minBlockDecreaseQuotient,
		)
	}

	num = num - num/minBlockDecreaseQuotient
	return bd.findEnoughPeersWithMinBlock(num)
}
