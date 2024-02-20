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
	"github.com/ledgerwatch/erigon/polygon/p2p"
)

const headerDownloaderLogPrefix = "HeaderDownloader"

//go:generate mockgen -destination=./headers_writer_mock.go -package=sync . HeadersWriter
type HeadersWriter interface {
	PutHeaders(headers []*types.Header) error
}

func NewHeaderDownloader(
	logger log.Logger,
	p2pService p2p.Service,
	heimdall heimdall.Heimdall,
	checkpoints heimdall.CheckpointStore,
	milestones heimdall.MilestoneStore,
	verify AccumulatedHeadersVerifier,
	headersWriter HeadersWriter,
) *HeaderDownloader {
	return &HeaderDownloader{
		logger:     logger,
		p2pService: p2pService,

		heimdall:    heimdall,
		checkpoints: checkpoints,
		milestones:  milestones,

		verify: verify,

		headersWriter: headersWriter,
	}
}

type HeaderDownloader struct {
	logger     log.Logger
	p2pService p2p.Service

	heimdall    heimdall.Heimdall
	checkpoints heimdall.CheckpointStore
	milestones  heimdall.MilestoneStore

	verify AccumulatedHeadersVerifier

	headersWriter HeadersWriter
}

func (hd *HeaderDownloader) DownloadUsingCheckpoints(ctx context.Context, start uint64) error {
	waypoints, err := hd.heimdall.FetchCheckpointsFromBlock(ctx, hd.checkpoints, start)
	if err != nil {
		return err
	}

	err = hd.downloadUsingWaypoints(ctx, waypoints)
	if err != nil {
		return err
	}

	return nil
}

func (hd *HeaderDownloader) DownloadUsingMilestones(ctx context.Context, start uint64) error {
	waypoints, err := hd.heimdall.FetchMilestonesFromBlock(ctx, hd.milestones, start)
	if err != nil {
		return err
	}

	err = hd.downloadUsingWaypoints(ctx, waypoints)
	if err != nil {
		return err
	}

	return nil
}

func (hd *HeaderDownloader) downloadUsingWaypoints(ctx context.Context, waypoints heimdall.Waypoints) error {
	// waypoint rootHash->[headers part of waypoint]
	waypointHeadersMemo, err := lru.New[common.Hash, []*types.Header](hd.p2pService.MaxPeers())
	if err != nil {
		return err
	}

	for len(waypoints) > 0 {
		allPeers := hd.p2pService.PeersSyncProgress()
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
				"lowestMaxSeenBlockNum", allPeers[0].MaxSeenBlockNum,
				"lowestPeerId", allPeers[0].Id,
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
			go func(i int, waypoint heimdall.Waypoint, peerId p2p.PeerId) {
				defer wg.Done()

				if headers, ok := waypointHeadersMemo.Get(waypoint.RootHash()); ok {
					headerBatches[i] = headers
					return
				}

				start, end := waypoint.StartBlock().Uint64(), waypoint.EndBlock().Uint64()
				headers, err := hd.p2pService.FetchHeaders(ctx, start, end, peerId)
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
			}(i, waypoint, peers[i].Id)
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
		if err := hd.headersWriter.PutHeaders(headers); err != nil {
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
func (hd *HeaderDownloader) choosePeers(peers p2p.PeersSyncProgress, waypoints heimdall.Waypoints) p2p.PeersSyncProgress {
	var peersIdx int
	chosenPeers := make(p2p.PeersSyncProgress, 0, len(peers))
	for _, waypoint := range waypoints {
		if peersIdx >= len(peers) {
			break
		}

		peer := peers[peersIdx]
		if peer.MaxSeenBlockNum >= waypoint.EndBlock().Uint64() {
			chosenPeers = append(chosenPeers, peer)
		}

		peersIdx++
	}

	return chosenPeers
}
