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

//go:generate mockgen -destination=./headers_writer_mock.go -package=sync . HeadersWriter
type HeadersWriter interface {
	PutHeaders(ctx context.Context, headers []*types.Header) error
}

type HeaderDownloader interface {
	DownloadUsingCheckpoints(ctx context.Context, start uint64) (*types.Header, error)
	DownloadUsingMilestones(ctx context.Context, start uint64) (*types.Header, error)
}

func NewHeaderDownloader(
	logger log.Logger,
	p2pService p2p.Service,
	heimdall heimdall.HeimdallNoStore,
	headersVerifier AccumulatedHeadersVerifier,
	headersWriter HeadersWriter,
) HeaderDownloader {
	return newHeaderDownloader(
		logger,
		p2pService,
		heimdall,
		headersVerifier,
		headersWriter,
		notEnoughPeersBackOffDuration,
	)
}

func newHeaderDownloader(
	logger log.Logger,
	p2pService p2p.Service,
	heimdall heimdall.HeimdallNoStore,
	headersVerifier AccumulatedHeadersVerifier,
	headersWriter HeadersWriter,
	notEnoughPeersBackOffDuration time.Duration,
) HeaderDownloader {
	return &headerDownloader{
		logger:                        logger,
		p2pService:                    p2pService,
		heimdall:                      heimdall,
		headersVerifier:               headersVerifier,
		headersWriter:                 headersWriter,
		notEnoughPeersBackOffDuration: notEnoughPeersBackOffDuration,
	}
}

type headerDownloader struct {
	logger                        log.Logger
	p2pService                    p2p.Service
	heimdall                      heimdall.HeimdallNoStore
	headersVerifier               AccumulatedHeadersVerifier
	headersWriter                 HeadersWriter
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

	// waypoint rootHash->[headers part of waypoint]
	waypointHeadersMemo, err := lru.New[common.Hash, []*types.Header](hd.p2pService.MaxPeers())
	if err != nil {
		return nil, err
	}

	lastBlockNum := waypoints[len(waypoints)-1].EndBlock().Uint64()
	var lastHeader *types.Header

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

		headerBatches := make([][]*types.Header, len(waypointsBatch))
		maxWaypointLength := float64(0)
		wg := sync.WaitGroup{}
		for i, waypoint := range waypointsBatch {
			maxWaypointLength = math.Max(float64(waypoint.Length()), maxWaypointLength)
			wg.Add(1)
			go func(i int, waypoint heimdall.Waypoint, peerId *p2p.PeerId) {
				defer wg.Done()

				if headers, ok := waypointHeadersMemo.Get(waypoint.RootHash()); ok {
					headerBatches[i] = headers
					return
				}

				start := waypoint.StartBlock().Uint64()
				end := waypoint.EndBlock().Uint64() + 1 // waypoint end is inclusive, fetch headers is [start, end)
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

				if err := hd.headersVerifier(waypoint, headers); err != nil {
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
		if err := hd.headersWriter.PutHeaders(ctx, headers); err != nil {
			return nil, err
		}

		hd.logger.Debug(
			fmt.Sprintf("[%s] wrote headers to db", headerDownloaderLogPrefix),
			"numHeaders", len(headers),
			"time", time.Since(dbWriteStartTime),
		)

		if (endBlockNum == lastBlockNum) && (len(headers) > 0) {
			lastHeader = headers[len(headers)-1]
		}
	}

	return lastHeader, nil
}
