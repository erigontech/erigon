// Copyright 2025 The Erigon Authors
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

package bbd

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
	"github.com/erigontech/erigon/polygon/p2p"
)

var ErrChainLengthExceedsLimit = errors.New("chain length exceeds limit")

type BackwardBlockDownloader struct {
	logger          log.Logger
	fetcher         p2p.Fetcher
	peerTracker     *p2p.PeerTracker
	peerPenalizer   *p2p.PeerPenalizer
	messageListener *p2p.MessageListener
	headerReader    HeaderReader
	tmpDir          string
	stopped         atomic.Bool
}

func NewBackwardBlockDownloader(
	logger log.Logger,
	sentryClient sentryproto.SentryClient,
	statusDataFactory libsentry.StatusDataFactory,
	headerReader HeaderReader,
	tmpDir string,
) *BackwardBlockDownloader {
	peerPenalizer := p2p.NewPeerPenalizer(sentryClient)
	messageListener := p2p.NewMessageListener(logger, sentryClient, statusDataFactory, peerPenalizer)
	messageSender := p2p.NewMessageSender(sentryClient)
	peerTracker := p2p.NewPeerTracker(logger, sentryClient, messageListener)
	var fetcher p2p.Fetcher
	fetcher = p2p.NewFetcher(logger, messageListener, messageSender)
	fetcher = p2p.NewPenalizingFetcher(logger, fetcher, peerPenalizer)
	fetcher = p2p.NewTrackingFetcher(fetcher, peerTracker)
	return &BackwardBlockDownloader{
		logger:          logger,
		fetcher:         fetcher,
		peerTracker:     peerTracker,
		peerPenalizer:   peerPenalizer,
		headerReader:    headerReader,
		tmpDir:          tmpDir,
		messageListener: messageListener,
	}
}

func (bbd *BackwardBlockDownloader) Run(ctx context.Context) error {
	bbd.logger.Debug("[backward-block-downloader] running")
	defer func() {
		bbd.logger.Debug("[backward-block-downloader] stopped")
		bbd.stopped.Store(true)
	}()
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		err := bbd.peerTracker.Run(ctx)
		if err != nil {
			return fmt.Errorf("backward block downloader peer tracker failed: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		err := bbd.messageListener.Run(ctx)
		if err != nil {
			return fmt.Errorf("backward block downloader message listener failed: %w", err)
		}
		return nil
	})
	return eg.Wait()
}

// DownloadBlocksBackwards downloads blocks backwards given a starting block hash. It uses the underlying header reader
// to figure out when a header chain connects with a header that we already have. The backward download can handle
// chain lengths of unlimited size by using an etl for temporarily storing the headers. This is also enabled by a
// paging-like ResultFeed, which can be used to return pages of blocks as they get fetched in batches.
//
// There are a number of Option-s that can be passed in to customise the behaviour of the request:
//   - WithPeerId - in case the backward needs to happen from a specific peer only
//     (default: distributes requests across all that have the initial block hash)
//   - WithBlocksBatchSize - controls the size of the block batch that we fetch from all and send to the result feed
//     (default: 500 blocks)
//   - WithChainLengthLimit - terminate the download if the backward header chain goes beyond a certain length.
//     (default: unlimited)
//   - WithChainLengthCurrentHead - optional, can be used in conjunction with WithChainLengthLimit to enable a quick
//     validation of chain length limit breach. With this we can terminate early after fetching the initial header from
//     peers if the fetched header is too far ahead than the current head. This will prevent further batched backward
//     fetches of headers until such a chain length limit is breached.
func (bbd *BackwardBlockDownloader) DownloadBlocksBackwards(ctx context.Context, hash common.Hash, opts ...Option) (ResultFeed, error) {
	if bbd.stopped.Load() {
		return ResultFeed{}, errors.New("backward block downloader is stopped")
	}
	feed := ResultFeed{ch: make(chan BatchResult)}
	go func() {
		defer feed.close()
		err := bbd.fetchBlocksBackwardsByHash(ctx, hash, feed, opts...)
		if err != nil {
			feed.consumeErr(ctx, err)
		}
	}()
	return feed, nil
}

func (bbd *BackwardBlockDownloader) fetchBlocksBackwardsByHash(ctx context.Context, hash common.Hash, feed ResultFeed, opts ...Option) error {
	bbd.logger.Debug("[backward-block-downloader] fetching blocks backwards by hash", "hash", hash)
	// 1. Get all peers
	config := applyOptions(opts...)
	peers, err := bbd.loadPeers(config)
	if err != nil {
		return err
	}

	// 2. Check which peers have the header to build knowledge about which peers we can use for syncing
	//    and to also terminate early if none of the peers have seen it
	bbd.logger.Info("[backward-block-downloader] downloading initial header from all peers", "hash", hash)
	initialHeader, err := bbd.downloadInitialHeader(ctx, hash, peers, config)
	if err != nil {
		return err
	}

	// 3. Download the header chain backwards in an etl collector until we connect it to a known local header.
	//    Note we fetch headers in batches of min(config.blocksBatchSize,1024) from 1 peer and send every following
	//    request to the next available peer (in rotating fashion) to distribute the requests across all peers.
	bbd.logger.Info(
		"[backward-block-downloader] downloading header chain backward from initial header",
		"num", initialHeader.Number.Uint64(),
		"hash", initialHeader.Hash(),
	)
	etlSortableBuf := etl.NewSortableBuffer(etl.BufferOptimalSize)
	headerCollector := etl.NewCollector("backward-block-downloader", bbd.tmpDir, etlSortableBuf, bbd.logger)
	defer headerCollector.Close()
	connectionPoint, err := bbd.downloadHeaderChainBackwards(ctx, initialHeader, headerCollector, peers, config)
	if err != nil {
		return err
	}

	// 4. Start forward downloading the bodies in batches of config.blocksBatchSize from the next in-turn peer.
	//    Upon every complete batch we construct the blocks and feed them to the result feed.
	bbd.logger.Info(
		"[backward-block-downloader] starting forward downloading of blocks",
		"count", (initialHeader.Number.Uint64()-connectionPoint.Number.Uint64())+1,
		"fromNum", connectionPoint.Number.Uint64(),
		"fromHash", connectionPoint.Hash(),
		"toNum", initialHeader.Number.Uint64(),
		"toHash", initialHeader.Hash(),
	)
	return bbd.downloadBlocks(ctx, headerCollector, peers, config, feed)
}

func (bbd *BackwardBlockDownloader) loadPeers(config requestConfig) (peersContext, error) {
	if config.peerId != nil {
		return newPeersContext([]*p2p.PeerId{config.peerId}), nil
	}

	peers := bbd.peerTracker.ListPeers()
	if len(peers) == 0 {
		return peersContext{}, errors.New("no peers available")
	}

	return newPeersContext(peers), nil
}

func (bbd *BackwardBlockDownloader) downloadInitialHeader(
	ctx context.Context,
	hash common.Hash,
	peers peersContext,
	config requestConfig,
) (*types.Header, error) {
	peersHeadersResponses := make([][]*types.Header, len(peers.all))
	eg := errgroup.Group{}
	fetcherOpts := []p2p.FetcherOption{
		p2p.WithResponseTimeout(config.initialHeaderFetchTimeout),
		p2p.WithMaxRetries(config.initialHeaderFetchRetries),
	}
	for _, peer := range peers.all {
		eg.Go(func() error {
			peerIndex := peers.peerIdToIndex[*peer]
			resp, err := bbd.fetcher.FetchHeadersBackwards(ctx, hash, 1, peer, fetcherOpts...)
			if err != nil {
				bbd.logger.Trace(
					"[backward-block-downloader] peer does not have initial header",
					"hash", hash,
					"peer", peer,
					"err", err,
				)
				peers.exhaustedPeers[peerIndex] = true
				return nil
			}
			header := resp.Data[0]
			peersHeadersResponses[peerIndex] = []*types.Header{header}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		panic(err) // err not expected since we don't return any errors in the above goroutines
	}
	var header *types.Header
	for _, headers := range peersHeadersResponses {
		if len(headers) > 0 {
			header = headers[0]
			break
		}
	}
	if header == nil {
		return nil, fmt.Errorf("no peers have initial header hash for backward download: %s", hash)
	}
	headerNum := header.Number.Uint64()
	if headerNum == 0 {
		return nil, fmt.Errorf("asked to download hash at num 0: %s", hash)
	}
	currentHead := config.chainLengthCurrentHead
	if currentHead != nil && *currentHead > headerNum && *currentHead-headerNum >= config.chainLengthLimit {
		return nil, fmt.Errorf(
			"%w: num=%d, hash=%s, currentHead=%d, limit=%d",
			ErrChainLengthExceedsLimit,
			headerNum,
			hash,
			*config.chainLengthCurrentHead,
			config.chainLengthLimit,
		)
	}
	return header, nil
}

func (bbd *BackwardBlockDownloader) downloadHeaderChainBackwards(
	ctx context.Context,
	initialHeader *types.Header,
	headerCollector *etl.Collector,
	peers peersContext,
	config requestConfig,
) (*types.Header, error) {
	headerBytes, err := rlp.EncodeToBytes(initialHeader)
	if err != nil {
		return nil, err
	}
	err = headerCollector.Collect(dbutils.HeaderKey(initialHeader.Number.Uint64(), initialHeader.Hash()), headerBytes)
	if err != nil {
		return nil, err
	}

	fetcherOpts := []p2p.FetcherOption{
		p2p.WithResponseTimeout(config.headerChainBatchFetchTimeout),
		p2p.WithMaxRetries(config.headerChainBatchFetchRetries),
	}
	logProgressTicker := time.NewTicker(30 * time.Second)
	defer logProgressTicker.Stop()
	chainLen := uint64(1)
	lastHeader := initialHeader
	maxHeadersBatchLen := min(config.blocksBatchSize, eth.MaxHeadersServe)
	var connectionPoint *types.Header
	for connectionPoint == nil && lastHeader.Number.Uint64() > 0 {
		if chainLen >= config.chainLengthLimit {
			return nil, fmt.Errorf(
				"%w: num=%d, hash=%s, len=%d, limit=%d",
				ErrChainLengthExceedsLimit,
				initialHeader.Number.Uint64(),
				initialHeader.Hash(),
				chainLen,
				config.chainLengthLimit,
			)
		}

		parentHash := lastHeader.ParentHash
		parentNum := lastHeader.Number.Uint64() - 1
		amount := min(parentNum, maxHeadersBatchLen)
		if amount == 0 {
			// can't fetch 0 blocks, just check if the hash matches our genesis and if it does set the connecting point
			h, err := bbd.headerReader.HeaderByHash(ctx, parentHash)
			if err != nil {
				return nil, err
			}
			if h != nil {
				connectionPoint = lastHeader
			}
			break
		}

		peerId, err := peers.nextAvailablePeer()
		if err != nil {
			return nil, fmt.Errorf(
				"no peers available before reaching connection point num=%d, hash=%s: %w",
				parentNum,
				parentHash,
				err,
			)
		}

		progressLogArgs := []interface{}{
			"num", parentNum,
			"hash", parentHash,
			"amount", amount,
			"peerId", peerId.String(),
		}
		select {
		case <-logProgressTicker.C:
			bbd.logger.Info("[backward-block-downloader] fetching headers backward periodic progress", progressLogArgs...)
		default:
			bbd.logger.Trace("[backward-block-downloader] fetching headers backward", progressLogArgs...)
		}

		peerIndex := peers.peerIdToIndex[peerId]
		resp, err := bbd.fetcher.FetchHeadersBackwards(ctx, parentHash, amount, &peerId, fetcherOpts...)
		if err != nil {
			bbd.logger.Debug(
				"[backward-block-downloader] could not fetch headers batch from peer",
				"num", parentNum,
				"hash", parentHash,
				"amount", amount,
				"peerId", peerId.String(),
				"err", err,
			)
			peers.exhaustedPeers[peerIndex] = true
			continue
		}

		// collect the headers batch into the etl and check for a connecting point
		headers := resp.Data
		for i := len(headers) - 1; i >= 0; i-- {
			header := headers[i]
			headerNum := header.Number.Uint64()
			if headerNum == 0 {
				break
			}
			headerBytes, err = rlp.EncodeToBytes(header)
			if err != nil {
				return nil, err
			}
			err = headerCollector.Collect(dbutils.HeaderKey(headerNum, header.Hash()), headerBytes)
			if err != nil {
				return nil, err
			}
			chainLen++
			lastHeader = header
			h, err := bbd.headerReader.HeaderByHash(ctx, header.ParentHash)
			if err != nil {
				return nil, err
			}
			if h != nil {
				connectionPoint = header
				break
			}
		}
	}
	if connectionPoint == nil {
		return nil, fmt.Errorf("connection point not found: num=%d, hash=%s", initialHeader.Number.Uint64(), initialHeader.Hash())
	}
	return connectionPoint, nil
}

func (bbd *BackwardBlockDownloader) downloadBlocks(
	ctx context.Context,
	headerCollector *etl.Collector,
	peers peersContext,
	config requestConfig,
	feed ResultFeed,
) error {
	logProgressTicker := time.NewTicker(30 * time.Second)
	defer logProgressTicker.Stop()
	headers := make([]*types.Header, 0, config.blocksBatchSize)
	headerCollectorLoadFunc := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		var header types.Header
		err := rlp.DecodeBytes(v, &header)
		if err != nil {
			return err
		}
		headers = append(headers, &header)
		if uint64(len(headers)) < config.blocksBatchSize {
			return nil // keep accumulating headers
		}
		// we've accumulated enough headers in the batch - time to fetch them from peers and send to the result feed
		err = bbd.downloadBlocksForHeaders(ctx, headers, peers, config, logProgressTicker, feed)
		if err != nil {
			return err
		}
		headers = headers[:0]
		return nil
	}
	err := headerCollector.Load(nil, "", headerCollectorLoadFunc, etl.TransformArgs{Quit: ctx.Done()})
	if err != nil {
		return err
	}
	if len(headers) == 0 {
		return feed.consumeData(ctx, nil)
	}
	// make sure to download blocks for the remaining incomplete header batch after the etl collector has been loaded
	return bbd.downloadBlocksForHeaders(ctx, headers, peers, config, logProgressTicker, feed)
}

// downloadBlocksForHeaders downloads the bodies for the corresponding headers in parallel across all available peers
// and constructs the corresponding blocks which get fed to the result feed.
func (bbd *BackwardBlockDownloader) downloadBlocksForHeaders(
	ctx context.Context,
	headers []*types.Header,
	peers peersContext,
	config requestConfig,
	logProgressTicker *time.Ticker,
	feed ResultFeed,
) error {
	// split the headers into batches
	neededPeers := min(len(headers), config.maxParallelBodyDownloads)
	availablePeers, err := peers.nextAvailablePeers(neededPeers)
	if err != nil {
		return err
	}
	batchSize := (len(headers) + len(availablePeers) - 1) / len(availablePeers)
	batchesCount := (len(headers) + batchSize - 1) / batchSize
	progressLogArgs := []interface{}{
		"fromNum", headers[0].Number.Uint64(),
		"fromHash", headers[0].Hash(),
		"toNum", headers[len(headers)-1].Number.Uint64(),
		"toHash", headers[len(headers)-1].Hash(),
		"numHeaders", len(headers),
		"neededPeers", neededPeers,
		"availablePeers", len(availablePeers),
		"batchSize", batchSize,
		"batchesCount", batchesCount,
	}
	select {
	case <-logProgressTicker.C:
		bbd.logger.Info("[backward-block-downloader] downloading blocks for headers batch periodic progress", progressLogArgs...)
	default:
		bbd.logger.Trace("[backward-block-downloader] downloading blocks for headers batch", progressLogArgs...)
	}
	headerBatches := make([][]*types.Header, batchesCount)
	for i := range headerBatches {
		headerStartIndex := i * batchSize
		headerEndIndex := min(headerStartIndex+batchSize, len(headers))
		headerBatches[i] = headers[headerStartIndex:headerEndIndex]
	}

	// download from available peers until all batches are downloaded
	fetcherOpts := []p2p.FetcherOption{
		p2p.WithResponseTimeout(config.bodiesBatchFetchTimeout),
		p2p.WithMaxRetries(config.bodiesBatchFetchRetries),
	}
	type batchAssignment struct {
		peerId     p2p.PeerId
		batchIndex int
	}
	batchAssignments := make([]batchAssignment, 0, len(headerBatches))
	blockBatches := make([][]*types.Block, len(availablePeers))
	pendingBatches := true
	attempts := 1
	for pendingBatches {
		// assign remaining batches to available peers
		var peerIndex int
		batchAssignments = batchAssignments[:0]
		for batchIndex := range headerBatches {
			if blockBatches[batchIndex] != nil {
				continue // already fetched
			}
			if peerIndex == len(availablePeers) {
				break
			}
			batchAssignments = append(batchAssignments, batchAssignment{
				peerId:     availablePeers[peerIndex],
				batchIndex: batchIndex,
			})
			peerIndex++
		}
		eg := errgroup.Group{}
		for _, assignment := range batchAssignments {
			peerId := assignment.peerId
			batchIndex := assignment.batchIndex
			headerBatch := headerBatches[batchIndex]
			bbd.logger.Trace(
				"[backward-block-downloader] fetching bodies for headerBatch",
				"attempt", attempts,
				"fromNum", headerBatch[0].Number.Uint64(),
				"fromHash", headerBatch[0].Hash(),
				"toNum", headerBatch[len(headerBatch)-1].Number.Uint64(),
				"toHash", headerBatch[len(headerBatch)-1].Hash(),
				"peerId", peerId.String(),
			)

			eg.Go(func() error {
				bodiesResponse, err := bbd.fetcher.FetchBodies(ctx, headerBatch, &peerId, fetcherOpts...)
				if err != nil {
					bbd.logger.Debug(
						"[backward-block-downloader] could not fetch bodies batch",
						"fromNum", headerBatch[0].Number.Uint64(),
						"fromHash", headerBatch[0].Hash(),
						"toNum", headerBatch[len(headerBatch)-1].Number.Uint64(),
						"toHash", headerBatch[len(headerBatch)-1].Hash(),
						"peerId", peerId.String(),
						"err", err,
					)
					return nil
				}

				bodies := bodiesResponse.Data
				blockBatch := make([]*types.Block, 0, len(headerBatch))
				for i, header := range headerBatch {
					block := types.NewBlockFromNetwork(header, bodies[i])
					err = block.HashCheck(true)
					if err == nil {
						blockBatch = append(blockBatch, block)
						continue
					}

					bbd.logger.Debug(
						"[backward-block-downloader] block hash check failed, penalizing peer",
						"num", block.NumberU64(),
						"hash", block.Hash(),
						"peerId", peerId.String(),
						"err", err,
					)

					err = bbd.peerPenalizer.Penalize(ctx, &peerId)
					if err != nil {
						bbd.logger.Debug(
							"[backward-block-downloader] could not penalize peer",
							"peerId", peerId.String(),
							"err", err,
						)
					}

					break
				}
				if len(blockBatch) == len(headerBatch) {
					blockBatches[batchIndex] = blockBatch
				}
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			panic(err) // workers do not return errs
		}
		// mark peers as exhausted for those that had unsuccessful fetches
		var incompleteBatches int
		for _, assignment := range batchAssignments {
			if blockBatches[assignment.batchIndex] == nil {
				peers.exhaustedPeers[peers.peerIdToIndex[assignment.peerId]] = true
				incompleteBatches++
			}
		}
		if incompleteBatches > 0 {
			attempts++
			// recalculate the available peers as some have become exhausted
			availablePeers, err = peers.nextAvailablePeers(incompleteBatches)
			if err != nil {
				return err
			}
		} else {
			pendingBatches = false
		}
	}

	blocks := make([]*types.Block, 0, len(headers))
	for _, blocksBatch := range blockBatches {
		blocks = append(blocks, blocksBatch...)
	}

	err = feed.consumeData(ctx, blocks)
	if err != nil {
		return fmt.Errorf("result feed could not consume blocks batch: %w", err)
	}

	return nil
}

func newPeersContext(peers []*p2p.PeerId) peersContext {
	peerIdToIndex := make(map[p2p.PeerId]int, len(peers))
	peerIndexToId := make(map[int]p2p.PeerId, len(peers))
	for i, peer := range peers {
		peerIdToIndex[*peer] = i
		peerIndexToId[i] = *peer
	}
	return peersContext{
		all:            peers,
		peerIdToIndex:  peerIdToIndex,
		peerIndexToId:  peerIndexToId,
		exhaustedPeers: make([]bool, len(peers)),
	}
}

type peersContext struct {
	all              []*p2p.PeerId
	peerIdToIndex    map[p2p.PeerId]int
	peerIndexToId    map[int]p2p.PeerId
	exhaustedPeers   []bool
	currentPeerIndex int
}

func (pc *peersContext) nextAvailablePeer() (p2p.PeerId, error) {
	var iterations int
	for pc.exhaustedPeers[pc.currentPeerIndex] {
		pc.incrementCurrentPeerIndex()
		if iterations == len(pc.exhaustedPeers) {
			return p2p.PeerId{}, errors.New("all peers exhausted")
		}
		iterations++
	}
	peer := pc.peerIndexToId[pc.currentPeerIndex]
	pc.incrementCurrentPeerIndex()
	return peer, nil
}

func (pc *peersContext) nextAvailablePeers(n int) ([]p2p.PeerId, error) {
	peers := make([]p2p.PeerId, 0, n)
	unique := make(map[p2p.PeerId]struct{}, n)
	for len(peers) < n {
		pid, err := pc.nextAvailablePeer()
		if err != nil {
			return nil, err
		}
		if _, ok := unique[pid]; ok {
			break // we've done a full rotation across all available peers
		}
		unique[pid] = struct{}{}
		peers = append(peers, pid)
	}
	return peers, nil
}

func (pc *peersContext) incrementCurrentPeerIndex() {
	pc.currentPeerIndex++
	pc.currentPeerIndex %= len(pc.exhaustedPeers)
}
