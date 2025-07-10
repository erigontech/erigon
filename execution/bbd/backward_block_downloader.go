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

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/kv/dbutils"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/p2p/sentry"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
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
	statusDataFactory sentry.StatusDataFactory,
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
//     (default: distributes requests across all all that have the initial block hash)
//   - WithBlocksBatchSize - controls the size of the block batch that we fetch from all and send to the result feed
//     (default: 500 blocks)
//   - WithChainLengthLimit - terminate the download if the backward header chain goes beyond a certain length.
//     (default: unlimited)
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
	initialHeader, err := bbd.downloadInitialHeader(ctx, hash, peers)
	if err != nil {
		return err
	}

	// 3. Download the header chain backwards in an etl collector until we connect it to a known local header.
	//    Note we fetch headers in batches of min(config.blocksBatchSize,1024) from 1 peer and send every following
	//    request to the next available peer (in rotating fashion) to distribute the requests across all peers.
	etlSortableBuf := etl.NewSortableBuffer(etl.BufferOptimalSize)
	headerCollector := etl.NewCollector("backward-block-downloader", bbd.tmpDir, etlSortableBuf, bbd.logger)
	defer headerCollector.Close()
	err = bbd.downloadHeaderChain(ctx, initialHeader, headerCollector, peers, config)
	if err != nil {
		return err
	}

	// 4. Start forward downloading the bodies in batches of config.blocksBatchSize from the next in-turn peer.
	//    Upon every complete batch we construct the blocks and feed them to the result feed.
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
) (*types.Header, error) {
	bbd.logger.Debug("[backward-block-downloader] downloading initial header", "hash", hash)
	peersHeadersResponses := make([][]*types.Header, len(peers.all))
	eg := errgroup.Group{}
	for _, peer := range peers.all {
		eg.Go(func() error {
			peerIndex := peers.peerIdToIndex[*peer]
			resp, err := bbd.fetcher.FetchHeadersBackwards(ctx, hash, 1, peer)
			if err != nil {
				bbd.logger.Debug(
					"[backward-block-downloader] peer does not have initial header",
					"peer", peer,
					"hash", hash,
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
	var initialHeader *types.Header
	for _, headers := range peersHeadersResponses {
		if len(headers) > 0 {
			initialHeader = headers[0]
			break
		}
	}
	if initialHeader == nil {
		return nil, fmt.Errorf("no peers have initial header hash for backward download: %s", hash)
	}
	if initialHeader.Number.Uint64() == 0 {
		return nil, fmt.Errorf("asked to download hash at height 0: %s", hash)
	}
	return initialHeader, nil
}

func (bbd *BackwardBlockDownloader) downloadHeaderChain(
	ctx context.Context,
	start *types.Header,
	headerCollector *etl.Collector,
	peers peersContext,
	config requestConfig,
) error {
	headerBytes, err := rlp.EncodeToBytes(start)
	if err != nil {
		return err
	}
	err = headerCollector.Collect(dbutils.HeaderKey(start.Number.Uint64(), start.Hash()), headerBytes)
	if err != nil {
		return err
	}

	chainLen := uint64(1)
	lastHeader := start
	maxHeadersBatchLen := min(config.blocksBatchSize, eth.MaxHeadersServe)
	var connectionPoint *types.Header
	for connectionPoint == nil && lastHeader.Number.Uint64() > 0 {
		if chainLen >= config.chainLengthLimit {
			return fmt.Errorf(
				"%w: hash=%s, height=%d, len=%d",
				ErrChainLengthExceedsLimit,
				start.Hash(),
				start.Number.Uint64(),
				chainLen,
			)
		}

		parentHash := lastHeader.ParentHash
		parentHeight := lastHeader.Number.Uint64() - 1
		amount := min(parentHeight, maxHeadersBatchLen)
		if amount == 0 {
			// can't fetch 0 blocks, just check if the hash matches our genesis and if it does set the connecting point
			h, err := bbd.headerReader.HeaderByHash(ctx, parentHash)
			if err != nil {
				return err
			}
			if h != nil {
				connectionPoint = lastHeader
			}
			break
		}

		peerId, err := peers.nextAvailablePeer()
		if err != nil {
			return fmt.Errorf(
				"no peers available before reaching connection point hash=%s, height=%d: %w",
				parentHash,
				parentHeight,
				err,
			)
		}

		bbd.logger.Debug(
			"[backward-block-downloader] fetching headers backward",
			"hash", parentHash,
			"height", parentHeight,
			"amount", amount,
			"peerId", peerId,
		)

		peerIndex := peers.peerIdToIndex[peerId]
		resp, err := bbd.fetcher.FetchHeadersBackwards(ctx, parentHash, amount, &peerId)
		if err != nil {
			bbd.logger.Debug(
				"[backward-block-downloader] could not fetch headers batch",
				"hash", parentHash,
				"height", parentHeight,
				"amount", amount,
				"peerId", peerId,
				"err", err,
			)
			peers.exhaustedPeers[peerIndex] = true
			return nil
		}

		// collect the headers batch into the etl and check for a connecting point
		headers := resp.Data
		for i := len(headers) - 1; i >= 0; i-- {
			header := headers[i]
			headerNum := header.Number.Uint64()
			if headerNum == 0 {
				return nil
			}
			headerBytes, err = rlp.EncodeToBytes(header)
			if err != nil {
				return err
			}
			err = headerCollector.Collect(dbutils.HeaderKey(headerNum, header.Hash()), headerBytes)
			if err != nil {
				return err
			}
			chainLen++
			lastHeader = header
			h, err := bbd.headerReader.HeaderByHash(ctx, header.ParentHash)
			if err != nil {
				return err
			}
			if h != nil {
				connectionPoint = header
				break
			}
		}
	}
	if connectionPoint == nil {
		return fmt.Errorf("connection point not found: hash=%s, height=%d", start.Hash(), start.Number.Uint64())
	}
	return nil
}

func (bbd *BackwardBlockDownloader) downloadBlocks(
	ctx context.Context,
	headerCollector *etl.Collector,
	peers peersContext,
	config requestConfig,
	feed ResultFeed,
) error {
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
		err = bbd.downloadBlocksForHeaders(ctx, headers, peers, config, feed)
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
	return bbd.downloadBlocksForHeaders(ctx, headers, peers, config, feed)
}

// downloadBlocksForHeaders downloads the bodies for the corresponding headers in parallel across all available peers
// and constructs the corresponding blocks which get fed to the result feed.
func (bbd *BackwardBlockDownloader) downloadBlocksForHeaders(
	ctx context.Context,
	headers []*types.Header,
	peers peersContext,
	config requestConfig,
	feed ResultFeed,
) error {
	neededPeers := min(len(headers), config.maxParallelBodyDownloads)
	availablePeers, err := peers.nextAvailablePeers(neededPeers)
	if err != nil {
		return err
	}

	// split the headers into batches
	batchSize := (len(headers) + len(availablePeers)) / len(availablePeers)
	headerBatches := make([][]*types.Header, 0, len(availablePeers))
	for i := range availablePeers {
		headerStartIndex := i * batchSize
		headerEndIndex := min(headerStartIndex+batchSize, len(headers))
		headerBatches[i] = headers[headerStartIndex:headerEndIndex]
	}

	// download from peers available until all batches are downloaded
	blockBatches := make([][]*types.Block, 0, len(availablePeers))
	pendingBatches := true
	attempts := 1
	for pendingBatches {
		// assign remaining batches to available peers
		batchAssignments := make(map[int]p2p.PeerId, len(headerBatches))
		var peerIndex int
		for batchIndex := range headerBatches {
			if blockBatches[batchIndex] != nil {
				continue // already fetched
			}
			if peerIndex == len(availablePeers) {
				break
			}
			batchAssignments[batchIndex] = availablePeers[peerIndex]
			peerIndex++
		}
		eg := errgroup.Group{}
		for headerBatchIndex, peerId := range batchAssignments {
			headers := headerBatches[headerBatchIndex]
			bbd.logger.Debug(
				"[backward-block-downloader] fetching bodies for headers",
				"attempt", attempts,
				"fromHeight", headers[0].Number.Uint64(),
				"fromHash", headers[0].Hash(),
				"toHeight", headers[len(headers)-1].Number.Uint64(),
				"toHash", headers[len(headers)-1].Hash(),
				"peerId", peerId,
			)

			eg.Go(func() error {
				bodiesResponse, err := bbd.fetcher.FetchBodies(ctx, headers, &peerId)
				if err != nil {
					bbd.logger.Debug(
						"[backward-block-downloader] could not fetch bodies batch",
						"fromHeight", headers[0].Number.Uint64(),
						"fromHash", headers[0].Hash(),
						"toHeight", headers[len(headers)-1].Number.Uint64(),
						"toHash", headers[len(headers)-1].Hash(),
						"peerId", peerId,
						"err", err,
					)
					return nil
				}

				bodies := bodiesResponse.Data
				blocks := make([]*types.Block, len(headers))
				for i, header := range headers {
					block := types.NewBlockFromNetwork(header, bodies[i])
					err = block.HashCheck(true)
					if err == nil {
						blocks = append(blocks, block)
						continue
					}

					bbd.logger.Debug(
						"[backward-block-downloader] block hash check failed, penalizing peer",
						"hash", block.Hash(),
						"num", block.NumberU64(),
						"peerId", peerId,
						"err", err,
					)

					err = bbd.peerPenalizer.Penalize(ctx, &peerId)
					if err != nil {
						bbd.logger.Debug(
							"[backward-block-downloader] could not penalize peer",
							"peerId", peerId,
							"err", err,
						)
					}

					break
				}
				if len(blocks) == len(headers) {
					blockBatches[headerBatchIndex] = blocks
				}
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			panic(err) // workers do not return errs
		}
		// mark peers as exhausted for those that had unsuccessful fetches
		allBatchesComplete := true
		for batchIndex, peerId := range batchAssignments {
			if blockBatches[batchIndex] == nil {
				peers.exhaustedPeers[peers.peerIdToIndex[peerId]] = true
				allBatchesComplete = false
			}
		}
		if !allBatchesComplete {
			attempts++
			// recalculate the available peers as some have become exhausted
			availablePeers, err = peers.nextAvailablePeers(config.maxParallelBodyDownloads)
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
	for !pc.exhaustedPeers[pc.currentPeerIndex] {
		pc.incrementCurrentPeerIndex()
		if iterations == len(pc.exhaustedPeers) {
			return p2p.PeerId{}, errors.New("all all exhausted")
		}
		iterations++
	}
	peer := pc.peerIndexToId[pc.currentPeerIndex]
	pc.incrementCurrentPeerIndex()
	return peer, nil
}

func (pc *peersContext) nextAvailablePeers(n int) ([]p2p.PeerId, error) {
	peers := make([]p2p.PeerId, 0, len(pc.exhaustedPeers))
	for iteration := 0; iteration < len(pc.exhaustedPeers) && n > 0; iteration++ {
		pid, err := pc.nextAvailablePeer()
		if err != nil {
			return nil, err
		}
		peers = append(peers, pid)
		n--
	}
	return peers, nil
}

func (pc *peersContext) incrementCurrentPeerIndex() {
	pc.currentPeerIndex++
	pc.currentPeerIndex %= len(pc.exhaustedPeers)
}
