package bbd

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv/dbutils"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/polygon/p2p"
)

var ErrChainLengthExceedsLimit = errors.New("chain length exceeds limit")

type BackwardBlockDownloader struct {
	logger       log.Logger
	fetcher      p2p.Fetcher
	peerTracker  *p2p.PeerTracker
	headerReader headerReader
	tmpDir       string
}

// DownloadBlocksBackwards downloads blocks backwards given a starting block hash. It uses the underlying header reader
// to figure out when a header chain connects with a header that we already have. The backward download can handle
// chain lengths of unlimited size by using an etl for temporarily storing the headers. This is also enabled by a
// paging-like ResultFeed, which can be used to return pages of blocks as they get fetched in batches.
//
// There are a number of Option-s that can be passed in to customise the behaviour of the request:
//   - WithPeerId - in case the backward needs to happen from a specific peer only
//     (default: distributes requests across all peers that have the initial block hash)
//   - WithBlocksBatchSize - controls the size of the block batch that we fetch from peers and send to the result feed
//     (default: 500 blocks)
//   - WithChainLengthLimit - terminate the download if the backward header chain goes beyond a certain length.
//     (default: unlimited)
func (bbd BackwardBlockDownloader) DownloadBlocksBackwards(ctx context.Context, hash common.Hash, opts ...Option) ResultFeed {
	resultC := make(chan BatchResult)
	sendResult := func(ctx context.Context, blocks []*types.Block, hasNext bool) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resultC <- BatchResult{Blocks: blocks, HasNext: hasNext}:
			return nil
		}
	}
	go func() {
		defer close(resultC)
		err := bbd.fetchBlocksBackwardsByHash(ctx, hash, sendResult, opts...)
		if err != nil {
			resultC <- BatchResult{Err: err}
		}
	}()
	return ResultFeed{resultC}
}

func (bbd BackwardBlockDownloader) fetchBlocksBackwardsByHash(
	ctx context.Context,
	hash common.Hash,
	sendResult func(ctx context.Context, blocks []*types.Block, hasNext bool) error,
	opts ...Option,
) error {
	config := applyOptions(opts...)
	// 1. Get all peers
	var peers []*p2p.PeerId
	if config.peerId != nil {
		peers = []*p2p.PeerId{config.peerId}
	} else {
		peers = bbd.peerTracker.ListPeers()
	}
	if len(peers) == 0 {
		return errors.New("no peers")
	}
	peerIdToIndex := make(map[p2p.PeerId]int, len(peers))
	peerIndexToId := make(map[int]p2p.PeerId, len(peers))
	for i, peer := range peers {
		peerIdToIndex[*peer] = i
		peerIndexToId[i] = *peer
	}

	// 2. Check which peers have the header to build knowledge of which peers we can use for syncing
	//    and terminate early if none have seen it
	exhaustedPeers := make([]bool, len(peers))
	peersHeadersResponses := make([][]*types.Header, len(peers))
	eg := errgroup.Group{}
	for _, peer := range peers {
		eg.Go(func() error {
			peerIndex := peerIdToIndex[*peer]
			resp, err := bbd.fetcher.FetchHeadersBackwards(ctx, hash, 1, peer)
			if err != nil {
				bbd.logger.Debug(
					"[backward-block-downloader] peer does not have initial header",
					"peer", peer,
					"hash", hash,
					"err", err,
				)
				exhaustedPeers[peerIndex] = true
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
		return fmt.Errorf("no peers have initial header hash for backward download: %s", hash)
	}
	if initialHeader.Number.Uint64() == 0 {
		return fmt.Errorf("asked to download hash at height 0: %s", hash)
	}

	// 3. Download the header chain backwards in an etl collector until we connect it to a known local header.
	//    Note we fetch headers in batches of 1024 max from 1 peer and send every following request to
	//    the next available peer (in rotating fashion) to distribute the requests across all peers.
	sortableBuffer := etl.NewSortableBuffer(etl.BufferOptimalSize)
	headerCollector := etl.NewCollector("backward-block-downloader", bbd.tmpDir, sortableBuffer, bbd.logger)
	defer headerCollector.Close()
	headerRlpBuf := make([]byte, 0, 512)
	err := initialHeader.EncodeRLP(bytes.NewBuffer(headerRlpBuf))
	if err != nil {
		return err
	}
	err = headerCollector.Collect(dbutils.HeaderKey(initialHeader.Number.Uint64(), initialHeader.Hash()), headerRlpBuf)
	if err != nil {
		return err
	}

	chainLen := uint64(1)
	lastHeader := initialHeader
	var connectionPoint *types.Header
	var curPeerIndex int
	for connectionPoint == nil && lastHeader.Number.Uint64() > 0 {
		if chainLen >= config.chainLengthLimit {
			return fmt.Errorf(
				"%w: hash=%s, height=%d, len=%d",
				ErrChainLengthExceedsLimit,
				initialHeader.Hash(),
				initialHeader.Number.Uint64(),
				chainLen,
			)
		}

		parentHash := lastHeader.ParentHash
		parentHeight := lastHeader.Number.Uint64() - 1
		amount := min(parentHeight, eth.MaxHeadersServe)
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

		peerId, err := nextAvailablePeer(exhaustedPeers, &curPeerIndex, peerIndexToId)
		if err != nil {
			return fmt.Errorf(
				"no peers available before reaching connection point hash=%s, height=%d: %w",
				parentHash,
				parentHeight,
				err,
			)
		}

		peerIndex := peerIdToIndex[peerId]
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
			exhaustedPeers[peerIndex] = true
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
			w := bytes.NewBuffer(headerRlpBuf[:0])
			err = header.EncodeRLP(w)
			if err != nil {
				return err
			}
			err = headerCollector.Collect(dbutils.HeaderKey(headerNum, header.Hash()), w.Bytes())
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
		return fmt.Errorf("connection point not found: hash=%s, height=%d", initialHeader.Hash(), initialHeader.Number.Uint64())
	}

	// 4. Start downloading the bodies from 1 random peer at a time
	blockBatch := make([]*types.Block, 0, config.blocksBatchSize)
	headerCollectorLoadFunc := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		var header types.Header
		header.DecodeRLP()
		return nil
	}

	return headerCollector.Load(nil, "", headerCollectorLoadFunc, etl.TransformArgs{Quit: ctx.Done()})
}

func nextAvailablePeer(exhaustedPeers []bool, cur *int, peerIndexToId map[int]p2p.PeerId) (p2p.PeerId, error) {
	var iterations int
	for !exhaustedPeers[*cur] {
		*cur = (*cur + 1) % len(exhaustedPeers)
		if iterations == len(exhaustedPeers) {
			return p2p.PeerId{}, errors.New("all peers exhausted")
		}
		iterations++
	}
	return peerIndexToId[*cur], nil
}
