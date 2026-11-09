// Copyright 2026 The Erigon Authors
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

package follower

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/deriver"
	execp2p "github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
)

// ErrNoPeers reports a fetch that no connected L2 peer serves: none is
// connected, or none has the target.
var ErrNoPeers = errors.New("no L2 peer serves the block")

// fetchBatch is the number of blocks downloaded and inserted at a time, the
// engine API downloader's batch.
const fetchBatch = 500

// BlockDownloader downloads blocks by hash from the chain's devp2p peers.
type BlockDownloader interface {
	DownloadBlocksBackwards(ctx context.Context, hash common.Hash, headers execp2p.BbdHeaderReader, opts ...execp2p.BbdOption) (execp2p.BbdResultFeed, error)
}

// PeerCounter counts the node's connected devp2p peers.
type PeerCounter interface {
	NetPeerCount() (uint64, error)
}

// FetchOptions bound a Fetch.
type FetchOptions struct {
	// Limit, when set, refuses a target more than Limit blocks from the
	// local head and a chain of more than Limit missing blocks.
	Limit uint64
	// Inserted, when set, runs after each inserted batch.
	Inserted func(ctx context.Context, blocks []*types.Block) error
}

// Fetcher fetches blocks by hash from the chain's L2 peers into the local
// chain.
type Fetcher struct {
	blocks BlockDownloader
	peers  PeerCounter
	chain  deriver.Chain
	logger log.Logger
}

// NewFetcher returns a fetcher into chain that downloads through blocks;
// peers counts the connected peers it reports.
func NewFetcher(blocks BlockDownloader, peers PeerCounter, chain deriver.Chain, logger log.Logger) *Fetcher {
	return &Fetcher{blocks: blocks, peers: peers, chain: chain, logger: logger}
}

// Peers returns the number of connected peers, zero when it cannot count
// them.
func (f *Fetcher) Peers() uint64 {
	n, err := f.peers.NetPeerCount()
	if err != nil {
		return 0
	}
	return n
}

// Fetch inserts into the local chain the blocks up to hash that it does not
// store, oldest first, and returns the header of hash. It moves no fork
// choice. Without a connected peer it sends no request.
func (f *Fetcher) Fetch(ctx context.Context, hash common.Hash, opts FetchOptions) (*types.Header, error) {
	if h, err := f.chain.HeaderByHash(ctx, hash); err != nil || h != nil {
		return h, err
	}
	var headers execp2p.BbdHeaderReader = f.chain
	dl := []execp2p.BbdOption{execp2p.WithBlocksBatchSize(fetchBatch)}
	if opts.Limit > 0 {
		head, err := f.chain.Head(ctx)
		if err != nil {
			return nil, err
		}
		dl = append(dl, execp2p.WithChainLengthLimit(opts.Limit), execp2p.WithChainLengthCurrentHead(head.Number.Uint64()))
		headers = &boundedHeaders{headers: headers, limit: opts.Limit}
	}
	// Ends the download when an insert fails.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	feed, err := f.blocks.DownloadBlocksBackwards(ctx, hash, headers, dl...)
	if err != nil {
		return nil, fmt.Errorf("fetch %x: %w", hash, err)
	}
	var tip *types.Header
	for {
		batch, err := feed.Next(ctx)
		if errors.Is(err, execp2p.ErrNoPeers) || errors.Is(err, execp2p.ErrNoPeerHasHeader) {
			return nil, fmt.Errorf("fetch %x: %w: %w", hash, ErrNoPeers, err)
		}
		if err != nil {
			return nil, fmt.Errorf("fetch %x: %w", hash, err)
		}
		blocks := batch.Blocks
		if len(blocks) == 0 {
			break
		}
		first, last := blocks[0].NumberU64(), blocks[len(blocks)-1].NumberU64()
		if err := f.chain.Insert(ctx, blocks); err != nil {
			return nil, fmt.Errorf("insert fetched blocks %d..%d: %w", first, last, err)
		}
		tip = blocks[len(blocks)-1].Header()
		f.logger.Debug("[eez] fetched blocks from L2 peers", "from", first, "to", last, "target", hash)
		if opts.Inserted != nil {
			if err := opts.Inserted(ctx, blocks); err != nil {
				return nil, err
			}
		}
	}
	if tip == nil || tip.Hash() != hash {
		return nil, fmt.Errorf("fetch %x: the download ended below it", hash)
	}
	return tip, nil
}

// boundedHeaders fails the downloader's walk back once it has asked for
// more than limit missing blocks: the walk checks its length only between
// header batches.
type boundedHeaders struct {
	headers execp2p.BbdHeaderReader
	limit   uint64
	missing uint64
}

func (b *boundedHeaders) HeaderByHash(ctx context.Context, hash common.Hash) (*types.Header, error) {
	h, err := b.headers.HeaderByHash(ctx, hash)
	if err != nil || h != nil {
		return h, err
	}
	if b.missing++; b.missing > b.limit {
		return nil, fmt.Errorf("%w: more than %d missing blocks", execp2p.ErrChainLengthExceedsLimit, b.limit)
	}
	return nil, nil
}
