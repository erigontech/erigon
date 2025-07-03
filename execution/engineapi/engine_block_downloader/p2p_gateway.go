package engine_block_downloader

import (
	"context"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/polygon/p2p"
)

type p2pGateway interface {
	ListPeers() []*p2p.PeerId

	FetchHeadersBackwards(
		ctx context.Context,
		hash common.Hash,
		amount uint64,
		peerId *p2p.PeerId,
		opts ...p2p.FetcherOption,
	) (p2p.FetcherResponse[[]*types.Header], error)

	FetchBodies(
		ctx context.Context,
		headers []*types.Header,
		peerId *p2p.PeerId,
		opts ...p2p.FetcherOption,
	) (p2p.FetcherResponse[[]*types.Body], error)
}
