// Copyright 2024 The Erigon Authors
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

package p2p

import (
	"context"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
)

type Fetcher interface {
	// FetchHeaders fetches [start,end) headers from a peer. Blocks until data is received.
	FetchHeaders(
		ctx context.Context,
		start uint64,
		end uint64,
		peerId *PeerId,
		opts ...FetcherOption,
	) (FetcherResponse[[]*types.Header], error)

	// FetchBodies fetches block bodies for the given headers from a peer. Blocks until data is received.
	FetchBodies(
		ctx context.Context,
		headers []*types.Header,
		peerId *PeerId,
		opts ...FetcherOption,
	) (FetcherResponse[[]*types.Body], error)

	// FetchBlocksBackwardsByHash fetches a number of blocks backwards starting from a block hash. Max amount is 1024
	// blocks back. Blocks until data is received.
	FetchBlocksBackwardsByHash(
		ctx context.Context,
		hash common.Hash,
		amount uint64,
		peerId *PeerId,
		opts ...FetcherOption,
	) (FetcherResponse[[]*types.Block], error)
}

type FetcherResponse[T any] struct {
	Data      T
	TotalSize int
}
