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

package sync

import (
	"context"
	"math/big"

	"github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/p2p"
)

//go:generate mockgen -typed=true -source=./p2p_service.go -destination=./p2p_service_mock.go -package=sync . p2pservice
type p2pService interface {
	Run(ctx context.Context) error
	MaxPeers() int
	ListPeersMayHaveBlockNum(blockNum uint64) []*p2p.PeerId
	FetchHeaders(ctx context.Context, start, end uint64, peerId *p2p.PeerId, opts ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Header], error)
	FetchBodies(ctx context.Context, headers []*types.Header, peerId *p2p.PeerId, opts ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Body], error)
	FetchBlocksBackwardsByHash(ctx context.Context, hash common.Hash, amount uint64, peerId *p2p.PeerId, opts ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Block], error)
	PublishNewBlock(block *types.Block, td *big.Int)
	PublishNewBlockHashes(block *types.Block)
	Penalize(ctx context.Context, peerId *p2p.PeerId) error
}
