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

package bodydownload

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/google/btree"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/v3/turbo/services"

	"github.com/erigontech/erigon/v3/consensus"
	"github.com/erigontech/erigon/v3/core/types"
)

// BodyHashes is to be used for the mapping between TxHash, UncleHash, and WithdrawalsHash to the block header
type BodyHashes [3 * length.Hash]byte

const MaxBodiesInRequest = 1024

type Delivery struct {
	peerID          [64]byte
	txs             [][][]byte
	uncles          [][]*types.Header
	withdrawals     []types.Withdrawals
	lenOfP2PMessage uint64
}

// BodyQueueItem is part of the body cache kept in memory
type BodyTreeItem struct {
	blockNum    uint64
	payloadSize int
	rawBody     *types.RawBody
}

// BodyDownload represents the state of body downloading process
type BodyDownload struct {
	peerMap          map[[64]byte]int
	requestedMap     map[BodyHashes]uint64
	DeliveryNotify   chan struct{}
	deliveryCh       chan Delivery
	Engine           consensus.Engine
	delivered        *roaring64.Bitmap
	prefetchedBlocks *PrefetchedBlocks
	deliveriesH      map[uint64]*types.Header
	requests         map[uint64]*BodyRequest
	maxProgress      uint64
	requestedLow     uint64 // Lower bound of block number for outstanding requests
	deliveredCount   float64
	wastedCount      float64
	bodyCache        *btree.BTreeG[BodyTreeItem]
	bodyCacheSize    int
	bodyCacheLimit   int // Limit of body Cache size
	blockBufferSize  int
	br               services.FullBlockReader
	logger           log.Logger
}

// BodyRequest is a sketch of the request for block bodies, meaning that access to the database is required to convert it to the actual BlockBodies request (look up hashes of canonical blocks)
type BodyRequest struct {
	BlockNums []uint64
	Hashes    []libcommon.Hash
	peerID    [64]byte
	waitUntil uint64
}

// NewBodyDownload create a new body download state object
func NewBodyDownload(engine consensus.Engine, blockBufferSize, bodyCacheLimit int, br services.FullBlockReader, logger log.Logger) *BodyDownload {
	bd := &BodyDownload{
		requestedMap:     make(map[BodyHashes]uint64),
		bodyCacheLimit:   bodyCacheLimit,
		delivered:        roaring64.New(),
		deliveriesH:      make(map[uint64]*types.Header),
		requests:         make(map[uint64]*BodyRequest),
		peerMap:          make(map[[64]byte]int),
		prefetchedBlocks: NewPrefetchedBlocks(),
		// DeliveryNotify has capacity 1, and it is also used so that senders never block
		// This makes this channel a mailbox with no more than one letter in it, meaning
		// that there is something to collect
		DeliveryNotify: make(chan struct{}, 1),
		// delivery channel needs to have enough capacity not to create contention
		// between delivery and collections
		deliveryCh:      make(chan Delivery, 2*MaxBodiesInRequest),
		Engine:          engine,
		bodyCache:       btree.NewG[BodyTreeItem](32, func(a, b BodyTreeItem) bool { return a.blockNum < b.blockNum }),
		br:              br,
		blockBufferSize: blockBufferSize,
		logger:          logger,
	}
	return bd
}
