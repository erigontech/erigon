package bodydownload

import (
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

// DoubleHash is type to be used for the mapping between TxHash and UncleHash to the block header
type DoubleHash [2 * common.HashLength]byte

const MaxBodiesInRequest = 1024

// BodyDownload represents the state of body downloading process
type BodyDownload struct {
	lock             sync.RWMutex
	delivered        *roaring64.Bitmap
	deliveries       []*types.Block
	deliveredCount   float64
	wastedCount      float64
	requests         []*BodyRequest
	requestedMap     map[DoubleHash]uint64
	maxProgress      uint64
	requestedLow     uint64 // Lower bound of block number for outstanding requests
	requestHigh      uint64
	lowWaitUntil     uint64 // Time to wait for before starting the next round request from requestedLow
	outstandingLimit uint64 // Limit of number of outstanding blocks for body requests
	peerMap          map[string]int
	prefetchedBlocks *PrefetchedBlocks
}

// BodyRequest is a sketch of the request for block bodies, meaning that access to the database is required to convert it to the actual BlockBodies request (look up hashes of canonical blocks)
type BodyRequest struct {
	BlockNums []uint64
	Hashes    []common.Hash
	peerID    []byte
	waitUntil uint64
}

// NewBodyDownload create a new body download state object
func NewBodyDownload(outstandingLimit int) *BodyDownload {
	bd := &BodyDownload{
		requestedMap:     make(map[DoubleHash]uint64),
		outstandingLimit: uint64(outstandingLimit),
		delivered:        roaring64.New(),
		deliveries:       make([]*types.Block, outstandingLimit+MaxBodiesInRequest),
		requests:         make([]*BodyRequest, outstandingLimit+MaxBodiesInRequest),
		peerMap:          make(map[string]int),
		prefetchedBlocks: NewPrefetchedBlocks(),
	}
	return bd
}
