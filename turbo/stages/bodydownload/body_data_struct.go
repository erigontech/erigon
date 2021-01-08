package bodydownload

import (
	"container/list"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth"
)

// DoubleHash is type to be used for the mapping between TxHash and UncleHash to the block header
type DoubleHash [2 * common.HashLength]byte

const MaxBodiesInRequest = 128

// BodyDownload represents the state of body downloading process
type BodyDownload struct {
	lock              sync.RWMutex
	required          *roaring64.Bitmap // Bitmap of block numbers for which the block bodies are required
	requested         *roaring64.Bitmap // Bitmap of block numbers for which block bodies were requested
	delivered         *roaring64.Bitmap // Bitmap of block numbers that have been delivered but not yet inserted into the database
	deliveries        []*eth.BlockBody
	requestedMap      map[DoubleHash]*types.Header
	requestedLow      uint64     // Lower bound of block number for outstanding requests
	requestedHigh     uint64     // Higher bound of block number for outstanding requests
	outstandingLimit  uint64     // Limit of number of outstanding blocks for body requests
	requestQueue      *list.List // Queue of items of type RequestQueueItem to deal with the request timeouts
	RequestQueueTimer *time.Timer
}

type RequestQueueItem struct {
	requested *roaring64.Bitmap
	waitUntil uint64
}

// BodyRequest is a sketch of the request for block bodies, meaning that access to the database is required to convert it to the actual BlockBodies request (look up hashes of canonical blocks)
type BodyRequest struct {
	BlockNums []uint64
}

// NewBodyDownload create a new body download state object
func NewBodyDownload(outstandingLimit int) *BodyDownload {
	return &BodyDownload{
		required:          roaring64.New(),
		requested:         roaring64.New(),
		delivered:         roaring64.New(),
		requestedMap:      make(map[DoubleHash]*types.Header),
		outstandingLimit:  uint64(outstandingLimit),
		deliveries:        make([]*eth.BlockBody, outstandingLimit+MaxBodiesInRequest),
		requestQueue:      list.New(),
		RequestQueueTimer: time.NewTimer(time.Hour),
	}
}
