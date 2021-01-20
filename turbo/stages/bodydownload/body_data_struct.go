package bodydownload

import (
	"container/heap"
	"container/list"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
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
	deliveries        []*types.Block
	requestedMap      map[DoubleHash]uint64
	requestedLow      uint64       // Lower bound of block number for outstanding requests
	outstandingLimit  uint64       // Limit of number of outstanding blocks for body requests
	expirationList    *list.List   // List of block body requests that have not "expired" yet. After expiry they are moved to the requestQueue
	requestQueue      RequestQueue // Priority queue of block body requests by minimum block number. Used to proactively retry requests that will be blocking the progress soon
	RequestQueueTimer *time.Timer
	blockChannel      chan *types.Block
}

type RequestQueueItem struct {
	lowestBlockNum uint64
	requested      *roaring64.Bitmap
	waitUntil      uint64
}

type RequestQueue []RequestQueueItem

func (rq RequestQueue) Len() int {
	return len(rq)
}

func (rq RequestQueue) Less(i, j int) bool {
	return rq[i].lowestBlockNum < rq[j].lowestBlockNum
}

func (rq RequestQueue) Swap(i, j int) {
	rq[i], rq[j] = rq[j], rq[i]
}

func (rq *RequestQueue) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*rq = append(*rq, x.(RequestQueueItem))
}

func (rq *RequestQueue) Pop() interface{} {
	old := *rq
	n := len(old)
	x := old[n-1]
	*rq = old[0 : n-1]
	return x
}

// BodyRequest is a sketch of the request for block bodies, meaning that access to the database is required to convert it to the actual BlockBodies request (look up hashes of canonical blocks)
type BodyRequest struct {
	BlockNums []uint64
	Hashes    []common.Hash
	requested *roaring64.Bitmap
}

// NewBodyDownload create a new body download state object
func NewBodyDownload(outstandingLimit int) *BodyDownload {
	bd := &BodyDownload{
		required:          roaring64.New(),
		requested:         roaring64.New(),
		delivered:         roaring64.New(),
		requestedMap:      make(map[DoubleHash]uint64),
		outstandingLimit:  uint64(outstandingLimit),
		deliveries:        make([]*types.Block, outstandingLimit+MaxBodiesInRequest),
		RequestQueueTimer: time.NewTimer(time.Hour),
		expirationList:    list.New(),
		requestQueue:      make([]RequestQueueItem, 0, 64),
	}
	heap.Init(&bd.requestQueue)
	return bd
}
