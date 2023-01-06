package bodydownload

import (
	"github.com/RoaringBitmap/roaring/roaring64"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
)

// TripleHash is type to be used for the mapping between TxHash, UncleHash, and WithdrawalsHash to the block header
type TripleHash [3 * common.HashLength]byte

const MaxBodiesInRequest = 1024

type Delivery struct {
	peerID          [64]byte
	txs             [][][]byte
	uncles          [][]*types.Header
	withdrawals     []types.Withdrawals
	lenOfP2PMessage uint64
}

// BodyDownload represents the state of body downloading process
type BodyDownload struct {
	peerMap          map[[64]byte]int
	requestedMap     map[TripleHash]uint64
	DeliveryNotify   chan struct{}
	deliveryCh       chan Delivery
	Engine           consensus.Engine
	delivered        *roaring64.Bitmap
	prefetchedBlocks *PrefetchedBlocks
	deliveriesH      map[uint64]*types.Header
	requests         map[uint64]*BodyRequest
	maxProgress      uint64
	requestedLow     uint64 // Lower bound of block number for outstanding requests
	requestHigh      uint64
	lowWaitUntil     uint64 // Time to wait for before starting the next round request from requestedLow
	outstandingLimit uint64 // Limit of number of outstanding blocks for body requests
	deliveredCount   float64
	wastedCount      float64
	bodiesAdded      bool
	bodyCache        map[uint64]*types.RawBody
	UsingExternalTx  bool
}

// BodyRequest is a sketch of the request for block bodies, meaning that access to the database is required to convert it to the actual BlockBodies request (look up hashes of canonical blocks)
type BodyRequest struct {
	BlockNums []uint64
	Hashes    []common.Hash
	peerID    [64]byte
	waitUntil uint64
}

// NewBodyDownload create a new body download state object
func NewBodyDownload(outstandingLimit int, engine consensus.Engine) *BodyDownload {
	bd := &BodyDownload{
		requestedMap:     make(map[TripleHash]uint64),
		outstandingLimit: uint64(outstandingLimit),
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
		// between delivery and collections. since we assume that there will be
		// no more than `outstandingLimit+MaxBodiesInRequest` requested
		// deliveris, this is a good number for the channel capacity
		deliveryCh: make(chan Delivery, outstandingLimit+MaxBodiesInRequest),
		Engine:     engine,
		bodyCache:  make(map[uint64]*types.RawBody),
	}
	return bd
}
