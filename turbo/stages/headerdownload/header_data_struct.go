package headerdownload

import (
	"container/heap"
	"fmt"
	"math/big"
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/engineapi"
)

type QueueID uint8

const (
	NoQueue QueueID = iota
	EntryQueueID
	VerifyQueueID
	InsertQueueID
	PersistedQueueID
)

// Link is a chain link that can be connect to other chain links
// For a given link, parent link can be found by hd.links[link.header.ParentHash], and child links by link.next (there may be more than one child in case of forks)
// Links encapsule block headers
// Links can be either persistent or not. Persistent links encapsule headers that have already been saved to the database, but these links are still
// present to allow potential reorgs
type Link struct {
	header      *types.Header
	headerRaw   []byte
	next        []*Link     // Allows iteration over links in ascending block height order
	hash        common.Hash // Hash of the header
	blockHeight uint64
	persisted   bool    // Whether this link comes from the database record
	verified    bool    // Ancestor of pre-verified header or verified by consensus engine
	idx         int     // Index in the heap
	queueId     QueueID // which queue this link belongs to
}

// LinkQueue is the priority queue of links. It is instantiated once for persistent links, and once for non-persistent links
// In other instances, it is used to limit number of links of corresponding type (persistent and non-persistent) in memory
type LinkQueue []*Link

// Len (part of heap.Interface) returns the current size of the link queue
func (lq LinkQueue) Len() int {
	return len(lq)
}

// Less (part of heap.Interface) compares two links. For persisted links, those with the lower block heights get evicted first. This means that more recently persisted links are preferred.
// For non-persisted links, those with the highest block heights get evicted first. This is to prevent "holes" in the block heights that may cause inability to
// insert headers in the ascending order of their block heights.
func (lq LinkQueue) Less(i, j int) bool {
	if lq[i].persisted {
		return lq[i].blockHeight < lq[j].blockHeight
	}
	return lq[i].blockHeight > lq[j].blockHeight
}

// Swap (part of heap.Interface) moves two links in the queue into each other's places. Note that each link has idx attribute that is getting adjusted during
// the swap. The idx attribute allows the removal of links from the middle of the queue (in case if links are getting invalidated due to
// failed verification of unavailability of parent headers)
func (lq LinkQueue) Swap(i, j int) {
	lq[i].idx = j
	lq[j].idx = i
	lq[i], lq[j] = lq[j], lq[i]
}

// Push (part of heap.Interface) places a new link onto the end of queue. Note that idx attribute is set to the correct position of the new link
func (lq *LinkQueue) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	l := x.(*Link)
	l.idx = len(*lq)
	*lq = append(*lq, l)
}

// Pop (part of heap.Interface) removes the first link from the queue
func (lq *LinkQueue) Pop() interface{} {
	old := *lq
	n := len(old)
	x := old[n-1]
	x.idx = -1
	x.queueId = NoQueue
	*lq = old[0 : n-1]
	return x
}

// Anchor represents a header that we do not yet have, but that we would like to have soon
// anchors are created when we know the hash of the header (from its children), and we can
// also derive its blockheight (also from its children), but the corresponding header
// either has not been requested yet, or has not been delivered yet.
// It is possible for one anchor to reference multiple links, because more than one
// header may share the same parent header. In such cases, `links` field will contain
// more than one pointer.
type Anchor struct {
	peerID        [64]byte
	links         []*Link     // Links attached immediately to this anchor
	parentHash    common.Hash // Hash of the header this anchor can be connected to (to disappear)
	blockHeight   uint64
	nextRetryTime uint64 // Zero when anchor has just been created, otherwise time when anchor needs to be check to see if retry is needed
	timeouts      int    // Number of timeout that this anchor has experiences - after certain threshold, it gets invalidated
	idx           int    // Index of the anchor in the queue to be able to modify specific items
}

// AnchorQueue is a priority queue of anchors that priorises by the time when
// another retry on the extending the anchor needs to be attempted
// Every time anchor's extension is requested, the `nextRetryTime` is reset
// to 5 seconds in the future, and when it expires, and the anchor is still
// retry is made
// It implement heap.Interface to be useable by the standard library `heap`
// as a priority queue (implemented as a binary heap)
// As anchors are moved around in the binary heap, they internally track their
// position in the heap (using `idx` field). This feature allows updating
// the heap (using `Fix` function) in situations when anchor is accessed not
// through the priority queue, but through the map `anchor` in the
// HeaderDownloader type.
type AnchorQueue []*Anchor

func (aq AnchorQueue) Len() int {
	return len(aq)
}

func (aq AnchorQueue) Less(i, j int) bool {
	if aq[i].nextRetryTime == aq[j].nextRetryTime {
		// When next retry times are the same, we prioritise low block height anchors
		return aq[i].blockHeight < aq[j].blockHeight
	}
	return aq[i].nextRetryTime < aq[j].nextRetryTime
}

func (aq AnchorQueue) Swap(i, j int) {
	aq[i], aq[j] = aq[j], aq[i]
	aq[i].idx, aq[j].idx = i, j // Restore indices after the swap
}

func (aq *AnchorQueue) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	x.(*Anchor).idx = len(*aq)
	*aq = append(*aq, x.(*Anchor))
}

func (aq *AnchorQueue) Pop() interface{} {
	old := *aq
	n := len(old)
	x := old[n-1]
	*aq = old[0 : n-1]
	return x
}

type ChainSegmentHeader struct {
	HeaderRaw rlp.RawValue
	Header    *types.Header
	Hash      common.Hash
	Number    uint64
}

// First item in ChainSegment is the anchor
// ChainSegment must be contigous and must not include bad headers
type ChainSegment []ChainSegmentHeader

type PeerHandle int // This is int just for the PoC phase - will be replaced by more appropriate type to find a peer

type Penalty int

const (
	NoPenalty Penalty = iota
	BadBlockPenalty
	DuplicateHeaderPenalty
	WrongChildBlockHeightPenalty
	WrongChildDifficultyPenalty
	InvalidSealPenalty
	TooFarFuturePenalty
	TooFarPastPenalty
	AbandonedAnchorPenalty
)

type PeerPenalty struct {
	// This type may also contain the "severity" of penalty, if we find that it helps
	err        error // Underlying error if available
	penalty    Penalty
	peerHandle PeerHandle
}

// Request for chain segment starting with hash and going to its parent, etc, with length headers in total
type HeaderRequest struct {
	Hash    common.Hash
	Number  uint64
	Length  uint64
	Skip    uint64
	Reverse bool
	Anchor  *Anchor
}

type PenaltyItem struct {
	PeerID  [64]byte
	Penalty Penalty
}
type Announce struct {
	Hash   common.Hash
	Number uint64
}

type VerifySealFunc func(header *types.Header) error
type CalcDifficultyFunc func(childTimestamp uint64, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentHash, parentUncleHash common.Hash) *big.Int

// InsertQueue keeps the links before they are inserted in the database
// It priorities them by block height (the lowest block height on the top),
// and if block heights are the same, by the verification status (verified/preverified on the top)
type InsertQueue []*Link

func (iq InsertQueue) Len() int {
	return len(iq)
}

func (iq InsertQueue) Less(i, j int) bool {
	return iq[i].blockHeight < iq[j].blockHeight
}

func (iq InsertQueue) Swap(i, j int) {
	iq[i], iq[j] = iq[j], iq[i]
	iq[i].idx, iq[j].idx = i, j // Restore indices after the swap
}

func (iq *InsertQueue) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	x.(*Link).idx = len(*iq)
	*iq = append(*iq, x.(*Link))
}

func (iq *InsertQueue) Pop() interface{} {
	old := *iq
	n := len(old)
	x := old[n-1]
	*iq = old[0 : n-1]
	x.idx = -1
	x.queueId = NoQueue
	return x
}

type SyncStatus int

const ( // SyncStatus values
	Idle = iota
	Syncing
	Synced // if we found a canonical hash during backward sync, in this case our sync process is done
)

type HeaderDownload struct {
	badHeaders         map[common.Hash]struct{}
	anchors            map[common.Hash]*Anchor  // Mapping from parentHash to collection of anchors
	preverifiedHashes  map[common.Hash]struct{} // Set of hashes that are known to belong to canonical chain
	links              map[common.Hash]*Link    // Links by header hash
	engine             consensus.Engine
	verifyQueue        InsertQueue    // Priority queue of non-peristed links ready for verification
	insertQueue        InsertQueue    // Priority queue of non-persisted links that are verified and can be inserted
	seenAnnounces      *SeenAnnounces // External announcement hashes, after header verification if hash is in this set - will broadcast it further
	persistedLinkQueue LinkQueue      // Priority queue of persisted links used to limit their number
	linkQueue          LinkQueue      // Priority queue of non-persisted links used to limit their number
	anchorQueue        *AnchorQueue   // Priority queue of anchors used to sequence the header requests
	DeliveryNotify     chan struct{}
	SkipCycleHack      chan struct{} // devenet will signal to this channel to skip sync cycle and release write db transaction. It's temporary solution - later we will do mining without write transaction.
	toAnnounce         []Announce
	lock               sync.RWMutex
	preverifiedHeight  uint64 // Block height corresponding to the last preverified hash
	linkLimit          int    // Maximum allowed number of links
	persistedLinkLimit int    // Maximum allowed number of persisted links
	anchorLimit        int    // Maximum allowed number of anchors
	highestInDb        uint64 // Height of the highest block header in the database
	requestChaining    bool   // Whether the downloader is allowed to issue more requests when previous responses created or moved an anchor
	fetchingNew        bool   // Set when the stage that is actively fetching the headers is in progress
	topSeenHeightPoW   uint64

	consensusHeaderReader consensus.ChainHeaderReader
	headerReader          interfaces.HeaderReader

	// Proof of Stake (PoS)
	topSeenHeightPoS     uint64
	requestId            int
	posAnchor            *Anchor
	posStatus            SyncStatus
	posSync              bool                          // Whether the chain is syncing in the PoS mode
	headersCollector     *etl.Collector                // ETL collector for headers
	BeaconRequestList    *engineapi.RequestList        // Requests from ethbackend to staged sync
	PayloadStatusCh      chan privateapi.PayloadStatus // Responses (validation/execution status)
	pendingPayloadStatus common.Hash                   // Header whose status we still should send to PayloadStatusCh
	unsettledForkChoice  *engineapi.ForkChoiceMessage  // Forkchoice to process after unwind
	unsettledHeadHeight  uint64                        // Height of unsettledForkChoice.headBlockHash
}

// HeaderRecord encapsulates two forms of the same header - raw RLP encoding (to avoid duplicated decodings and encodings), and parsed value types.Header
type HeaderRecord struct {
	Header *types.Header
	Raw    []byte
}

func NewHeaderDownload(
	anchorLimit int,
	linkLimit int,
	engine consensus.Engine,
	headerReader interfaces.HeaderAndCanonicalReader,
) *HeaderDownload {
	persistentLinkLimit := linkLimit / 16
	hd := &HeaderDownload{
		badHeaders:         make(map[common.Hash]struct{}),
		anchors:            make(map[common.Hash]*Anchor),
		persistedLinkLimit: persistentLinkLimit,
		linkLimit:          linkLimit - persistentLinkLimit,
		anchorLimit:        anchorLimit,
		engine:             engine,
		preverifiedHashes:  make(map[common.Hash]struct{}),
		links:              make(map[common.Hash]*Link),
		anchorQueue:        &AnchorQueue{},
		seenAnnounces:      NewSeenAnnounces(),
		DeliveryNotify:     make(chan struct{}, 1),
		SkipCycleHack:      make(chan struct{}),
		BeaconRequestList:  engineapi.NewRequestList(),
		PayloadStatusCh:    make(chan privateapi.PayloadStatus, 1),
		headerReader:       headerReader,
	}
	heap.Init(&hd.persistedLinkQueue)
	heap.Init(&hd.linkQueue)
	heap.Init(hd.anchorQueue)
	heap.Init(&hd.verifyQueue)
	heap.Init(&hd.insertQueue)
	return hd
}

func (p Penalty) String() string {
	switch p {
	case NoPenalty:
		return "None"
	case BadBlockPenalty:
		return "BadBlock"
	case DuplicateHeaderPenalty:
		return "DuplicateHeader"
	case WrongChildBlockHeightPenalty:
		return "WrongChildBlockHeight"
	case WrongChildDifficultyPenalty:
		return "WrongChildDifficulty"
	case InvalidSealPenalty:
		return "InvalidSeal"
	case TooFarFuturePenalty:
		return "TooFarFuture"
	case TooFarPastPenalty:
		return "TooFarPast"
	default:
		return fmt.Sprintf("Unknown(%d)", p)
	}
}

func (pp PeerPenalty) String() string {
	return fmt.Sprintf("peerPenalty{peer: %d, penalty: %s, err: %v}", pp.peerHandle, pp.penalty, pp.err)
}

func (hd *HeaderDownload) moveLinkToQueue(link *Link, queueId QueueID) {
	if link.queueId == queueId {
		return
	}
	// Remove
	switch link.queueId {
	case NoQueue:
	case EntryQueueID:
		heap.Remove(&hd.linkQueue, link.idx)
	case VerifyQueueID:
		heap.Remove(&hd.verifyQueue, link.idx)
	case InsertQueueID:
		heap.Remove(&hd.insertQueue, link.idx)
	case PersistedQueueID:
		heap.Remove(&hd.persistedLinkQueue, link.idx)
	}
	// Push
	switch queueId {
	case NoQueue:
	case EntryQueueID:
		heap.Push(&hd.linkQueue, link)
	case VerifyQueueID:
		heap.Push(&hd.verifyQueue, link)
	case InsertQueueID:
		heap.Push(&hd.insertQueue, link)
	case PersistedQueueID:
		heap.Push(&hd.persistedLinkQueue, link)
	}
	link.queueId = queueId
}

// HeaderInserter encapsulates necessary variable for inserting header records to the database, abstracting away the source of these headers
// The headers are "fed" by repeatedly calling the FeedHeader function.
type HeaderInserter struct {
	localTd          *big.Int
	logPrefix        string
	prevHash         common.Hash // Hash of previously seen header - to filter out potential duplicates
	highestHash      common.Hash
	newCanonical     bool
	unwind           bool
	unwindPoint      uint64
	highest          uint64
	highestTimestamp uint64
	canonicalCache   *lru.Cache
	headerReader     interfaces.HeaderAndCanonicalReader
}

func NewHeaderInserter(logPrefix string, localTd *big.Int, headerProgress uint64, headerReader interfaces.HeaderAndCanonicalReader) *HeaderInserter {
	hi := &HeaderInserter{
		logPrefix:    logPrefix,
		localTd:      localTd,
		unwindPoint:  headerProgress,
		headerReader: headerReader,
	}
	hi.canonicalCache, _ = lru.New(1000)
	return hi
}

// SeenAnnounces - external announcement hashes, after header verification if hash is in this set - will broadcast it further
type SeenAnnounces struct {
	hashes *lru.Cache
}

func NewSeenAnnounces() *SeenAnnounces {
	cache, err := lru.New(1000)
	if err != nil {
		panic("error creating prefetching cache for blocks")
	}
	return &SeenAnnounces{hashes: cache}
}

func (s *SeenAnnounces) Pop(hash common.Hash) bool {
	_, ok := s.hashes.Get(hash)
	if ok {
		s.hashes.Remove(hash)
	}
	return ok
}

func (s SeenAnnounces) Seen(hash common.Hash) bool {
	_, ok := s.hashes.Get(hash)
	return ok
}

func (s *SeenAnnounces) Add(b common.Hash) {
	s.hashes.ContainsOrAdd(b, struct{}{})
}
