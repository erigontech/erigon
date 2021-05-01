package headerdownload

import (
	"container/heap"
	"fmt"
	"math/big"
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// Link is a chain link that can be connect to other chain links
// For a given link, parent link can be found by hd.links[link.header.ParentHash], and child links by link.next (there may be more than one child in case of forks)
// Links encapsule block headers
// Links can be either persistent or not. Persistent links encapsule headers that have already been saved to the database, but these links are still
// present to allow potential reorgs
type Link struct {
	blockHeight uint64
	header      *types.Header
	hash        common.Hash // Hash of the header
	next        []*Link     // Allows iteration over links in ascending block height order
	persisted   bool        // Whether this link comes from the database record
	preverified bool        // Ancestor of pre-verified header
	idx         int         // Index in the heap
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
	if (*lq[i]).persisted {
		return (*lq[i]).blockHeight < (*lq[j]).blockHeight
	}
	return (*lq[i]).blockHeight > (*lq[j]).blockHeight
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
	*lq = old[0 : n-1]
	return x
}

type Anchor struct {
	parentHash  common.Hash // Hash of the header this anchor can be connected to (to disappear)
	blockHeight uint64
	timestamp   uint64  // Zero when anchor has just been created, otherwise timestamps when timeout on this anchor request expires
	timeouts    int     // Number of timeout that this anchor has experiences - after certain threshold, it gets invalidated
	links       []*Link // Links attached immediately to this anchor
}

type AnchorQueue []*Anchor

func (aq AnchorQueue) Len() int {
	return len(aq)
}

func (aq AnchorQueue) Less(i, j int) bool {
	if (*aq[i]).timestamp == (*aq[j]).timestamp {
		// When timestamps are the same, we prioritise low block height anchors
		return (*aq[i]).blockHeight < (*aq[j]).blockHeight
	}
	return (*aq[i]).timestamp < (*aq[j]).timestamp
}

func (aq AnchorQueue) Swap(i, j int) {
	aq[i], aq[j] = aq[j], aq[i]
}

func (aq *AnchorQueue) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*aq = append(*aq, x.(*Anchor))
}

func (aq *AnchorQueue) Pop() interface{} {
	old := *aq
	n := len(old)
	x := old[n-1]
	*aq = old[0 : n-1]
	return x
}

// First item in ChainSegment is the anchor
// ChainSegment must be contigous and must not include bad headers
type ChainSegment struct {
	HeadersRaw [][]byte
	Headers    []*types.Header
}

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
)

type PeerPenalty struct {
	// This type may also contain the "severity" of penalty, if we find that it helps
	peerHandle PeerHandle
	penalty    Penalty
	err        error // Underlying error if available
}

// Request for chain segment starting with hash and going to its parent, etc, with length headers in total
type HeaderRequest struct {
	Hash    common.Hash
	Number  uint64
	Length  uint64
	Skip    uint64
	Reverse bool
}

type Announce struct {
	Hash   common.Hash
	Number uint64
}

type VerifySealFunc func(header *types.Header) error
type CalcDifficultyFunc func(childTimestamp uint64, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentHash, parentUncleHash common.Hash) *big.Int

type HeaderDownload struct {
	lock               sync.RWMutex
	badHeaders         map[common.Hash]struct{}
	anchors            map[common.Hash]*Anchor  // Mapping from parentHash to collection of anchors
	preverifiedHashes  map[common.Hash]struct{} // Set of hashes that are known to belong to canonical chain
	preverifiedHeight  uint64                   // Block height corresponding to the last preverified hash
	links              map[common.Hash]*Link    // Links by header hash
	linkLimit          int                      // Maximum allowed number of links
	persistedLinkLimit int                      // Maximum allowed number of persisted links
	anchorLimit        int                      // Maximum allowed number of anchors
	engine             consensus.Engine
	headerReader       consensus.ChainHeaderReader
	highestInDb        uint64 // Height of the highest block header in the database
	stageReady         bool
	stageReadyCh       chan struct{}
	stageHeight        uint64
	topSeenHeight      uint64
	insertList         []*Link        // List of non-persisted links that can be inserted (their parent is persisted)
	seenAnnounces      *SeenAnnounces // External announcement hashes, after header verification if hash is in this set - will broadcast it further
	toAnnounce         []Announce
	persistedLinkQueue *LinkQueue   // Priority queue of persisted links used to limit their number
	linkQueue          *LinkQueue   // Priority queue of non-persisted links used to limit their number
	anchorQueue        *AnchorQueue // Priority queue of anchors used to sequence the header requests
}

// HeaderRecord encapsulates two forms of the same header - raw RLP encoding (to avoid duplicated decodings and encodings), and parsed value types.Header
type HeaderRecord struct {
	Raw    []byte
	Header *types.Header
}

func NewHeaderDownload(
	anchorLimit int,
	linkLimit int,
	engine consensus.Engine,
) *HeaderDownload {
	hd := &HeaderDownload{
		badHeaders:         make(map[common.Hash]struct{}),
		anchors:            make(map[common.Hash]*Anchor),
		persistedLinkLimit: linkLimit / 2,
		linkLimit:          linkLimit / 2,
		anchorLimit:        anchorLimit,
		engine:             engine,
		preverifiedHashes:  make(map[common.Hash]struct{}),
		links:              make(map[common.Hash]*Link),
		stageReadyCh:       make(chan struct{}, 1), // channel needs to have capacity at least 1, so that the signal is not lost
		persistedLinkQueue: &LinkQueue{},
		linkQueue:          &LinkQueue{},
		anchorQueue:        &AnchorQueue{},
		seenAnnounces:      NewSeenAnnounces(),
	}
	heap.Init(hd.persistedLinkQueue)
	heap.Init(hd.linkQueue)
	heap.Init(hd.anchorQueue)
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

// HeaderInserter incapsulates necessary variable for inserting header records to the database, abstracting away the source of these headers
// The headers are "fed" by repeatedly calling the FeedHeader function.
type HeaderInserter struct {
	logPrefix      string
	batch          ethdb.DbWithPendingMutations
	prevHash       common.Hash // Hash of previously seen header - to filter out potential duplicates
	prevHeight     uint64
	newCanonical   bool
	unwindPoint    uint64
	highest        uint64
	highestHash    common.Hash
	localTd        *big.Int
	headerProgress uint64
}

func NewHeaderInserter(logPrefix string, batch ethdb.DbWithPendingMutations, localTd *big.Int, headerProgress uint64) *HeaderInserter {
	return &HeaderInserter{
		logPrefix:      logPrefix,
		batch:          batch,
		localTd:        localTd,
		headerProgress: headerProgress,
		unwindPoint:    headerProgress,
	}
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

func (s *SeenAnnounces) Add(b common.Hash) {
	s.hashes.ContainsOrAdd(b, struct{}{})
}
