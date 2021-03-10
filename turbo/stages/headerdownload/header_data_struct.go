package headerdownload

import (
	"container/heap"
	"fmt"
	"math/big"
	"sync"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

type Tip struct {
	blockHeight uint64
	header      *types.Header
	hash        common.Hash // Hash of the header
	next        []*Tip      // Allows iteration over tips in ascending block height order
	persisted   bool        // Whether this tip comes from the database record
	preverified bool        // Ancestor of pre-verified header
	idx         int         // Index in the heap
}

// TipQueue is the priority queue of persistent tips that exist to limit number of persistent tips
type TipQueue []*Tip

func (tq TipQueue) Len() int {
	return len(tq)
}

func (tq TipQueue) Less(i, j int) bool {
	if (*tq[i]).persisted {
		return (*tq[i]).blockHeight < (*tq[j]).blockHeight
	}
	return (*tq[i]).blockHeight > (*tq[j]).blockHeight
}

func (tq TipQueue) Swap(i, j int) {
	tq[i].idx = j
	tq[j].idx = i
	tq[i], tq[j] = tq[j], tq[i]
}

func (tq *TipQueue) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	t := x.(*Tip)
	t.idx = len(*tq)
	*tq = append(*tq, t)
}

func (tq *TipQueue) Pop() interface{} {
	old := *tq
	n := len(old)
	x := old[n-1]
	*tq = old[0 : n-1]
	return x
}

type Anchor struct {
	parentHash  common.Hash // Hash of the header this anchor can be connected to (to disappear)
	blockHeight uint64
	timestamp   uint64 // Zero when anchor has just been created, otherwise timestamps when timeout on this anchor request expires
	timeouts    int    // Number of timeout that this anchor has experiences - after certain threshold, it gets invalidated
	tips        []*Tip // Tips attached immediately to this anchor
}

type AnchorQueue []*Anchor

func (aq AnchorQueue) Len() int {
	return len(aq)
}

func (aq AnchorQueue) Less(i, j int) bool {
	if (*aq[i]).timestamp == (*&aq[j]).timestamp {
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

type VerifySealFunc func(header *types.Header) error
type CalcDifficultyFunc func(childTimestamp uint64, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentHash, parentUncleHash common.Hash) *big.Int

type HeaderDownload struct {
	lock                   sync.RWMutex
	bufferLimit            int
	filesDir               string
	files                  []string
	badHeaders             map[common.Hash]struct{}
	anchors                map[common.Hash]*Anchor      // Mapping from parentHash to collection of anchors
	hardTips               map[common.Hash]HeaderRecord // Set of hashes for hard-coded tips
	maxHardTipHeight       uint64
	tips                   map[common.Hash]*Tip // Tips by tip hash
	linkLimit              int                  // Maximum allowed number of links
	persistedLinkLimit     int                  // Maximum allowed number of persisted tips
	anchorLimit            int                  // Maximum allowed number of anchors
	highestTotalDifficulty uint256.Int
	calcDifficultyFunc     CalcDifficultyFunc
	verifySealFunc         VerifySealFunc
	highestInDb            uint64 // Height of the highest block header in the database
	initialHash            common.Hash
	stageReady             bool
	stageReadyCh           chan struct{}
	stageHeight            uint64
	topSeenHeight          uint64
	insertList             []*Tip       // List of non-persisted tips that can be inserted (their parent is persisted)
	persistedLinkQueue     *TipQueue    // Priority queue of persistent tips used to limit their number
	linkQueue              *TipQueue    // Priority queue of non-persistent tip used to limit their number
	anchorQueue            *AnchorQueue // Priority queue of anchor used to sequence the header requests
}

// HeaderRecord encapsulates two forms of the same header - raw RLP encoding (to avoid duplicated decodings and encodings), and parsed value types.Header
type HeaderRecord struct {
	Raw    []byte
	Header *types.Header
}

func NewHeaderDownload(
	anchorLimit int,
	linkLimit int,
	calcDifficultyFunc CalcDifficultyFunc,
	verifySealFunc VerifySealFunc,
) *HeaderDownload {
	hd := &HeaderDownload{
		badHeaders:         make(map[common.Hash]struct{}),
		anchors:            make(map[common.Hash]*Anchor),
		persistedLinkLimit: linkLimit / 2,
		linkLimit:          linkLimit / 2,
		anchorLimit:        anchorLimit,
		calcDifficultyFunc: calcDifficultyFunc,
		verifySealFunc:     verifySealFunc,
		hardTips:           make(map[common.Hash]HeaderRecord),
		tips:               make(map[common.Hash]*Tip),
		stageReadyCh:       make(chan struct{}, 1), // channel needs to have capacity at least 1, so that the signal is not lost
		persistedLinkQueue: &TipQueue{},
		linkQueue:          &TipQueue{},
		anchorQueue:        &AnchorQueue{},
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
	tx             ethdb.DbWithPendingMutations
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

func NewHeaderInserter(logPrefix string, tx, batch ethdb.DbWithPendingMutations, localTd *big.Int, headerProgress uint64) *HeaderInserter {
	return &HeaderInserter{
		logPrefix:      logPrefix,
		tx:             tx,
		batch:          batch,
		localTd:        localTd,
		headerProgress: headerProgress,
		unwindPoint:    headerProgress,
	}
}
