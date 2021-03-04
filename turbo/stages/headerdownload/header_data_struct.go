package headerdownload

import (
	"container/list"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/petar/GoLLRB/llrb"
)

// AnchorTipItem is element of the priority queue of tips belonging to an anchor
// This queue is prioritised by block heights, lowest block height being first out
type AnchorTipItem struct {
	hash   common.Hash
	height uint64
	hard   bool // Whether the tip is hard coded
}

type AnchorTipQueue []AnchorTipItem

func (atq AnchorTipQueue) Len() int {
	return len(atq)
}

func (atq AnchorTipQueue) Less(i, j int) bool {
	if atq[i].hard == atq[j].hard {
		return atq[i].height < atq[j].height
	}
	return !atq[i].hard
}

func (atq AnchorTipQueue) Swap(i, j int) {
	atq[i], atq[j] = atq[j], atq[i]
}

func (atq *AnchorTipQueue) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*atq = append(*atq, x.(AnchorTipItem))
}

func (atq *AnchorTipQueue) Pop() interface{} {
	old := *atq
	n := len(old)
	x := old[n-1]
	*atq = old[0 : n-1]
	return x
}

type Anchor struct {
	hardCoded    bool // Whether this anchor originated from a hard-coded header
	tipQueue     *AnchorTipQueue
	difficulty   uint256.Int
	parentHash   common.Hash
	hash         common.Hash
	blockHeight  uint64
	timestamp    uint64
	maxTipHeight uint64 // Maximum value of `blockHeight` of all tips associated with this anchor
	anchorID     int    // Unique ID of this anchor to be able to find it in the balanced tree
}

// For placing anchors into the sorting tree
func (a *Anchor) Less(bi llrb.Item) bool {
	b := bi.(*Anchor)
	if a.tipStretch() == b.tipStretch() {
		return a.anchorID < b.anchorID
	}
	return a.tipStretch() > b.tipStretch()
}

func (a *Anchor) tipStretch() uint64 {
	if a.tipQueue.Len() == 0 {
		return 0
	}
	return a.maxTipHeight - (*a.tipQueue)[0].height
}

type Tip struct {
	anchor               *Anchor
	cumulativeDifficulty uint256.Int
	timestamp            uint64
	difficulty           uint256.Int
	blockHeight          uint64
	uncleHash            common.Hash
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
	Hash   common.Hash
	Number uint64
	Length int
}

type VerifySealFunc func(header *types.Header) error
type CalcDifficultyFunc func(childTimestamp uint64, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentHash, parentUncleHash common.Hash) *big.Int

type HeaderBuffer struct {
	buffer       []byte
	heights      []uint64
	startOffsets []int
	endOffsets   []int
	idx, pos     int
}

func (hb *HeaderBuffer) AddHeader(headerRaw []byte, height uint64) {
	hb.startOffsets = append(hb.startOffsets, len(hb.buffer))
	hb.buffer = append(hb.buffer, headerRaw...)
	hb.heights = append(hb.heights, height)
	hb.endOffsets = append(hb.endOffsets, len(hb.buffer))
}

func (hb *HeaderBuffer) Len() int {
	return len(hb.heights)
}

func (hb *HeaderBuffer) Less(i, j int) bool {
	return hb.heights[i] < hb.heights[j]
}

func (hb *HeaderBuffer) Swap(i, j int) {
	hb.heights[i], hb.heights[j] = hb.heights[j], hb.heights[i]
	hb.startOffsets[i], hb.startOffsets[j] = hb.startOffsets[j], hb.startOffsets[i]
	hb.endOffsets[i], hb.endOffsets[j] = hb.endOffsets[j], hb.endOffsets[i]
}

func (hb *HeaderBuffer) Flush(w io.Writer) error {
	for i, startOffset := range hb.startOffsets {
		endOffset := hb.endOffsets[i]
		if _, err := w.Write(hb.buffer[startOffset:endOffset]); err != nil {
			return err
		}
	}
	hb.Clear()
	return nil
}

func (hb *HeaderBuffer) Read(p []byte) (int, error) {
	if hb.idx >= len(hb.startOffsets) {
		return 0, io.EOF
	}
	var n int
	for hb.idx < len(hb.startOffsets) && hb.endOffsets[hb.idx]-hb.startOffsets[hb.idx]-hb.pos <= len(p)-n {
		copy(p[n:], hb.buffer[hb.startOffsets[hb.idx]+hb.pos:hb.endOffsets[hb.idx]])
		n += hb.endOffsets[hb.idx] - hb.startOffsets[hb.idx] - hb.pos
		hb.idx++
		hb.pos = 0
	}
	if hb.idx < len(hb.startOffsets) {
		copy(p[n:], hb.buffer[hb.startOffsets[hb.idx]+hb.pos:hb.endOffsets[hb.idx]])
		hb.pos += len(p) - n
		n = len(p)
	}
	return n, nil
}

func (hb *HeaderBuffer) Clear() {
	hb.buffer = hb.buffer[:0]
	hb.heights = hb.heights[:0]
	hb.startOffsets = hb.startOffsets[:0]
	hb.endOffsets = hb.endOffsets[:0]
	hb.idx = 0
	hb.pos = 0
}

func (hb *HeaderBuffer) IsEmpty() bool {
	return len(hb.buffer) == 0
}

type HeaderDownload struct {
	lock                   sync.RWMutex
	buffer                 *HeaderBuffer
	anotherBuffer          *HeaderBuffer
	bufferLimit            int
	filesDir               string
	files                  []string
	badHeaders             map[common.Hash]struct{}
	anchors                map[common.Hash][]*Anchor // Mapping from parentHash to collection of anchors
	anchorTree             *llrb.LLRB                // Balanced tree of anchors sorted by tip stretch (longest stretch first)
	nextAnchorID           int
	hardTips               map[common.Hash]HeaderRecord // Set of hashes for hard-coded tips
	maxHardTipHeight       uint64
	tips                   map[common.Hash]*Tip // Tips by tip hash
	tipCount               int                  // Total number of tips associated to all anchors
	tipLimit               int                  // Maximum allowed number of tips
	initPowDepth           int                  // powDepth assigned to the newly inserted anchor
	newAnchorFutureLimit   uint64               // How far in the future (relative to current time) the new anchors are allowed to be
	newAnchorPastLimit     uint64               // How far in the past (relative to current time) the new anchors are allowed to be
	highestTotalDifficulty uint256.Int
	requestQueue           *list.List
	calcDifficultyFunc     CalcDifficultyFunc
	verifySealFunc         VerifySealFunc
	RequestQueueTimer      *time.Timer
	highestInDb            uint64 // Height of the highest block header in the database
	initialHash            common.Hash
	stageReady             bool
	stageReadyCh           chan struct{}
	stageHeight            uint64
	headersAdded           int
}

// HeaderRecord encapsulates two forms of the same header - raw RLP encoding (to avoid duplicated decodings and encodings), and parsed value types.Header
type HeaderRecord struct {
	Raw    []byte
	Header *types.Header
}

type TipQueueItem struct {
	tip     *Tip
	tipHash common.Hash
}

type RequestQueueItem struct {
	anchorParent common.Hash
	waitUntil    uint64
}

type RequestQueue []RequestQueueItem

func (rq RequestQueue) Len() int {
	return len(rq)
}

func (rq RequestQueue) Less(i, j int) bool {
	return rq[i].waitUntil < rq[j].waitUntil
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

func NewHeaderDownload(
	initialHash common.Hash,
	filesDir string,
	bufferLimit, tipLimit, initPowDepth int,
	calcDifficultyFunc CalcDifficultyFunc,
	verifySealFunc VerifySealFunc,
	newAnchorFutureLimit, newAnchorPastLimit uint64,
) *HeaderDownload {
	hd := &HeaderDownload{
		initialHash:          initialHash,
		filesDir:             filesDir,
		buffer:               &HeaderBuffer{},
		anotherBuffer:        &HeaderBuffer{},
		bufferLimit:          bufferLimit,
		badHeaders:           make(map[common.Hash]struct{}),
		anchors:              make(map[common.Hash][]*Anchor),
		tipLimit:             tipLimit,
		initPowDepth:         initPowDepth,
		requestQueue:         list.New(),
		anchorTree:           llrb.New(),
		calcDifficultyFunc:   calcDifficultyFunc,
		verifySealFunc:       verifySealFunc,
		newAnchorFutureLimit: newAnchorFutureLimit,
		newAnchorPastLimit:   newAnchorPastLimit,
		hardTips:             make(map[common.Hash]HeaderRecord),
		tips:                 make(map[common.Hash]*Tip),
		stageReadyCh:         make(chan struct{}),
		RequestQueueTimer:    time.NewTimer(time.Hour),
	}
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
	logPrefix          string
	tx                 ethdb.DbWithPendingMutations
	batch              ethdb.DbWithPendingMutations
	prevHash           common.Hash // Hash of previously seen header - to filter out potential duplicates
	prevHeight         uint64
	newCanonical       bool
	unwindPoint        uint64
	canonicalBacktrack map[common.Hash]common.Hash
	parentDiffs        map[common.Hash]*big.Int
	childDiffs         map[common.Hash]*big.Int
	highest            uint64
	localTd            *big.Int
	headerProgress     uint64
}

func NewHeaderInserter(logPrefix string, tx, batch ethdb.DbWithPendingMutations, localTd *big.Int, headerProgress uint64) *HeaderInserter {
	return &HeaderInserter{
		logPrefix:          logPrefix,
		tx:                 tx,
		batch:              batch,
		localTd:            localTd,
		headerProgress:     headerProgress,
		unwindPoint:        headerProgress,
		canonicalBacktrack: make(map[common.Hash]common.Hash),
		parentDiffs:        make(map[common.Hash]*big.Int),
		childDiffs:         make(map[common.Hash]*big.Int),
	}
}
