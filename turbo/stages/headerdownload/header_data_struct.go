package headerdownload

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/petar/GoLLRB/llrb"
)

type Anchor struct {
	powDepth        int
	totalDifficulty uint256.Int
	tips            []common.Hash
	difficulty      uint256.Int
	hash            common.Hash
	blockHeight     uint64
	timestamp       uint64
}

type Tip struct {
	anchor               *Anchor
	cumulativeDifficulty uint256.Int
	timestamp            uint64
	difficulty           uint256.Int
	blockHeight          uint64
	uncleHash            common.Hash
	noPrepend            bool
}

type TipItem struct {
	tipHash              common.Hash
	cumulativeDifficulty uint256.Int
}

// First item in ChainSegment is the anchor
// ChainSegment must be contigous and must not include bad headers
type ChainSegment struct {
	headers []*types.Header
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

type RequestQueueItem struct {
	anchorParent common.Hash
	waitUntil    uint64
}

type RequestQueue []RequestQueueItem

// Request for chain segment starting with hash and going to its parent, etc, with length headers in total
type HeaderRequest struct {
	Hash   common.Hash
	Number uint64
	Length int
}

type VerifySealFunc func(header *types.Header) error
type CalcDifficultyFunc func(childTimestamp uint64, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentHash, parentUncleHash common.Hash) *big.Int

type HeaderDownload struct {
	buffer   []byte
	filesDir string
	//currentFile            *os.File
	//currentFileWriter      io.Writer
	badHeaders             map[common.Hash]struct{}
	anchors                map[common.Hash][]*Anchor // Mapping from parentHash to collection of anchors
	tips                   map[common.Hash]*Tip
	tipLimiter             *llrb.LLRB
	tipLimit               int
	initPowDepth           int    // powDepth assigned to the newly inserted anchor
	newAnchorFutureLimit   uint64 // How far in the future (relative to current time) the new anchors are allowed to be
	newAnchorPastLimit     uint64 // How far in the past (relative to current time) the new anchors are allowed to be
	highestTotalDifficulty uint256.Int
	requestQueue           *RequestQueue
	calcDifficultyFunc     CalcDifficultyFunc
	verifySealFunc         VerifySealFunc
	RequestQueueTimer      *time.Timer
}

func (a *TipItem) Less(b llrb.Item) bool {
	bi := b.(*TipItem)
	if a.cumulativeDifficulty.Eq(&bi.cumulativeDifficulty) {
		// hash is unique and it breaks the ties
		return bytes.Compare(a.tipHash[:], bi.tipHash[:]) < 0
	}
	return a.cumulativeDifficulty.Lt(&bi.cumulativeDifficulty)
}

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

func NewHeaderDownload(filesDir string,
	tipLimit, initPowDepth int,
	calcDifficultyFunc CalcDifficultyFunc,
	verifySealFunc VerifySealFunc,
	newAnchorFutureLimit, newAnchorPastLimit uint64,
) *HeaderDownload {
	hd := &HeaderDownload{
		filesDir:             filesDir,
		badHeaders:           make(map[common.Hash]struct{}),
		anchors:              make(map[common.Hash][]*Anchor),
		tips:                 make(map[common.Hash]*Tip),
		tipLimiter:           llrb.New(),
		tipLimit:             tipLimit,
		initPowDepth:         initPowDepth,
		requestQueue:         &RequestQueue{},
		calcDifficultyFunc:   calcDifficultyFunc,
		verifySealFunc:       verifySealFunc,
		newAnchorFutureLimit: newAnchorFutureLimit,
		newAnchorPastLimit:   newAnchorPastLimit,
	}
	heap.Init(hd.requestQueue)
	hd.RequestQueueTimer = time.NewTimer(time.Hour)
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

const HeaderPreBlockHeight = 32 /*ParentHash*/ + 32 /*UncleHash*/ + 20 /*Coinbase*/ + 32 /*Root*/ + 32 /*TxHash*/ + 32 /*ReceiptHash*/ +
																256 /*Bloom*/ + 16 /*Difficulty */
const HeaderPostBlockHeight = 8 /*Number*/ + 8 /*GasLimit*/ + 8 /*GasUsed*/ + 8 /*Time*/ + 1 /*len(Extra)*/ + 32 /*Extra*/ + 8 /*Nonce*/

const HeaderSerLength = HeaderPreBlockHeight + HeaderPostBlockHeight

func SerialiseHeader(header *types.Header, buffer []byte) {
	pos := 0
	copy(buffer[pos:pos+32], header.ParentHash[:])
	pos += 32
	copy(buffer[pos:pos+32], header.UncleHash[:])
	pos += 32
	copy(buffer[pos:pos+20], header.Coinbase[:])
	pos += 20
	copy(buffer[pos:pos+32], header.Root[:])
	pos += 32
	copy(buffer[pos:pos+32], header.TxHash[:])
	pos += 32
	copy(buffer[pos:pos+32], header.ReceiptHash[:])
	pos += 32
	copy(buffer[pos:pos+256], header.Bloom[:])
	pos += 256
	if header.Difficulty == nil {
		header.Difficulty = new(big.Int)
	}
	header.Difficulty.FillBytes(buffer[pos : pos+16])
	pos += 16
	if header.Number == nil {
		header.Number = new(big.Int)
	}
	header.Number.FillBytes(buffer[pos : pos+8])
	pos += 8
	binary.BigEndian.PutUint64(buffer[pos:pos+8], header.GasLimit)
	pos += 8
	binary.BigEndian.PutUint64(buffer[pos:pos+8], header.GasUsed)
	pos += 8
	binary.BigEndian.PutUint64(buffer[pos:pos+8], header.Time)
	pos += 8
	buffer[pos] = byte(len(header.Extra))
	pos++
	copy(buffer[pos:pos+32], header.Extra)
	pos += 32
	binary.BigEndian.PutUint64(buffer[pos:pos+8], header.Nonce.Uint64())
}

func DeserialiseHeader(header *types.Header, buffer []byte) {
	pos := 0
	copy(header.ParentHash[:], buffer[pos:pos+32])
	pos += 32
	copy(header.UncleHash[:], buffer[pos:pos+32])
	pos += 32
	copy(header.Coinbase[:], buffer[pos:pos+20])
	pos += 20
	copy(header.Root[:], buffer[pos:pos+32])
	pos += 32
	copy(header.TxHash[:], buffer[pos:pos+32])
	pos += 32
	copy(header.ReceiptHash[:], buffer[pos:pos+32])
	pos += 32
	copy(header.Bloom[:], buffer[pos:pos+256])
	pos += 256
	if header.Difficulty == nil {
		header.Difficulty = new(big.Int)
	}
	header.Difficulty.SetBytes(buffer[pos : pos+16])
	pos += 16
	if header.Number == nil {
		header.Number = new(big.Int)
	}
	header.Number.SetBytes(buffer[pos : pos+8])
	pos += 8
	header.GasLimit = binary.BigEndian.Uint64(buffer[pos : pos+8])
	pos += 8
	header.GasUsed = binary.BigEndian.Uint64(buffer[pos : pos+8])
	pos += 8
	header.Time = binary.BigEndian.Uint64(buffer[pos : pos+8])
	pos += 8
	extraLen := int(buffer[pos])
	pos++
	header.Extra = header.Extra[:0]
	header.Extra = append(header.Extra, buffer[pos:pos+extraLen]...)
	pos += 32
	header.Nonce = types.EncodeNonce(binary.BigEndian.Uint64(buffer[pos : pos+8]))
}

// Wrapper for the header buffer to sort headers within by block height
type BufferSorter []byte

func (bs BufferSorter) Len() int {
	return len(bs) / HeaderSerLength
}

func (bs BufferSorter) Less(i, j int) bool {
	hi := binary.BigEndian.Uint64(bs[i*HeaderSerLength+HeaderPreBlockHeight:])
	hj := binary.BigEndian.Uint64(bs[j*HeaderSerLength+HeaderPreBlockHeight:])
	return hi < hj
}

func (bs BufferSorter) Swap(i, j int) {
	var swapBuffer [HeaderSerLength]byte
	copy(swapBuffer[:], bs[i*HeaderSerLength:])
	copy(bs[i*HeaderSerLength:i*HeaderSerLength+HeaderSerLength], bs[j*HeaderSerLength:j*HeaderSerLength+HeaderSerLength])
	copy(bs[j*HeaderSerLength:], swapBuffer[:])
}
