package headerdownload

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/log"
)

// Implements sort.Interface so we can sort the incoming header in the message by block height
type HeadersByBlockHeight []*types.Header

func (h HeadersByBlockHeight) Len() int {
	return len(h)
}

func (h HeadersByBlockHeight) Less(i, j int) bool {
	// Note - the ordering is the inverse ordering of the block heights
	return h[i].Number.Cmp(h[j].Number) > 0
}

func (h HeadersByBlockHeight) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// SplitIntoSegments converts message containing headers into a collection of chain segments
func (hd *HeaderDownload) SplitIntoSegments(msg []*types.Header) ([]*ChainSegment, Penalty, error) {
	sort.Sort(HeadersByBlockHeight(msg))
	// Now all headers are order from the highest block height to the lowest
	var segments []*ChainSegment                         // Segments being built
	segmentMap := make(map[common.Hash]int)              // Mapping of the header hash to the index of the chain segment it belongs
	childrenMap := make(map[common.Hash][]*types.Header) // Mapping parent hash to the children
	dedupMap := make(map[common.Hash]struct{})           // Map used for detecting duplicate headers
	for _, header := range msg {
		headerHash := header.Hash()
		if _, bad := hd.badHeaders[headerHash]; bad {
			return nil, BadBlockPenalty, nil
		}
		if _, duplicate := dedupMap[headerHash]; duplicate {
			return nil, DuplicateHeaderPenalty, nil
		}
		dedupMap[headerHash] = struct{}{}
		var segmentIdx int
		children := childrenMap[headerHash]
		for _, child := range children {
			if valid, penalty := hd.childParentValid(child, header); !valid {
				return nil, penalty, nil
			}
		}
		if len(children) == 1 {
			// Single child, extract segmentIdx
			segmentIdx = segmentMap[headerHash]
		} else {
			// No children, or more than one child, create new segment
			segmentIdx = len(segments)
			segments = append(segments, &ChainSegment{})
		}
		segments[segmentIdx].Headers = append(segments[segmentIdx].Headers, header)
		segmentMap[header.ParentHash] = segmentIdx
		siblings := childrenMap[header.ParentHash]
		siblings = append(siblings, header)
		childrenMap[header.ParentHash] = siblings
	}
	return segments, NoPenalty, nil
}

// Checks whether child-parent relationship between two headers is correct
// (excluding Proof Of Work validity)
func (hd *HeaderDownload) childParentValid(child, parent *types.Header) (bool, Penalty) {
	if parent.Number.Uint64()+1 != child.Number.Uint64() {
		return false, WrongChildBlockHeightPenalty
	}
	childDifficulty := hd.calcDifficultyFunc(child.Time, parent.Time, parent.Difficulty, parent.Number, parent.Hash(), parent.UncleHash)
	if child.Difficulty.Cmp(childDifficulty) != 0 {
		return false, WrongChildDifficultyPenalty
	}
	return true, NoPenalty
}

// SingleHeaderAsSegment converts message containing 1 header into one singleton chain segment
func (hd *HeaderDownload) SingleHeaderAsSegment(header *types.Header) ([]*ChainSegment, Penalty, error) {
	headerHash := header.Hash()
	if _, bad := hd.badHeaders[headerHash]; bad {
		return nil, BadBlockPenalty, nil
	}
	return []*ChainSegment{{Headers: []*types.Header{header}}}, NoPenalty, nil
}

// FindAnchors attempts to find anchors to which given chain segment can be attached to
func (hd *HeaderDownload) FindAnchors(segment *ChainSegment) (found bool, start int, anchorParent common.Hash, invalidAnchors []int) {
	// Walk the segment from children towards parents
	for i, header := range segment.Headers {
		// Check if the header can be attached to an anchor of a working tree
		if anchors, attaching := hd.anchors[header.Hash()]; attaching {
			var invalidAnchors []int
			for anchorIdx, anchor := range anchors {
				if valid := hd.anchorParentValid(anchor, header); !valid {
					invalidAnchors = append(invalidAnchors, anchorIdx)
				}
			}
			return true, i, header.Hash(), invalidAnchors
		}
	}
	return false, 0, common.Hash{}, nil
}

// InvalidateAnchors removes trees with given anchor hashes (belonging to the given anchor parent)
func (hd *HeaderDownload) InvalidateAnchors(anchorParent common.Hash, invalidAnchors []int) (tombstones []common.Hash, err error) {
	if len(invalidAnchors) > 0 {
		if anchors, attaching := hd.anchors[anchorParent]; attaching {
			j := 0
			var filteredAnchors []*Anchor
			for k, anchor := range anchors {
				if j < len(invalidAnchors) && invalidAnchors[j] == k {
					// Invalidate the entire tree that is rooted at this anchor anchor
					hd.anchorTree.Delete(anchor)
					for _, anchorTipItem := range *anchor.tipQueue {
						delete(hd.tips, anchorTipItem.hash)
						hd.tipCount--
					}
					tombstones = append(tombstones, anchor.hash)
					j++
				} else {
					filteredAnchors = append(filteredAnchors, anchor)
				}
			}
			if len(filteredAnchors) > 0 {
				hd.anchors[anchorParent] = filteredAnchors
			} else {
				delete(hd.anchors, anchorParent)
			}
		} else {
			return nil, fmt.Errorf("invalidateAnchors anchors were not found for %x", anchorParent)
		}
	}
	return tombstones, nil
}

// FindTip attempts to find tip of a tree that given chain segment can be attached to
// the given chain segment may be found invalid relative to a working tree, in this case penalty for peer is returned
func (hd *HeaderDownload) FindTip(segment *ChainSegment, start int) (found bool, end int, penalty Penalty) {
	if _, duplicate := hd.getTip(segment.Headers[start].Hash()); duplicate {
		return true, 0, NoPenalty
	}
	// Walk the segment from children towards parents
	for i, header := range segment.Headers[start:] {
		// Check if the header can be attached to any tips
		if tip, attaching := hd.getTip(header.ParentHash); attaching {
			// Before attaching, we must check the parent-child relationship
			if valid, penalty := hd.childTipValid(header, header.ParentHash, tip); !valid {
				return true, start + i + 1, penalty
			}
			return true, start + i + 1, NoPenalty
		}
	}
	return false, len(segment.Headers), NoPenalty
}

// VerifySeals verifies Proof Of Work for the part of the given chain segment
// It reports first verification error, or returns the powDepth that the anchor of this
// chain segment should have, if created
func (hd *HeaderDownload) VerifySeals(segment *ChainSegment, anchorFound, tipFound bool, start, end int, currentTime uint64) (powDepth int, err error) {
	if !anchorFound && !tipFound {
		anchorHeader := segment.Headers[end-1]
		if anchorHeader.Time > currentTime+hd.newAnchorFutureLimit {
			return 0, fmt.Errorf("detached segment too far in the future")
		}
		if anchorHeader.Time+hd.newAnchorPastLimit < currentTime {
			return 0, fmt.Errorf("detached segment too far in the past")
		}
	}

	var powDepthSet bool
	if anchorFound {
		if anchors, ok := hd.anchors[segment.Headers[start].Hash()]; ok {
			for _, anchor := range anchors {
				if !powDepthSet || anchor.powDepth < powDepth {
					powDepth = anchor.powDepth
					powDepthSet = true
				}
			}
		} else {
			return 0, fmt.Errorf("verifySeals anchors were not found for %x", segment.Headers[start].Hash())
		}
	}
	for _, header := range segment.Headers[start:end] {
		if !anchorFound || powDepth > 0 {
			if err := hd.verifySealFunc(header); err != nil {
				return powDepth, err
			}
		}
		if anchorFound && powDepth > 0 {
			powDepth--
		}
	}
	return powDepth, nil
}

// ExtendUp extends a working tree up from the tip, using given chain segment
func (hd *HeaderDownload) ExtendUp(segment *ChainSegment, start, end int, currentTime uint64) error {
	// Find attachment tip again
	tipHeader := segment.Headers[end-1]
	if attachmentTip, attaching := hd.getTip(tipHeader.ParentHash); attaching {
		newAnchor := attachmentTip.anchor
		cumulativeDifficulty := attachmentTip.cumulativeDifficulty
		// Iterate over headers backwards (from parents towards children), to be able calculate cumulative difficulty along the way
		for i := end - 1; i >= start; i-- {
			header := segment.Headers[i]
			diff, overflow := uint256.FromBig(header.Difficulty)
			if overflow {
				return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
			}
			cumulativeDifficulty.Add(&cumulativeDifficulty, diff)
			if err := hd.addHeaderAsTip(header, newAnchor, cumulativeDifficulty, currentTime); err != nil {
				return fmt.Errorf("extendUp addHeaderAsTip for %x: %v", header.Hash(), err)
			}
		}
	} else {
		return fmt.Errorf("extendUp attachment tip not found for %x", tipHeader.ParentHash)
	}
	return nil
}

// ExtendDown extends some working trees down from the anchor, using given chain segment
// it creates a new anchor and collects all the tips from the attached anchors to it
func (hd *HeaderDownload) ExtendDown(segment *ChainSegment, start, end int, powDepth int, currentTime uint64) error {
	// Find attachement anchors again
	anchorHeader := segment.Headers[start]
	if anchors, attaching := hd.anchors[anchorHeader.Hash()]; attaching {
		newAnchorHeader := segment.Headers[end-1]
		diff, overflow := uint256.FromBig(newAnchorHeader.Difficulty)
		if overflow {
			return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", newAnchorHeader.Difficulty)
		}
		newAnchor := &Anchor{
			powDepth:    powDepth,
			timestamp:   newAnchorHeader.Time,
			difficulty:  *diff,
			hash:        newAnchorHeader.Hash(),
			blockHeight: newAnchorHeader.Number.Uint64(),
			tipQueue:    &AnchorTipQueue{},
			anchorID:    hd.nextAnchorID,
		}
		hd.nextAnchorID++
		heap.Init(newAnchor.tipQueue)
		hd.anchors[newAnchorHeader.ParentHash] = append(hd.anchors[newAnchorHeader.ParentHash], newAnchor)
		// Iterate headers in the segment to compute difficulty difference along the way
		var difficultyDifference uint256.Int
		for _, header := range segment.Headers[start:end] {
			diff, overflow := uint256.FromBig(header.Difficulty)
			if overflow {
				return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
			}
			difficultyDifference.Add(&difficultyDifference, diff)
		}
		// Go over tips of the anchors we are replacing, bump their cumulative difficulty, and add them to the new anchor
		for _, anchor := range anchors {
			hd.anchorTree.Delete(anchor)
			for _, tipQueueItem := range *anchor.tipQueue {
				if tip, ok := hd.getTip(tipQueueItem.hash); ok {
					tip.cumulativeDifficulty.Add(&tip.cumulativeDifficulty, &difficultyDifference)
					tip.anchor = newAnchor
					heap.Push(newAnchor.tipQueue, tipQueueItem)
					if tip.blockHeight > newAnchor.maxTipHeight {
						newAnchor.maxTipHeight = tip.blockHeight
					}
				}
			}
		}
		delete(hd.anchors, anchorHeader.Hash())
		hd.anchorTree.ReplaceOrInsert(newAnchor)
		// Add all headers in the segments as tips to this anchor
		// Recalculate cumulative difficulty for each header
		var cumulativeDifficulty uint256.Int
		for i := end - 1; i >= start; i-- {
			header := segment.Headers[i]
			diff, overflow := uint256.FromBig(header.Difficulty)
			if overflow {
				return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
			}
			cumulativeDifficulty.Add(&cumulativeDifficulty, diff)
			if err := hd.addHeaderAsTip(header, newAnchor, cumulativeDifficulty, currentTime); err != nil {
				return fmt.Errorf("extendUp addHeaderAsTip for %x: %v", header.Hash(), err)
			}
		}
		hd.requestQueue.PushFront(RequestQueueItem{anchorParent: newAnchorHeader.ParentHash, waitUntil: currentTime})
	} else {
		return fmt.Errorf("extendDown attachment anchors not found for %x", anchorHeader.Hash())
	}
	return nil
}

// Connect connects some working trees using anchors of some, and a tip of another
func (hd *HeaderDownload) Connect(segment *ChainSegment, start, end int, currentTime uint64) error {
	// Find attachment tip again
	tipHeader := segment.Headers[end-1]
	// Find attachement anchors again
	anchorHeader := segment.Headers[start]
	attachmentTip, ok1 := hd.getTip(tipHeader.ParentHash)
	if !ok1 {
		return fmt.Errorf("connect attachment tip not found for %x", tipHeader.ParentHash)
	}
	anchors, ok2 := hd.anchors[anchorHeader.Hash()]
	if !ok2 {
		return fmt.Errorf("connect attachment anchors not found for %x", anchorHeader.Hash())
	}
	newAnchor := attachmentTip.anchor
	// Iterate headers in the segment to compute difficulty difference along the way
	difficultyDifference := attachmentTip.cumulativeDifficulty
	for _, header := range segment.Headers[start:end] {
		diff, overflow := uint256.FromBig(header.Difficulty)
		if overflow {
			return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
		}
		difficultyDifference.Add(&difficultyDifference, diff)
	}
	hd.anchorTree.Delete(newAnchor)
	// Go over tips of the anchors we are replacing, bump their cumulative difficulty, and add them to the new anchor
	for _, anchor := range anchors {
		hd.anchorTree.Delete(anchor)
		for _, tipQueueItem := range *anchor.tipQueue {
			if tip, ok := hd.getTip(tipQueueItem.hash); ok {
				tip.cumulativeDifficulty.Add(&tip.cumulativeDifficulty, &difficultyDifference)
				tip.anchor = newAnchor
				heap.Push(newAnchor.tipQueue, tipQueueItem)
				if tip.blockHeight > newAnchor.maxTipHeight {
					newAnchor.maxTipHeight = tip.blockHeight
				}
			}
		}
	}
	cumulativeDifficulty := attachmentTip.cumulativeDifficulty
	delete(hd.anchors, anchorHeader.Hash())
	hd.anchorTree.ReplaceOrInsert(newAnchor)
	// Iterate over headers backwards (from parents towards children), to be able calculate cumulative difficulty along the way
	for i := end - 1; i >= start; i-- {
		header := segment.Headers[i]
		diff, overflow := uint256.FromBig(header.Difficulty)
		if overflow {
			return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
		}
		cumulativeDifficulty.Add(&cumulativeDifficulty, diff)
		if err := hd.addHeaderAsTip(header, newAnchor, cumulativeDifficulty, currentTime); err != nil {
			return fmt.Errorf("extendUp addHeaderAsTip for %x: %v", header.Hash(), err)
		}
	}
	// If we connect to the hard-coded tip, we remove it. Once there is only one hard-coded tip left, it is clear that everything is connected
	delete(hd.hardTips, tipHeader.ParentHash)
	return nil
}

func (hd *HeaderDownload) NewAnchor(segment *ChainSegment, start, end int, currentTime uint64) error {
	anchorHeader := segment.Headers[end-1]
	var anchor *Anchor
	var err error
	if anchor, err = hd.addHeaderAsAnchor(anchorHeader, hd.initPowDepth); err != nil {
		return err
	}
	cumulativeDifficulty := uint256.Int{}
	// Iterate over headers backwards (from parents towards children), to be able calculate cumulative difficulty along the way
	for i := end - 1; i >= start; i-- {
		header := segment.Headers[i]
		diff, overflow := uint256.FromBig(header.Difficulty)
		if overflow {
			return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
		}
		cumulativeDifficulty.Add(&cumulativeDifficulty, diff)
		if err = hd.addHeaderAsTip(header, anchor, cumulativeDifficulty, currentTime); err != nil {
			return fmt.Errorf("newAnchor addHeaderAsTip for %x: %v", header.Hash(), err)
		}
	}
	if anchorHeader.ParentHash != (common.Hash{}) {
		hd.requestQueue.PushFront(RequestQueueItem{anchorParent: anchorHeader.ParentHash, waitUntil: currentTime})
	}
	return nil
}

func (hd *HeaderDownload) HardCodedHeader(header *types.Header, currentTime uint64) error {
	if anchor, err := hd.addHeaderAsAnchor(header, 0 /* powDepth */); err == nil {
		diff, overflow := uint256.FromBig(header.Difficulty)
		if overflow {
			return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
		}
		tip := &Tip{
			anchor:               anchor,
			cumulativeDifficulty: *diff,
			timestamp:            header.Time,
			blockHeight:          header.Number.Uint64(),
			uncleHash:            header.UncleHash,
			difficulty:           *diff,
		}
		tipHash := header.Hash()
		hd.tips[tipHash] = tip
		_, hard := hd.hardTips[tipHash]
		hd.tips[tipHash] = tip
		heap.Push(anchor.tipQueue, AnchorTipItem{hash: tipHash, height: tip.blockHeight, hard: hard})
		hd.tipCount++
		if tip.blockHeight > anchor.maxTipHeight {
			anchor.maxTipHeight = tip.blockHeight
		}
		hd.anchorTree.ReplaceOrInsert(anchor)
		if header.ParentHash != (common.Hash{}) {
			hd.requestQueue.PushFront(RequestQueueItem{anchorParent: header.ParentHash, waitUntil: currentTime})
		}
	} else {
		return err
	}
	return nil
}

// AddSegmentToBuffer adds another segment to the buffer and return true if the buffer is now full
func (hd *HeaderDownload) AddSegmentToBuffer(segment *ChainSegment, start, end int) {
	if end > start {
		fmt.Printf("Adding segment [%d-%d] to the buffer\n", segment.Headers[end-1].Number.Uint64(), segment.Headers[start].Number.Uint64())
	}
	var serBuffer [HeaderSerLength]byte
	for _, header := range segment.Headers[start:end] {
		SerialiseHeader(header, serBuffer[:])
		hd.buffer = append(hd.buffer, serBuffer[:]...)
	}
}

func (hd *HeaderDownload) AddHeaderToBuffer(header *types.Header) {
	fmt.Printf("Adding header %d to the buffer\n", header.Number.Uint64())
	var serBuffer [HeaderSerLength]byte
	SerialiseHeader(header, serBuffer[:])
	hd.buffer = append(hd.buffer, serBuffer[:]...)
}

func (hd *HeaderDownload) AnchorState() string {
	//nolint:prealloc
	var ss []string
	for anchorParent, anchors := range hd.anchors {
		var skip = true
		for _, anchor := range anchors {
			if anchor.maxTipHeight > anchor.blockHeight {
				skip = false
				break
			}
		}
		if skip {
			continue
		}
		var sb strings.Builder
		for i, anchor := range anchors {
			if i > 0 {
				sb.WriteString("; ")
			}
			sb.WriteString(fmt.Sprintf("{%8d", anchor.blockHeight))
			end := anchor.maxTipHeight
			var sbb strings.Builder
			var bs []int
			for _, tipQueueItem := range *anchor.tipQueue {
				bs = append(bs, int(tipQueueItem.height))
			}
			sort.Ints(bs)
			for j, b := range bs {
				if j == 0 {
					sbb.WriteString(fmt.Sprintf("%d", b))
				} else if j == len(bs)-1 {
					if bs[j-1]+1 == b {
						// Close interval
						sbb.WriteString(fmt.Sprintf("-%d", b))
					} else {
						// Standalone
						sbb.WriteString(fmt.Sprintf(" %d", b))
					}
				} else {
					if bs[j-1] == b {
						// Skip
					} else if bs[j-1]+1 == b {
						if b+1 == bs[j+1] {
							// Skip
						} else {
							// Close interval
							sbb.WriteString(fmt.Sprintf("-%d", b))
						}
					} else {
						// Open interval or standalone
						sbb.WriteString(fmt.Sprintf(" %d", b))
					}
				}
			}
			if end == 0 {
				sb.WriteString(fmt.Sprintf(" HardCoded tips=%d tipStretch=%d (%s)}", anchor.tipQueue.Len(), anchor.tipStretch(), sbb.String()))
			} else {
				sb.WriteString(fmt.Sprintf("-%d (%d) tips=%d tipStretch=%d (%s)}", end, end-anchor.blockHeight, anchor.tipQueue.Len(), anchor.tipStretch(), sbb.String()))
			}
		}
		sb.WriteString(fmt.Sprintf(" => %x", anchorParent))
		ss = append(ss, sb.String())
	}
	sort.Strings(ss)
	return strings.Join(ss, "\n")
}

// Heap element for merging together header files
type HeapElem struct {
	file        *os.File
	reader      io.Reader
	blockHeight uint64
	hash        common.Hash
	header      *types.Header
}

type Heap []HeapElem

func (h Heap) Len() int {
	return len(h)
}

func (h Heap) Less(i, j int) bool {
	if h[i].blockHeight == h[j].blockHeight {
		return bytes.Compare(h[i].hash[:], h[j].hash[:]) < 0
	}
	return h[i].blockHeight < h[j].blockHeight
}

func (h Heap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *Heap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(HeapElem))
}

func (h *Heap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

const AnchorSerLen = 32 /* ParentHash */ + 8 /* powDepth */ + 8 /* maxTipHeight */

func (hd *HeaderDownload) CheckFiles() error {
	fileInfos, err := ioutil.ReadDir(hd.filesDir)
	if err != nil {
		return err
	}
	var buffer [HeaderSerLength]byte
	var anchorBuf [AnchorSerLen]byte
	for _, fileInfo := range fileInfos {
		f, err1 := os.Open(path.Join(hd.filesDir, fileInfo.Name()))
		if err1 != nil {
			return fmt.Errorf("open file %s: %v", fileInfo.Name(), err1)
		}
		r := bufio.NewReader(f)
		if _, err = io.ReadFull(r, anchorBuf[:8]); err != nil {
			fmt.Printf("reading anchor sequence and count from file: %v\n", err)
			continue
		}
		anchorCount := int(binary.BigEndian.Uint32((anchorBuf[4:])))
		for i := 0; i < anchorCount; i++ {
			if _, err = io.ReadFull(r, anchorBuf[:]); err != nil {
				fmt.Printf("reading anchor %x from file: %v\n", i, err)
			}
		}
		for {
			var header types.Header
			if _, err = io.ReadFull(r, buffer[:]); err != nil {
				if !errors.Is(err, io.EOF) {
					fmt.Printf("reading header from file: %v\n", err)
				}
				break
			}
			DeserialiseHeader(&header, buffer[:])
			fmt.Printf("Read header %d from file %s\n", header.Number.Uint64(), fileInfo.Name())
		}
	}
	return nil
}

func InitHardCodedTips(filename string) map[common.Hash]struct{} {
	// Insert hard-coded headers if present
	hardTips := make(map[common.Hash]struct{})
	if _, err := os.Stat(filename); err == nil {
		if f, err1 := os.Open(filename); err1 == nil {
			var hBuffer [HeaderSerLength]byte
			for {
				var h types.Header
				if _, err2 := io.ReadFull(f, hBuffer[:]); err2 == nil {
					DeserialiseHeader(&h, hBuffer[:])
				} else if errors.Is(err2, io.EOF) {
					break
				} else {
					log.Error("Failed to read hard coded header", "error", err2)
					break
				}
				hardTips[h.Hash()] = struct{}{}
			}
		} else {
			log.Error("Failed to open hard-coded headers", "file", filename, "error", err1)
		}
	} else {
		log.Error("Failed to stat hard-coded headers", "file", filename, "error", err)
	}
	return hardTips
}

func (hd *HeaderDownload) RecoverFromFiles(currentTime uint64, hardTips map[common.Hash]struct{}) (bool, error) {
	fileInfos, err := ioutil.ReadDir(hd.filesDir)
	if err != nil {
		return false, err
	}
	h := &Heap{}
	heap.Init(h)
	var buffer [HeaderSerLength]byte
	var anchorBuf [AnchorSerLen]byte
	//nolint:prealloc
	var fs []*os.File
	//nolint:prealloc
	var rs []io.Reader
	// Open all files and only read anchor sequences to decide which one has the latest information about the anchors
	hd.anchorSequence = 0
	var lastAnchors = make(map[common.Hash]*Anchor)
	for _, fileInfo := range fileInfos {
		f, err1 := os.Open(path.Join(hd.filesDir, fileInfo.Name()))
		if err1 != nil {
			return false, fmt.Errorf("open file %s: %v", fileInfo.Name(), err1)
		}
		r := bufio.NewReader(f)
		if _, err = io.ReadFull(r, anchorBuf[:8]); err != nil {
			fmt.Printf("reading anchor sequence and count from file: %v\n", err)
			continue
		}
		anchorSequence := binary.BigEndian.Uint32(anchorBuf[:])
		anchorCount := int(binary.BigEndian.Uint32((anchorBuf[4:])))
		var anchors = make(map[common.Hash]*Anchor)
		if anchorSequence >= hd.anchorSequence {
			fmt.Printf("Reading anchor sequence %d, anchor count: %d\n", anchorSequence, anchorCount)
		}
		for i := 0; i < anchorCount; i++ {
			if _, err = io.ReadFull(r, anchorBuf[:]); err != nil {
				fmt.Printf("reading anchor %x from file: %v\n", i, err)
			}
			if anchorSequence >= hd.anchorSequence { // Don't bother with parsing if we are not going to use this info
				anchor := &Anchor{tipQueue: &AnchorTipQueue{}, anchorID: hd.nextAnchorID}
				hd.nextAnchorID++
				heap.Init(anchor.tipQueue)
				pos := 0
				copy(anchor.hash[:], anchorBuf[pos:])
				pos += 32
				anchor.powDepth = int(binary.BigEndian.Uint64(anchorBuf[pos:]))
				pos += 8
				anchor.maxTipHeight = binary.BigEndian.Uint64(anchorBuf[pos:])
				anchors[anchor.hash] = anchor
				fmt.Printf("anchor: %x, powDepth: %d, maxTipHeight %d\n", anchor.hash, anchor.powDepth, anchor.maxTipHeight)
			}
		}
		if anchorSequence >= hd.anchorSequence {
			hd.anchorSequence = anchorSequence + 1
			lastAnchors = anchors
		}
		fs = append(fs, f)
		rs = append(rs, r)
	}
	// Based on the last anchors, set the hardTips
	for _, anchor := range lastAnchors {
		if _, ok := hardTips[anchor.hash]; ok {
			hd.hardTips[anchor.hash] = struct{}{}
			fmt.Printf("Adding %d %x to hard-coded tips\n", anchor.blockHeight, anchor.hash)
		}
	}
	for i, f := range fs {
		r := rs[i]
		var header types.Header
		if _, err = io.ReadFull(r, buffer[:]); err != nil {
			if !errors.Is(err, io.EOF) {
				fmt.Printf("reading header from file: %v\n", err)
			}
			continue
		}
		DeserialiseHeader(&header, buffer[:])
		he := HeapElem{file: f, reader: r, blockHeight: header.Number.Uint64(), hash: header.Hash(), header: &header}
		heap.Push(h, he)
	}
	var prevHeight uint64
	var parentAnchors = make(map[common.Hash]*Anchor)
	var parentDiffs = make(map[common.Hash]*uint256.Int)
	var childAnchors = make(map[common.Hash]*Anchor)
	var childDiffs = make(map[common.Hash]*uint256.Int)
	var prevHash common.Hash // Hash of previously seen header - to filter out potential duplicates
	for h.Len() > 0 {
		he := (heap.Pop(h)).(HeapElem)
		hash := he.header.Hash()
		if hash != prevHash {
			if he.blockHeight > prevHeight {
				// Clear out parent map and move childMap to its place
				parentAnchors = childAnchors
				parentDiffs = childDiffs
				childAnchors = make(map[common.Hash]*Anchor)
				childDiffs = make(map[common.Hash]*uint256.Int)
				if he.blockHeight != prevHeight+1 {
					// Skipping the level, so no connection between grand-parents and grand-children
					parentAnchors = make(map[common.Hash]*Anchor)
					parentDiffs = make(map[common.Hash]*uint256.Int)
				}
				prevHeight = he.blockHeight
			}
			// Since this header has already been processed, we do not expect overflow
			cumulativeDiff, overflow := uint256.FromBig(he.header.Difficulty)
			if overflow {
				return false, fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", he.header.Difficulty)
			}
			parentHash := he.header.ParentHash
			if parentAnchor, found := parentAnchors[parentHash]; found {
				parentDiff := parentDiffs[parentHash]
				cumulativeDiff.Add(cumulativeDiff, parentDiff)
				if err = hd.addHeaderAsTip(he.header, parentAnchor, *cumulativeDiff, currentTime); err != nil {
					return false, fmt.Errorf("add header as tip: %v", err)
				}
				childAnchors[hash] = parentAnchor
				childDiffs[hash] = cumulativeDiff
			} else {
				anchor, anchorExisted := lastAnchors[hash]
				if !anchorExisted {
					anchor = &Anchor{powDepth: hd.initPowDepth, hash: hash, tipQueue: &AnchorTipQueue{}, anchorID: hd.nextAnchorID}
					hd.nextAnchorID++
					heap.Init(anchor.tipQueue)
					fmt.Printf("Undeclared anchor for hash %x, inserting as empty\n", hash)
				}
				diff, overflow := uint256.FromBig(he.header.Difficulty)
				if overflow {
					return false, fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", he.header.Difficulty)
				}
				anchor.difficulty = *diff
				anchor.timestamp = he.header.Time
				anchor.blockHeight = he.header.Number.Uint64()
				if err = hd.addHeaderAsTip(he.header, anchor, *cumulativeDiff, currentTime); err != nil {
					return false, fmt.Errorf("add header as tip: %v", err)
				}
				if len(hd.anchors[parentHash]) == 0 {
					if parentHash != (common.Hash{}) {
						hd.requestQueue.PushFront(RequestQueueItem{anchorParent: parentHash, waitUntil: currentTime})
					}
				}
				hd.anchors[parentHash] = append(hd.anchors[parentHash], anchor)
				childAnchors[hash] = anchor
				childDiffs[hash] = cumulativeDiff
			}
			prevHash = hash
		} else {
			fmt.Printf("Duplicate header: %d %x\n", he.header.Number.Uint64(), hash)
		}
		var header types.Header
		if _, err = io.ReadFull(he.reader, buffer[:]); err == nil {
			DeserialiseHeader(&header, buffer[:])
			he.blockHeight = header.Number.Uint64()
			he.hash = header.Hash()
			he.header = &header
			heap.Push(h, he)
		} else {
			if !errors.Is(err, io.EOF) {
				fmt.Printf("reading header from file: %v\n", err)
			}
			if err = he.file.Close(); err != nil {
				fmt.Printf("closing file: %v\n", err)
			}
		}
	}
	return hd.anchorSequence > 0, nil
}

func (hd *HeaderDownload) RequestMoreHeaders(currentTime, timeout uint64) []*HeaderRequest {
	if hd.requestQueue.Len() == 0 {
		return nil
	}
	var prevTopTime uint64 = hd.requestQueue.Front().Value.(RequestQueueItem).waitUntil
	var requests []*HeaderRequest
	for peek := hd.requestQueue.Front(); peek != nil && peek.Value.(RequestQueueItem).waitUntil <= currentTime; peek = hd.requestQueue.Front() {
		hd.requestQueue.Remove(peek)
		item := peek.Value.(RequestQueueItem)
		if anchors, present := hd.anchors[item.anchorParent]; present {
			// Anchor still exists after the timeout
			requests = append(requests, &HeaderRequest{Hash: item.anchorParent, Number: anchors[0].blockHeight - 1, Length: 192})
			hd.requestQueue.PushBack(RequestQueueItem{anchorParent: item.anchorParent, waitUntil: currentTime + timeout})
		}
	}
	hd.resetRequestQueueTimer(prevTopTime, currentTime)
	return requests
}

func (hd *HeaderDownload) resetRequestQueueTimer(prevTopTime, currentTime uint64) {
	var nextTopTime uint64
	if hd.requestQueue.Len() > 0 {
		nextTopTime = hd.requestQueue.Front().Value.(RequestQueueItem).waitUntil
	}
	if nextTopTime == prevTopTime {
		return // Nothing changed
	}
	if nextTopTime <= currentTime {
		nextTopTime = currentTime
	}
	hd.RequestQueueTimer.Stop()
	//fmt.Printf("Recreating RequestQueueTimer for delay %d seconds\n", nextTopTime-currentTime)
	hd.RequestQueueTimer = time.NewTimer(time.Duration(nextTopTime-currentTime) * time.Second)
}

func (hd *HeaderDownload) FlushBuffer() error {
	if len(hd.buffer) < hd.bufferLimit {
		// Not flushing the buffer unless it is full
		return nil
	}
	// Sort the buffer first
	sort.Sort(BufferSorter(hd.buffer))
	if bufferFile, err := ioutil.TempFile(hd.filesDir, "headers-buf"); err == nil {
		// First write the anchors
		var buf [AnchorSerLen]byte
		binary.BigEndian.PutUint32(buf[:], hd.anchorSequence)
		anchorCount := 0
		for _, anchors := range hd.anchors {
			anchorCount += len(anchors)
		}
		binary.BigEndian.PutUint32(buf[4:], uint32(anchorCount))
		if _, err = bufferFile.Write(buf[:8]); err != nil {
			bufferFile.Close()
			return err
		}
		for _, anchors := range hd.anchors {
			for _, anchor := range anchors {
				pos := 0
				copy(buf[pos:], anchor.hash[:])
				pos += 32
				binary.BigEndian.PutUint64(buf[pos:], uint64(anchor.powDepth))
				pos += 8
				binary.BigEndian.PutUint64(buf[pos:], anchor.maxTipHeight)
				if _, err = bufferFile.Write(buf[:]); err != nil {
					bufferFile.Close()
					return err
				}
			}
		}
		if _, err = bufferFile.Write(hd.buffer); err != nil {
			bufferFile.Close()
			return err
		}
		if err = bufferFile.Close(); err != nil {
			return err
		}
		hd.buffer = hd.buffer[:0]
		hd.anchorSequence++
	} else {
		return err
	}
	fmt.Printf("Successfully flushed the buffer\n")
	return nil
}

// CheckInitiation looks at the first header in the given segment, and assuming
// that it has been added as a tip, checks whether the anchor parent hash
// associated with this tip equals to pre-set value (0x00..00 for genesis)
func (hd *HeaderDownload) CheckInitiation(segment *ChainSegment, initialHash common.Hash) bool {
	tipHash := segment.Headers[0].Hash()
	tip, exists := hd.getTip(tipHash)
	if !exists {
		return false
	}
	if tip.anchor.hash != initialHash {
		return false
	}
	fmt.Printf("Tip %d %x has total difficulty %d, highest %d, len(hd.hardTips) %d\n", tip.blockHeight, tipHash, tip.cumulativeDifficulty.ToBig(), hd.highestTotalDifficulty.ToBig(), len(hd.hardTips))
	if tip.cumulativeDifficulty.Gt(&hd.highestTotalDifficulty) {
		hd.highestTotalDifficulty.Set(&tip.cumulativeDifficulty)
		return true
	}
	return false
}

// childTipValid checks whether child-tip relationship between child header and a tip (that is being extended), is correct
// (excluding Proof Of Work validity)
func (hd *HeaderDownload) childTipValid(child *types.Header, tipHash common.Hash, tip *Tip) (bool, Penalty) {
	if tip.blockHeight+1 != child.Number.Uint64() {
		return false, WrongChildBlockHeightPenalty
	}
	childDifficulty := hd.calcDifficultyFunc(child.Time, tip.timestamp, tip.difficulty.ToBig(), big.NewInt(int64(tip.blockHeight)), tipHash, tip.uncleHash)
	if child.Difficulty.Cmp(childDifficulty) != 0 {
		return false, WrongChildDifficultyPenalty
	}
	return true, NoPenalty
}

func (hd *HeaderDownload) HasTip(tipHash common.Hash) bool {
	if _, ok := hd.getTip(tipHash); ok {
		return true
	}
	return false
}

func (hd *HeaderDownload) getTip(tipHash common.Hash) (*Tip, bool) {
	if tip, ok := hd.tips[tipHash]; ok {
		return tip, true
	}
	return nil, false
}

// addHeaderAsTip adds given header as a tip belonging to a given anchorParent
func (hd *HeaderDownload) addHeaderAsTip(header *types.Header, anchor *Anchor, cumulativeDifficulty uint256.Int, currentTime uint64) error {
	diff, overflow := uint256.FromBig(header.Difficulty)
	if overflow {
		return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
	}
	tipHash := header.Hash()
	tip := &Tip{
		anchor:               anchor,
		cumulativeDifficulty: cumulativeDifficulty,
		timestamp:            header.Time,
		difficulty:           *diff,
		blockHeight:          header.Number.Uint64(),
		uncleHash:            header.UncleHash,
	}
	_, hard := hd.hardTips[tipHash]
	hd.anchorTree.Delete(anchor)
	hd.tips[tipHash] = tip
	heap.Push(anchor.tipQueue, AnchorTipItem{hash: tipHash, height: tip.blockHeight, hard: hard})
	hd.tipCount++
	if tip.blockHeight > anchor.maxTipHeight {
		anchor.maxTipHeight = tip.blockHeight
	}
	hd.anchorTree.ReplaceOrInsert(anchor)
	hd.limitTips()
	return nil
}

// addHardCodedTip adds a hard-coded tip for which cimulative difficulty is known and no prepend is allowed
func (hd *HeaderDownload) addHardCodedTip(blockHeight uint64, timestamp uint64, hash common.Hash, anchor *Anchor, cumulativeDifficulty uint256.Int) {
	tip := &Tip{
		anchor:               anchor,
		cumulativeDifficulty: cumulativeDifficulty,
		timestamp:            timestamp,
		blockHeight:          blockHeight,
	}
	hd.tips[hash] = tip
}

func (hd *HeaderDownload) addHeaderAsAnchor(header *types.Header, powDepth int) (*Anchor, error) {
	diff, overflow := uint256.FromBig(header.Difficulty)
	if overflow {
		return nil, fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
	}
	anchor := &Anchor{
		powDepth:    powDepth,
		difficulty:  *diff,
		timestamp:   header.Time,
		hash:        header.Hash(),
		blockHeight: header.Number.Uint64(),
		tipQueue:    &AnchorTipQueue{},
		anchorID:    hd.nextAnchorID,
	}
	hd.nextAnchorID++
	heap.Init(anchor.tipQueue)
	hd.anchors[header.ParentHash] = append(hd.anchors[header.ParentHash], anchor)
	return anchor, nil
}

// reserveTip makes sure there is a space for at least one more tip
func (hd *HeaderDownload) limitTips() {
	for hd.tipCount > hd.tipLimit {
		//fmt.Printf("limitTips tips %d >= %d\n", hd.tipCount, hd.tipLimit)
		// Pick the anchor with the largest (maxTipHeight - minTipHeight) difference
		anchor := hd.anchorTree.DeleteMin().(*Anchor)
		//fmt.Printf("Chose anchor %d with maxTipHeight %d, tipStetch: %d\n", anchor.blockHeight, anchor.maxTipHeight, anchor.tipStretch())
		//hd.anchorTree.Delete(&AnchorItem{ID: anchor.anchorID, tipStretch: anchor.tipStretch()})
		tipItem := heap.Pop(anchor.tipQueue).(AnchorTipItem)
		hd.anchorTree.ReplaceOrInsert(anchor)
		delete(hd.tips, tipItem.hash)
		hd.tipCount--
	}
}

// anchorParentValid checks whether child-parent relationship between an anchor and
// its extension (parent) is correct
// (excluding Proof Of Work validity)
func (hd *HeaderDownload) anchorParentValid(anchor *Anchor, parent *types.Header) bool {
	if anchor.blockHeight != parent.Number.Uint64()+1 {
		fmt.Printf("anchor.blockHeight(%d) != parent.Number+1(%d)\n", anchor.blockHeight, parent.Number.Uint64()+1)
		return false
	}
	childDifficulty := hd.calcDifficultyFunc(anchor.timestamp, parent.Time, parent.Difficulty, parent.Number, parent.Hash(), parent.UncleHash)
	if anchor.difficulty.ToBig().Cmp(childDifficulty) != 0 {
		fmt.Printf("anchor.difficulty (%s) != childDifficulty (%s)\n", anchor.difficulty.ToBig(), childDifficulty)
	}
	return anchor.difficulty.ToBig().Cmp(childDifficulty) == 0
}
