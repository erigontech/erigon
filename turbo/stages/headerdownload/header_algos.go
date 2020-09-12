package headerdownload

import (
	"bufio"
	"container/heap"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"os"
	"sort"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
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

// HandleHeadersMsg converts message containing headers into a collection of chain segments
func (hd *HeaderDownload) HandleHeadersMsg(msg []*types.Header) ([]*ChainSegment, Penalty, error) {
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
		segments[segmentIdx].headers = append(segments[segmentIdx].headers, header)
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

// HandleNewBlockMsg converts message containing 1 header into one singleton chain segment
func (hd *HeaderDownload) HandleNewBlockMsg(header *types.Header) ([]*ChainSegment, Penalty, error) {
	headerHash := header.Hash()
	if _, bad := hd.badHeaders[headerHash]; bad {
		return nil, BadBlockPenalty, nil
	}
	return []*ChainSegment{{headers: []*types.Header{header}}}, NoPenalty, nil
}

// FindAnchors attempts to find anchors to which given chain segment can be attached to
func (hd *HeaderDownload) FindAnchors(segment *ChainSegment) (found bool, start int, anchorParent common.Hash, invalidAnchors []int) {
	// Walk the segment from children towards parents
	for i, header := range segment.headers {
		headerHash := header.Hash()
		// Check if the header can be attached to an anchor of a working tree
		if anchors, attaching := hd.anchors[headerHash]; attaching {
			var invalidAnchors []int
			for anchorIdx, anchor := range anchors {
				if valid := hd.anchorParentValid(anchor, header); !valid {
					invalidAnchors = append(invalidAnchors, anchorIdx)
				}
			}
			return true, i, headerHash, invalidAnchors
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
					for _, tipHash := range anchor.tips {
						hd.tips.Remove(tipHash)
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
func (hd *HeaderDownload) FindTip(segment *ChainSegment) (found bool, end int, penalty Penalty) {
	// Walk the segment from children towards parents
	for i, header := range segment.headers {
		// Check if the header can be attached to any tips
		if tip, attaching := hd.getTip(header.ParentHash); attaching {
			// Before attaching, we must check the parent-child relationship
			if valid, penalty := hd.childTipValid(header, header.ParentHash, tip); !valid {
				return true, i + 1, penalty
			}
			return true, i + 1, NoPenalty
		}
	}
	return false, len(segment.headers), NoPenalty
}

// VerifySeals verifies Proof Of Work for the part of the given chain segment
// It reports first verification error, or returns the powDepth that the anchor of this
// chain segment should have, if created
func (hd *HeaderDownload) VerifySeals(segment *ChainSegment, anchorFound bool, start, end int) (powDepth int, err error) {
	var powDepthSet bool
	if anchorFound {
		if anchors, ok := hd.anchors[segment.headers[start].Hash()]; ok {
			for _, anchor := range anchors {
				if !powDepthSet || anchor.powDepth < powDepth {
					powDepth = anchor.powDepth
					powDepthSet = true
				}
			}
		} else {
			return 0, fmt.Errorf("verifySeals anchors were not found for %x", segment.headers[start].Hash())
		}
	}
	for _, header := range segment.headers[start:end] {
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
func (hd *HeaderDownload) ExtendUp(segment *ChainSegment, start, end int) error {
	// Find attachment tip again
	tipHeader := segment.headers[end-1]
	if attachmentTip, attaching := hd.getTip(tipHeader.ParentHash); attaching {
		if attachmentTip.noPrepend {
			return fmt.Errorf("extendUp attachment tip had noPrepend flag on for %x", tipHeader.ParentHash)
		}
		newAnchor := attachmentTip.anchor
		cumulativeDifficulty := attachmentTip.cumulativeDifficulty
		// Iterate over headers backwards (from parents towards children), to be able calculate cumulative difficulty along the way
		for i := end - 1; i >= start; i-- {
			header := segment.headers[i]
			diff, overflow := uint256.FromBig(header.Difficulty)
			if overflow {
				return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
			}
			cumulativeDifficulty.Add(&cumulativeDifficulty, diff)
			if err := hd.addHeaderAsTip(header, newAnchor, cumulativeDifficulty); err != nil {
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
	anchorHeader := segment.headers[start]
	if anchors, attaching := hd.anchors[anchorHeader.Hash()]; attaching {
		newAnchorHeader := segment.headers[end-1]
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
		}
		hd.anchors[newAnchorHeader.ParentHash] = append(hd.anchors[newAnchorHeader.ParentHash], newAnchor)
		heap.Push(hd.requestQueue, RequestQueueItem{anchorParent: newAnchorHeader.ParentHash, waitUntil: currentTime})

		// Add all headers in the segments as tips to this anchor
		// Iterate in reverse order to be able to compute cumulative difficulty along the way
		var cumulativeDifficulty uint256.Int
		for i := end - 1; i >= start; i-- {
			header := segment.headers[i]
			diff, overflow := uint256.FromBig(header.Difficulty)
			if overflow {
				return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
			}
			cumulativeDifficulty.Add(&cumulativeDifficulty, diff)
			if err := hd.addHeaderAsTip(header, newAnchor, cumulativeDifficulty); err != nil {
				return fmt.Errorf("extendUp addHeaderAsTip for %x: %v", header.Hash(), err)
			}
		}
		// Go over tips of the anchors we are replacing, bump their cumulative difficulty, and add them to the new anchor
		for _, anchor := range anchors {
			for _, tipHash := range anchor.tips {
				if tip, ok := hd.getTip(tipHash); ok {
					tip.cumulativeDifficulty.Add(&tip.cumulativeDifficulty, &cumulativeDifficulty)
					tip.anchor = newAnchor
					newAnchor.tips = append(newAnchor.tips, tipHash)
				}
			}
		}
		delete(hd.anchors, anchorHeader.Hash())
	} else {
		return fmt.Errorf("extendDown attachment anchors not found for %x", anchorHeader.Hash())
	}
	return nil
}

// Connect connects some working trees using anchors of some, and a tip of another
func (hd *HeaderDownload) Connect(segment *ChainSegment, start, end int) error {
	// Find attachment tip again
	tipHeader := segment.headers[end-1]
	// Find attachement anchors again
	anchorHeader := segment.headers[start]
	attachmentTip, ok1 := hd.getTip(tipHeader.ParentHash)
	if !ok1 {
		return fmt.Errorf("connect attachment tip not found for %x", tipHeader.ParentHash)
	}
	anchors, ok2 := hd.anchors[anchorHeader.Hash()]
	if !ok2 {
		return fmt.Errorf("connect attachment anchors not found for %x", anchorHeader.Hash())
	}
	newAnchor := attachmentTip.anchor
	cumulativeDifficulty := attachmentTip.cumulativeDifficulty
	// Iterate over headers backwards (from parents towards children), to be able calculate cumulative difficulty along the way
	for i := end - 1; i >= start; i-- {
		header := segment.headers[i]
		diff, overflow := uint256.FromBig(header.Difficulty)
		if overflow {
			return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
		}
		cumulativeDifficulty.Add(&cumulativeDifficulty, diff)
		if err := hd.addHeaderAsTip(header, newAnchor, cumulativeDifficulty); err != nil {
			return fmt.Errorf("extendUp addHeaderAsTip for %x: %v", header.Hash(), err)
		}
	}
	// Go over tips of the anchors we are replacing, bump their cumulative difficulty, and add them to the new anchor
	for _, anchor := range anchors {
		for _, tipHash := range anchor.tips {
			if tip, ok := hd.getTip(tipHash); ok {
				tip.cumulativeDifficulty.Add(&tip.cumulativeDifficulty, &cumulativeDifficulty)
				tip.anchor = newAnchor
				newAnchor.tips = append(newAnchor.tips, tipHash)
			}
		}
	}
	delete(hd.anchors, anchorHeader.Hash())
	return nil
}

func (hd *HeaderDownload) NewAnchor(segment *ChainSegment, start, end int, currentTime uint64) (Penalty, error) {
	anchorHeader := segment.headers[end-1]
	if anchorHeader.Time > currentTime+hd.newAnchorFutureLimit {
		return TooFarFuturePenalty, nil
	}
	if anchorHeader.Time+hd.newAnchorPastLimit < currentTime {
		return TooFarPastPenalty, nil
	}
	var anchor *Anchor
	var err error
	if anchor, err = hd.addHeaderAsAnchor(anchorHeader, hd.initPowDepth, uint256.Int{}); err != nil {
		return NoPenalty, err
	}
	if anchorHeader.ParentHash != (common.Hash{}) {
		heap.Push(hd.requestQueue, RequestQueueItem{anchorParent: anchorHeader.ParentHash, waitUntil: currentTime})
	}
	cumulativeDifficulty := uint256.Int{}
	// Iterate over headers backwards (from parents towards children), to be able calculate cumulative difficulty along the way
	for i := end - 1; i >= start; i-- {
		header := segment.headers[i]
		diff, overflow := uint256.FromBig(header.Difficulty)
		if overflow {
			return NoPenalty, fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
		}
		cumulativeDifficulty.Add(&cumulativeDifficulty, diff)
		if err = hd.addHeaderAsTip(header, anchor, cumulativeDifficulty); err != nil {
			return NoPenalty, fmt.Errorf("newAnchor addHeaderAsTip for %x: %v", header.Hash(), err)
		}
	}
	return NoPenalty, nil
}

func (hd *HeaderDownload) HardCodedHeader(header *types.Header, totalDifficulty uint256.Int) error {
	if anchor, err := hd.addHeaderAsAnchor(header, 0 /* powDepth */, totalDifficulty); err == nil {
		hd.addHardCodedTip(header.Number.Uint64(), header.Time, header.Hash(), anchor, totalDifficulty)
	} else {
		return err
	}
	return nil
}

func (hd *HeaderDownload) AddToBuffer(segment *ChainSegment, start, end int) {
	var serBuffer [HeaderSerLength]byte
	for _, header := range segment.headers[start:end] {
		SerialiseHeader(header, serBuffer[:])
		hd.buffer = append(hd.buffer, serBuffer[:]...)
	}
}

// Heap element for merging together header files
type HeapElem struct {
	file        *os.File
	reader      io.Reader
	blockHeight uint64
	header      *types.Header
}

type Heap []HeapElem

func (h Heap) Len() int {
	return len(h)
}

func (h Heap) Less(i, j int) bool {
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

func (hd *HeaderDownload) RecoverFromFiles() error {
	fileInfos, err := ioutil.ReadDir(hd.filesDir)
	if err != nil {
		return err
	}
	h := &Heap{}
	heap.Init(h)
	var buffer [HeaderSerLength]byte
	for _, fileInfo := range fileInfos {
		f, err1 := os.Open(fileInfo.Name())
		if err1 != nil {
			return fmt.Errorf("open file %s: %v", fileInfo.Name(), err1)
		}
		r := bufio.NewReader(f)
		var header types.Header
		if _, err = io.ReadFull(r, buffer[:]); err != nil {
			if !errors.Is(err, io.EOF) {
				fmt.Printf("reading header from file: %v\n", err)
			}
			continue
		}
		DeserialiseHeader(&header, buffer[:])
		he := HeapElem{file: f, reader: r, blockHeight: header.Number.Uint64(), header: &header}
		heap.Push(h, he)
	}
	var prevHeight uint64
	var parentAnchors = make(map[common.Hash]*Anchor)
	var parentDiffs = make(map[common.Hash]*uint256.Int)
	var childAnchors = make(map[common.Hash]*Anchor)
	var childDiffs = make(map[common.Hash]*uint256.Int)
	for h.Len() > 0 {
		he := (heap.Pop(h)).(HeapElem)
		if he.blockHeight > prevHeight {
			// Clear out parent map and move childMap to its place
			childAnchors = make(map[common.Hash]*Anchor)
			childDiffs = make(map[common.Hash]*uint256.Int)
			if he.blockHeight == prevHeight+1 {
				parentAnchors = childAnchors
				parentDiffs = childDiffs
			} else {
				// Skipping the level, so no connection between grand-parents and grand-children
				parentAnchors = make(map[common.Hash]*Anchor)
				parentDiffs = make(map[common.Hash]*uint256.Int)
			}
			prevHeight = he.blockHeight
		}
		// Since this header has already been processed, we do not expect overflow
		headerDiff, overflow := uint256.FromBig(he.header.Difficulty)
		if overflow {
			return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", he.header.Difficulty)
		}
		if parentAnchor, found := parentAnchors[he.header.ParentHash]; found {
			parentDiff := parentDiffs[he.header.ParentHash]
			cumulativeDiff := headerDiff.Add(headerDiff, parentDiff)
			if err = hd.addHeaderAsTip(he.header, parentAnchor, *cumulativeDiff); err != nil {
				return fmt.Errorf("add header as tip: %v", err)
			}
		} else {
			// Add header as anchor
			//TODO - persist powDepth and totalDifficulty
			if parentAnchor, err = hd.addHeaderAsAnchor(he.header, hd.initPowDepth, uint256.Int{}); err != nil {
				return fmt.Errorf("add header as anchor: %v", err)
			}
			childAnchors[he.header.Hash()] = parentAnchor
			childDiffs[he.header.Hash()] = new(uint256.Int)
		}
		var header types.Header
		if _, err = io.ReadFull(he.reader, buffer[:]); err == nil {
			DeserialiseHeader(&header, buffer[:])
			he.blockHeight = header.Number.Uint64()
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
	return nil
}

func (hd *HeaderDownload) RequestMoreHeaders(currentTime, timeout uint64) []*HeaderRequest {
	if hd.requestQueue.Len() == 0 {
		return nil
	}
	var prevTopTime uint64
	if hd.requestQueue.Len() > 0 {
		prevTopTime = (*hd.requestQueue)[0].waitUntil
	}
	var requests []*HeaderRequest
	peek := (*hd.requestQueue)[0]
	for hd.requestQueue.Len() > 0 && peek.waitUntil <= currentTime {
		pop := heap.Pop(hd.requestQueue).(RequestQueueItem)
		if anchors, present := hd.anchors[pop.anchorParent]; present {
			// Anchor still exists after the timeout
			//TODO: Figure out correct request length
			requests = append(requests, &HeaderRequest{Hash: pop.anchorParent, Number: anchors[0].blockHeight - 1, Length: 256})
			pop.waitUntil = currentTime + timeout
			heap.Push(hd.requestQueue, pop)
		}
		if hd.requestQueue.Len() > 0 {
			peek = (*hd.requestQueue)[0]
		}
	}
	hd.resetRequestQueueTimer(prevTopTime, currentTime)
	return requests
}

func (hd *HeaderDownload) resetRequestQueueTimer(prevTopTime, currentTime uint64) {
	var nextTopTime uint64
	if hd.requestQueue.Len() > 0 {
		nextTopTime = (*hd.requestQueue)[0].waitUntil
	}
	if nextTopTime == prevTopTime {
		return // Nothing changed
	}
	if nextTopTime <= currentTime {
		nextTopTime = currentTime
	}
	hd.RequestQueueTimer.Stop()
	fmt.Printf("Recreating RequestQueueTimer for delay %d seconds\n", nextTopTime-currentTime)
	hd.RequestQueueTimer = time.NewTimer(time.Duration(nextTopTime-currentTime) * time.Second)
}

func (hd *HeaderDownload) FlushBuffer() error {
	// Sort the buffer first
	sort.Sort(BufferSorter(hd.buffer))
	if bufferFile, err := ioutil.TempFile(hd.filesDir, "headers-buf"); err == nil {
		if _, err = bufferFile.Write(hd.buffer); err != nil {
			bufferFile.Close()
			return err
		}
		if err = bufferFile.Close(); err != nil {
			return err
		}
		hd.buffer = hd.buffer[:0]
	} else {
		return err
	}
	return nil
}

// CheckInitiation looks at the first header in the given segment, and assuming
// that it has been added as a tip, checks whether the anchor parent hash
// associated with this tip equals to pre-set value (0x00..00 for genesis)
func (hd *HeaderDownload) CheckInitiation(segment *ChainSegment, initialHash common.Hash) bool {
	// Find attachment tip again
	tip, exists := hd.getTip(segment.headers[0].Hash())
	if !exists {
		return false
	}
	if tip.anchor.hash != initialHash {
		return false
	}
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

func (hd *HeaderDownload) getTip(tipHash common.Hash) (*Tip, bool) {
	if hardTip, ok := hd.hardTips[tipHash]; ok {
		return hardTip, true
	}
	if tipRaw, ok := hd.tips.Get(tipHash); ok {
		tip := tipRaw.(*Tip)
		return tip, true
	}
	return nil, false
}

func (hd *HeaderDownload) tipEvicted(key interface{}, value interface{}) {
	tipHash := key.(common.Hash)
	tip := value.(*Tip)
	anchor := tip.anchor
	var newTips []common.Hash
	for _, anchorTipHash := range anchor.tips {
		if anchorTipHash != tipHash {
			newTips = append(newTips, anchorTipHash)
		}
	}
	anchor.tips = newTips
}

// addHeaderAsTip adds given header as a tip belonging to a given anchorParent
func (hd *HeaderDownload) addHeaderAsTip(header *types.Header, anchor *Anchor, cumulativeDifficulty uint256.Int) error {
	diff, overflow := uint256.FromBig(header.Difficulty)
	if overflow {
		return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
	}
	tip := &Tip{
		anchor:               anchor,
		cumulativeDifficulty: cumulativeDifficulty,
		timestamp:            header.Time,
		difficulty:           *diff,
		blockHeight:          header.Number.Uint64(),
		uncleHash:            header.UncleHash,
		noPrepend:            false,
	}
	hd.tips.Add(header.Hash(), tip)
	anchor.tips = append(anchor.tips, header.Hash())
	return nil
}

// addHardCodedTip adds a hard-coded tip for which cimulative difficulty is known and no prepend is allowed
func (hd *HeaderDownload) addHardCodedTip(blockHeight uint64, timestamp uint64, hash common.Hash, anchor *Anchor, cumulativeDifficulty uint256.Int) {
	tip := &Tip{
		anchor:               anchor,
		cumulativeDifficulty: cumulativeDifficulty,
		timestamp:            timestamp,
		blockHeight:          blockHeight,
		noPrepend:            true,
	}
	hd.hardTips[hash] = tip
}

func (hd *HeaderDownload) addHeaderAsAnchor(header *types.Header, powDepth int, totalDifficulty uint256.Int) (*Anchor, error) {
	diff, overflow := uint256.FromBig(header.Difficulty)
	if overflow {
		return nil, fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
	}
	anchor := &Anchor{
		powDepth:        powDepth,
		totalDifficulty: totalDifficulty,
		difficulty:      *diff,
		timestamp:       header.Time,
		hash:            header.Hash(),
		blockHeight:     header.Number.Uint64(),
	}
	hd.anchors[header.ParentHash] = append(hd.anchors[header.ParentHash], anchor)
	return anchor, nil
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
