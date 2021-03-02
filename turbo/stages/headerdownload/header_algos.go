package headerdownload

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/base64"
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
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
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
func (hd *HeaderDownload) SplitIntoSegments(headersRaw [][]byte, msg []*types.Header) ([]*ChainSegment, Penalty, error) {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	sort.Sort(HeadersByBlockHeight(msg))
	// Now all headers are order from the highest block height to the lowest
	var segments []*ChainSegment                         // Segments being built
	segmentMap := make(map[common.Hash]int)              // Mapping of the header hash to the index of the chain segment it belongs
	childrenMap := make(map[common.Hash][]*types.Header) // Mapping parent hash to the children
	dedupMap := make(map[common.Hash]struct{})           // Map used for detecting duplicate headers
	for i, header := range msg {
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
		segments[segmentIdx].HeadersRaw = append(segments[segmentIdx].HeadersRaw, headersRaw[i])
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
func (hd *HeaderDownload) SingleHeaderAsSegment(headerRaw []byte, header *types.Header) ([]*ChainSegment, Penalty, error) {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	headerHash := header.Hash()
	if _, bad := hd.badHeaders[headerHash]; bad {
		return nil, BadBlockPenalty, nil
	}
	return []*ChainSegment{{HeadersRaw: [][]byte{headerRaw}, Headers: []*types.Header{header}}}, NoPenalty, nil
}

// FindAnchors attempts to find anchors to which given chain segment can be attached to
func (hd *HeaderDownload) findAnchors(segment *ChainSegment) (found bool, start int, anchorParent common.Hash, invalidAnchors []int) {
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
func (hd *HeaderDownload) invalidateAnchors(anchorParent common.Hash, invalidAnchors []int) (tombstones []common.Hash, err error) {
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
func (hd *HeaderDownload) findTip(segment *ChainSegment, start int) (found bool, end int, penalty Penalty) {
	if _, duplicate := hd.getTip(segment.Headers[start].Hash()); duplicate {
		return false, 0, NoPenalty
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
func (hd *HeaderDownload) verifySeals(segment *ChainSegment, start, end int) error {
	for _, header := range segment.Headers[start:end] {
		if err := hd.verifySealFunc(header); err != nil {
			return err
		}
	}
	return nil
}

// ExtendUp extends a working tree up from the tip, using given chain segment
func (hd *HeaderDownload) extendUp(segment *ChainSegment, start, end int, currentTime uint64) error {
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
			if err := hd.addHeaderAsTip(header, newAnchor, cumulativeDifficulty, false /* hardCodedTip */); err != nil {
				return fmt.Errorf("extendUp addHeaderAsTip for %x: %v", header.Hash(), err)
			}
		}
		if start == 0 || end > 0 {
			// Check if the staged sync can start
			if ready, height := hd.checkInitiation(segment); ready {
				hd.stageReady = true
				hd.stageHeight = height
				// Signal at every opportunity to avoid deadlocks
				select {
				case hd.stageReadyCh <- struct{}{}:
				default:
				}
			}
		}
	} else {
		return fmt.Errorf("extendUp attachment tip not found for %x", tipHeader.ParentHash)
	}
	return nil
}

func (hd *HeaderDownload) Ready() (bool, uint64) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	return hd.stageReady, hd.stageHeight
}

func (hd *HeaderDownload) StageReadyChannel() chan struct{} {
	return hd.stageReadyCh
}

// ExtendDown extends some working trees down from the anchor, using given chain segment
// it creates a new anchor and collects all the tips from the attached anchors to it
func (hd *HeaderDownload) extendDown(segment *ChainSegment, start, end int, hardCoded bool, currentTime uint64) error {
	// Find attachement anchors again
	anchorHeader := segment.Headers[start]
	if anchors, attaching := hd.anchors[anchorHeader.Hash()]; attaching {
		newAnchorHeader := segment.Headers[end-1]
		diff, overflow := uint256.FromBig(newAnchorHeader.Difficulty)
		if overflow {
			return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", newAnchorHeader.Difficulty)
		}
		newAnchor := &Anchor{
			hardCoded:   hardCoded,
			timestamp:   newAnchorHeader.Time,
			difficulty:  *diff,
			parentHash:  newAnchorHeader.ParentHash,
			hash:        newAnchorHeader.Hash(),
			blockHeight: newAnchorHeader.Number.Uint64(),
			tipQueue:    &AnchorTipQueue{},
		}
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
			if err := hd.addHeaderAsTip(header, newAnchor, cumulativeDifficulty, false /* hardCodedTip */); err != nil {
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
func (hd *HeaderDownload) connect(segment *ChainSegment, start, end int, currentTime uint64) error {
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
		if err := hd.addHeaderAsTip(header, newAnchor, cumulativeDifficulty, false /* hardCodedTip */); err != nil {
			return fmt.Errorf("extendUp addHeaderAsTip for %x: %v", header.Hash(), err)
		}
	}
	// If we connect to the hard-coded tip, we remove it. Once there is only one hard-coded tip left, it is clear that everything is connected
	delete(hd.hardTips, tipHeader.ParentHash)
	if !hd.hardCodedPhaseDone && len(hd.hardTips) == 0 {
		fmt.Printf("=====================================================\n")
		fmt.Printf("HARD CODED PHASE DONE\n")
		fmt.Printf("=====================================================\n")
		hd.hardCodedPhaseDone = true
	}
	return nil
}

func (hd *HeaderDownload) newAnchor(segment *ChainSegment, start, end int, currentTime uint64) error {
	anchorHeader := segment.Headers[end-1]
	var anchor *Anchor
	var err error
	if anchor, err = hd.addHeaderAsAnchor(anchorHeader, false /* hardCoded */); err != nil {
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
		if err = hd.addHeaderAsTip(header, anchor, cumulativeDifficulty, false /* hardCodeTips */); err != nil {
			return fmt.Errorf("newAnchor addHeaderAsTip for %x: %v", header.Hash(), err)
		}
	}
	if anchorHeader.ParentHash != hd.initialHash {
		hd.requestQueue.PushFront(RequestQueueItem{anchorParent: anchorHeader.ParentHash, waitUntil: currentTime})
	}
	return nil
}

func (hd *HeaderDownload) hardCodedHeader(header *types.Header, currentTime uint64) error {
	if anchor, err := hd.addHeaderAsAnchor(header, true /* hardCoded */); err == nil {
		diff, overflow := uint256.FromBig(header.Difficulty)
		if overflow {
			return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
		}
		tip := &Tip{
			anchor:               anchor,
			cumulativeDifficulty: *diff,
			blockHeight:          header.Number.Uint64(),
			difficulty:           *diff,
			header:               header,
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
func (hd *HeaderDownload) addSegmentToBuffer(segment *ChainSegment, start, end int) {
	/*
		if end > start {
			fmt.Printf("Adding segment [%d-%d] to the buffer\n", segment.Headers[end-1].Number.Uint64(), segment.Headers[start].Number.Uint64())
		}
	*/
	for i, headerRaw := range segment.HeadersRaw[start:end] {
		hd.buffer.AddHeader(headerRaw, segment.Headers[start+i].Number.Uint64())
		hd.headersAdded++
	}
}

func (hd *HeaderDownload) AddHeaderToBuffer(headerRaw []byte, blockHeight uint64) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	hd.buffer.AddHeader(headerRaw, blockHeight)
	hd.headersAdded++
}

func (hd *HeaderDownload) AnchorState() string {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	return hd.anchorState()
}

func (hd *HeaderDownload) anchorState() string {
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
	rlpStream   *rlp.Stream
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

func InitHardCodedTips(network string) map[common.Hash]HeaderRecord {
	var encodings []string
	switch network {
	case "mainnet":
		encodings = mainnetHardCodedHeaders
	default:
		log.Error("Hard coded headers not found for", "network", network)
		return nil
	}

	// Insert hard-coded headers if present
	return DecodeTips(encodings)
}

func DecodeTips(encodings []string) map[common.Hash]HeaderRecord {
	hardTips := make(map[common.Hash]HeaderRecord, len(encodings))

	for _, encoding := range encodings {
		b, err := base64.RawStdEncoding.DecodeString(encoding)
		if err != nil {
			log.Error("Parsing hard coded header", "error", err)
		} else {
			var h types.Header
			if err := rlp.DecodeBytes(b, &h); err != nil {
				log.Error("Parsing hard coded header", "error", err)
			} else {
				hardTips[h.Hash()] = HeaderRecord{Raw: b, Header: &h}
			}
		}
	}

	return hardTips
}

func (hd *HeaderDownload) SetHardCodedTips(hardTips map[common.Hash]HeaderRecord) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	var maxHeight uint64
	for tipHash, headerRecord := range hardTips {
		height := headerRecord.Header.Number.Uint64()
		if height <= hd.highestInDb {
			// No need for this hard coded header anymore
			continue
		}
		if height <= hd.maxHardTipHeight {
			continue
		}
		if height > maxHeight {
			maxHeight = height
		}
		if err := hd.hardCodedHeader(headerRecord.Header, uint64(time.Now().Unix())); err != nil {
			log.Error("Failed to insert hard coded header", "block number", height, "error", err)
		} else {
			hd.buffer.AddHeader(headerRecord.Raw, height)
		}
		hd.hardTips[tipHash] = headerRecord
	}
	if maxHeight > hd.maxHardTipHeight {
		hd.maxHardTipHeight = maxHeight
	}
	if len(hd.hardTips) == 0 {
		hd.hardCodedPhaseDone = true
	}
}

func ReadFilesAndBuffer(files []string, headerBuf *HeaderBuffer, hf func(header *types.Header, blockHeight uint64) error) error {
	//nolint:prealloc
	var fs []*os.File
	//nolint:prealloc
	var rs []*rlp.Stream
	// Open all files and only read anchor sequences to decide which one has the latest information about the anchors
	for _, filename := range files {
		f, err1 := os.Open(filename)
		if err1 != nil {
			return fmt.Errorf("open file %s: %v", filename, err1)
		}
		r := bufio.NewReader(f)
		fs = append(fs, f)
		rs = append(rs, rlp.NewStream(r, 0 /* no limit */))
	}
	if headerBuf != nil {
		sort.Sort(headerBuf)
		fs = append(fs, nil)
		rs = append(rs, rlp.NewStream(headerBuf, 0 /* no limit */))
	}
	defer func() {
		for _, f := range fs {
			if f != nil {
				//lint:noerrcheck
				f.Close()
			}
		}
	}()
	h := &Heap{}
	heap.Init(h)
	for i, f := range fs {
		rlpStream := rs[i]
		var header types.Header
		if err := rlpStream.Decode(&header); err != nil {
			if !errors.Is(err, io.EOF) {
				return fmt.Errorf("reading header from file 1: %w", err)
			}
			continue
		}
		he := HeapElem{file: f, rlpStream: rlpStream, blockHeight: header.Number.Uint64(), hash: header.Hash(), header: &header}
		heap.Push(h, he)
	}
	for h.Len() > 0 {
		he := (heap.Pop(h)).(HeapElem)
		if err := hf(he.header, he.blockHeight); err != nil {
			return err
		}
		var header types.Header
		if err := he.rlpStream.Decode(&header); err == nil {
			he.blockHeight = header.Number.Uint64()
			he.hash = header.Hash()
			he.header = &header
			heap.Push(h, he)
		} else {
			if !errors.Is(err, io.EOF) {
				return fmt.Errorf("reading header from file: %w", err)
			}
			if he.file != nil {
				if err = he.file.Close(); err != nil {
					return fmt.Errorf("closing file: %w", err)
				}
			}
		}
	}
	return nil
}

func (hd *HeaderDownload) RecoverFromDb(db ethdb.Database, currentTime uint64) error {
	var anchor *Anchor
	err := db.(ethdb.HasKV).KV().View(context.Background(), func(tx ethdb.Tx) error {
		c := tx.Cursor(dbutils.HeaderPrefix)
		var anchorH types.Header
		// Take first header (with the lowest height) as the anchor
		for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
			if err != nil {
				return err
			}
			if len(k) == 40 {
				// This is header record
				if err = rlp.DecodeBytes(v, &anchorH); err != nil {
					return err
				}
				break
			}
		}
		// Take hd.tipLimit headers (with the highest heights) as tips
		for k, v, err := c.Last(); k != nil && hd.tipCount < hd.tipLimit; k, v, err = c.Prev() {
			if err != nil {
				return err
			}
			if len(k) != 40 {
				continue
			}
			var h types.Header
			if err = rlp.DecodeBytes(v, &h); err != nil {
				return err
			}
			var td *big.Int
			if td, err = rawdb.ReadTd(db, h.Hash(), h.Number.Uint64()); err != nil {
				return err
			}
			cumulativeDiff, overflow := uint256.FromBig(td)
			if overflow {
				return fmt.Errorf("overflow of difficulty: %d", td)
			}
			if anchor == nil {
				if anchor, err = hd.addHeaderAsAnchor(&anchorH, true /* hardCoded */); err != nil {
					return err
				}
			}
			if err = hd.addHeaderAsTip(&h, anchor, *cumulativeDiff, false /* hardCodedTip */); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	hd.highestInDb, err = stages.GetStageProgress(db, stages.Headers)
	if err != nil {
		return err
	}
	return nil
}

func (hd *HeaderDownload) RecoverFromFiles(currentTime uint64, hardTips map[common.Hash]HeaderRecord) error {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	if _, err := os.Stat(hd.filesDir); os.IsNotExist(err) {
		log.Warn("Temp file directory does not exist, will be created", "path", hd.filesDir)
		if err1 := os.MkdirAll(hd.filesDir, os.ModePerm); err1 != nil {
			return fmt.Errorf("could not create temp directory: %w", err1)
		}
	}
	fileInfos, err := ioutil.ReadDir(hd.filesDir)
	if err != nil {
		return err
	}
	var files = make([]string, len(fileInfos))
	for i, fileInfo := range fileInfos {
		files[i] = path.Join(hd.filesDir, fileInfo.Name())
	}
	var prevHeight uint64
	var parentAnchors = make(map[common.Hash]*Anchor)
	var parentDiffs = make(map[common.Hash]*uint256.Int)
	var childAnchors = make(map[common.Hash]*Anchor)
	var childDiffs = make(map[common.Hash]*uint256.Int)
	var prevHash common.Hash // Hash of previously seen header - to filter out potential duplicates
	var tips = make(map[common.Hash]struct{})
	var maxHardTipHeight uint64
	for _, headerRecord := range hardTips {
		if headerRecord.Header.Number.Uint64() > maxHardTipHeight {
			maxHardTipHeight = headerRecord.Header.Number.Uint64()
		}
	}
	var maxHeight uint64
	if err = ReadFilesAndBuffer(files, nil,
		func(header *types.Header, blockHeight uint64) error {
			hash := header.Hash()
			if hash == prevHash {
				//fmt.Printf("Duplicate header: %d %x\n", header.Number.Uint64(), hash)
				return nil
			}
			if blockHeight > prevHeight {
				// Clear out parent map and move childMap to its place
				if blockHeight != prevHeight+1 {
					// Skipping the level, so no connection between grand-parents and grand-children
					parentAnchors = make(map[common.Hash]*Anchor)
					parentDiffs = make(map[common.Hash]*uint256.Int)
				} else {
					parentAnchors = childAnchors
					parentDiffs = childDiffs
				}
				childAnchors = make(map[common.Hash]*Anchor)
				childDiffs = make(map[common.Hash]*uint256.Int)
				prevHeight = blockHeight
			} else if blockHeight < prevHeight {
				panic("files were not sorted")
			}
			// Since this header has already been processed, we do not expect overflow
			cumulativeDiff, overflow := uint256.FromBig(header.Difficulty)
			if overflow {
				return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
			}
			_, hard := hardTips[hash]
			parentHash := header.ParentHash
			delete(tips, parentHash)
			tips[hash] = struct{}{}
			if parentAnchor, found := parentAnchors[parentHash]; found {
				parentDiff := parentDiffs[parentHash]
				cumulativeDiff.Add(cumulativeDiff, parentDiff)
				if err = hd.addHeaderAsTip(header, parentAnchor, *cumulativeDiff, hard); err != nil {
					return fmt.Errorf("add header as tip: %v", err)
				}
				childAnchors[hash] = parentAnchor
				childDiffs[hash] = cumulativeDiff
			} else {
				anchor := &Anchor{hardCoded: blockHeight <= maxHardTipHeight, hash: hash, tipQueue: &AnchorTipQueue{}, anchorID: hd.nextAnchorID}
				hd.nextAnchorID++
				heap.Init(anchor.tipQueue)
				//fmt.Printf("Undeclared anchor for %d %x, inserting as empty to parentHash %x\n", blockHeight, hash, parentHash)
				diff, overflow := uint256.FromBig(header.Difficulty)
				if overflow {
					return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
				}
				anchor.difficulty = *diff
				anchor.timestamp = header.Time
				anchor.blockHeight = header.Number.Uint64()
				if err = hd.addHeaderAsTip(header, anchor, *cumulativeDiff, hard); err != nil {
					return fmt.Errorf("add header as tip: %v", err)
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
			if blockHeight > maxHeight {
				maxHeight = blockHeight
			}
			prevHash = hash
			return nil
		}); err != nil {
		return err
	}
	if maxHeight > hd.maxHardTipHeight {
		hd.maxHardTipHeight = maxHeight
	}
	for tipHash := range tips {
		if headerRecord, ok := hardTips[tipHash]; ok {
			hd.hardTips[tipHash] = headerRecord
			//fmt.Printf("Adding %d %x to hard-coded tips\n", headerRecord.Header.Number.Uint64(), tipHash)
		}
	}
	hd.files = files
	return nil
}

func (hd *HeaderDownload) RequestMoreHeaders(currentTime, timeout uint64) ([]*HeaderRequest, *time.Timer) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	if hd.requestQueue.Len() == 0 {
		return nil, hd.RequestQueueTimer
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
	return requests, hd.RequestQueueTimer
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

func (hd *HeaderDownload) flushBuffer() error {
	if len(hd.buffer.buffer) < hd.bufferLimit {
		// Not flushing the buffer unless it is full
		return nil
	}
	// Sort the buffer first
	sort.Sort(hd.buffer)
	if bufferFile, err := ioutil.TempFile(hd.filesDir, "headers-buf"); err == nil {
		if err = hd.buffer.Flush(bufferFile); err != nil {
			bufferFile.Close()
			return err
		}
		if err = bufferFile.Close(); err != nil {
			return err
		}
		hd.files = append(hd.files, bufferFile.Name())
	} else {
		return err
	}
	//fmt.Printf("Successfully flushed the buffer\n")
	return nil
}

func (hd *HeaderDownload) Progress() (files int, buffer int, headersAdded int) {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
	return len(hd.files), len(hd.buffer.buffer), hd.headersAdded
}

func (hd *HeaderDownload) PrepareStageData() (files []string, buffer *HeaderBuffer) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	if !hd.stageReady {
		return nil, nil
	}
	files = hd.files
	hd.files = nil
	buffer = hd.buffer
	hd.buffer, hd.anotherBuffer = hd.anotherBuffer, hd.buffer
	hd.buffer.Clear()
	hd.stageReady = false
	return
}

// CheckInitiation looks at the first header in the given segment, and assuming
// that it has been added as a tip, checks whether the anchor parent hash
// associated with this tip equals to pre-set value (0x00..00 for genesis)
func (hd *HeaderDownload) checkInitiation(segment *ChainSegment) (bool, uint64) {
	tipHash := segment.Headers[0].Hash()
	tip, exists := hd.getTip(tipHash)
	if !exists {
		//fmt.Printf("checkInitialisation: tipHash %x does not exist\n", tipHash)
		return false, 0
	}
	if tip.anchor.parentHash != hd.initialHash {
		return false, 0
	}
	//fmt.Printf("Tip %d %x has total difficulty %d, highest %d, len(hd.hardTips) %d\n", tip.blockHeight, tipHash, tip.cumulativeDifficulty.ToBig(), hd.highestTotalDifficulty.ToBig(), len(hd.hardTips))
	/*
		if len(hd.hardTips) > 0 {
			fmt.Printf("Hard tips:")
			for _, headerRecord := range hd.hardTips {
				fmt.Printf(" %d", headerRecord.Header.Number.Uint64())
			}
			fmt.Printf("\n")
		}
	*/
	if tip.cumulativeDifficulty.Gt(&hd.highestTotalDifficulty) {
		hd.highestTotalDifficulty.Set(&tip.cumulativeDifficulty)
		return len(hd.hardTips) == 0, tip.blockHeight
	}
	return false, 0
}

// childTipValid checks whether child-tip relationship between child header and a tip (that is being extended), is correct
// (excluding Proof Of Work validity)
func (hd *HeaderDownload) childTipValid(child *types.Header, tipHash common.Hash, tip *Tip) (bool, Penalty) {
	if tip.blockHeight+1 != child.Number.Uint64() {
		return false, WrongChildBlockHeightPenalty
	}
	childDifficulty := hd.calcDifficultyFunc(child.Time, tip.header.Time, tip.difficulty.ToBig(), big.NewInt(int64(tip.blockHeight)), tipHash, tip.header.UncleHash)
	if child.Difficulty.Cmp(childDifficulty) != 0 {
		return false, WrongChildDifficultyPenalty
	}
	return true, NoPenalty
}

func (hd *HeaderDownload) HasTip(tipHash common.Hash) bool {
	hd.lock.RLock()
	defer hd.lock.RUnlock()
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
func (hd *HeaderDownload) addHeaderAsTip(header *types.Header, anchor *Anchor, cumulativeDifficulty uint256.Int, hardCodedTip bool) error {
	diff, overflow := uint256.FromBig(header.Difficulty)
	if overflow {
		return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
	}
	height := header.Number.Uint64()
	tipHash := header.Hash()
	hd.anchorTree.Delete(anchor)
	if height > hd.maxHardTipHeight || hardCodedTip {
		tip := &Tip{
			anchor:               anchor,
			cumulativeDifficulty: cumulativeDifficulty,
			difficulty:           *diff,
			blockHeight:          height,
			header:               header,
		}
		hd.tips[tipHash] = tip
		heap.Push(anchor.tipQueue, AnchorTipItem{hash: tipHash, height: height, hard: hardCodedTip})
		hd.tipCount++
	}
	if height > anchor.maxTipHeight {
		anchor.maxTipHeight = height
	}
	hd.anchorTree.ReplaceOrInsert(anchor)
	hd.limitTips()
	return nil
}

func (hd *HeaderDownload) addHeaderAsAnchor(header *types.Header, hardCoded bool) (*Anchor, error) {
	diff, overflow := uint256.FromBig(header.Difficulty)
	if overflow {
		return nil, fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
	}
	anchor := &Anchor{
		hardCoded:   hardCoded,
		difficulty:  *diff,
		timestamp:   header.Time,
		parentHash:  header.ParentHash,
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
	//if anchor.blockHeight != parent.Number.Uint64()+1 {
	//fmt.Printf("anchor.blockHeight(%d) != parent.Number+1(%d)\n", anchor.blockHeight, parent.Number.Uint64()+1)
	//	return false
	//}
	childDifficulty := hd.calcDifficultyFunc(anchor.timestamp, parent.Time, parent.Difficulty, parent.Number, parent.Hash(), parent.UncleHash)
	//if anchor.difficulty.ToBig().Cmp(childDifficulty) != 0 {
	//fmt.Printf("anchor.difficulty (%s) != childDifficulty (%s)\n", anchor.difficulty.ToBig(), childDifficulty)
	//}
	return anchor.difficulty.ToBig().Cmp(childDifficulty) == 0
}

func (hi *HeaderInserter) FeedHeader(header *types.Header, blockHeight uint64) error {
	hash := header.Hash()
	if hash == hi.prevHash {
		// Skip duplicates
		return nil
	}
	if blockHeight < hi.prevHeight {
		return fmt.Errorf("[%s] headers are unexpectedly unsorted, got %d after %d", hi.logPrefix, blockHeight, hi.prevHeight)
	}
	if oldH := rawdb.ReadHeader(hi.batch, hash, blockHeight); oldH != nil {
		// Already inserted, skip
		return nil
	}
	// Load parent header
	parent := rawdb.ReadHeader(hi.batch, header.ParentHash, blockHeight-1)
	if parent == nil {
		log.Error(fmt.Sprintf("Could not find parent with hash %x and height %d for header %x %d", header.ParentHash, blockHeight-1, hash, blockHeight))
		// Skip headers without parents
		return nil
	}
	// Parent's total difficulty
	parentTd, err := rawdb.ReadTd(hi.batch, header.ParentHash, blockHeight-1)
	if err != nil {
		return fmt.Errorf("[%s] parent's total difficulty not found with hash %x and height %d for header %x %d: %v", hi.logPrefix, header.ParentHash, blockHeight-1, hash, blockHeight, err)
	}
	// Calculate total difficulty of this header using parent's total difficulty
	td := new(big.Int).Add(parentTd, header.Difficulty)
	// Now we can decide wether this header will create a change in the canonical head
	if td.Cmp(hi.localTd) > 0 {
		hi.newCanonical = true
		// Find the forking point - i.e. the latest header on the canonical chain which is an ancestor of this one
		// Most common case - forking point is the height of the parent header
		var forkingPoint uint64
		ch, err1 := rawdb.ReadCanonicalHash(hi.batch, blockHeight-1)
		if err1 != nil {
			return fmt.Errorf("reading canonical hash for height %d: %w", blockHeight-1, err1)
		}
		if ch == header.ParentHash {
			forkingPoint = blockHeight - 1
		} else {
			// Going further back
			ancestorHash := parent.ParentHash
			ancestorHeight := blockHeight - 2
			for ch, err = rawdb.ReadCanonicalHash(hi.batch, ancestorHeight); err == nil && ch != ancestorHash; ch, err = rawdb.ReadCanonicalHash(hi.batch, ancestorHeight) {
				ancestor := rawdb.ReadHeader(hi.batch, ancestorHash, ancestorHeight)
				ancestorHash = ancestor.ParentHash
				ancestorHeight--
			}
			if err != nil {
				return fmt.Errorf("[%s] reading canonical hash for %d: %w", hi.logPrefix, ancestorHeight, err)
			}
			// Loop above terminates when either err != nil (handled already) or ch == ancestorHash, therefore ancestorHeight is our forking point
			forkingPoint = ancestorHeight
		}
		if err = rawdb.WriteHeadHeaderHash(hi.batch, hash); err != nil {
			return fmt.Errorf("[%s] marking head header hash as %x: %w", hi.logPrefix, hash, err)
		}
		// See if the forking point affects the unwindPoint (the block number to which other stages will need to unwind before the new canonical chain is applied)
		if forkingPoint < hi.unwindPoint {
			hi.unwindPoint = forkingPoint
		}
	}
	data, err2 := rlp.EncodeToBytes(header)
	if err2 != nil {
		return fmt.Errorf("[%s] failed to RLP encode header: %w", hi.logPrefix, err2)
	}
	if err = rawdb.WriteTd(hi.batch, hash, blockHeight, td); err != nil {
		return fmt.Errorf("[%s] failed to WriteTd: %w", hi.logPrefix, err)
	}
	if err = hi.batch.Put(dbutils.HeaderPrefix, dbutils.HeaderKey(blockHeight, hash), data); err != nil {
		return fmt.Errorf("[%s] failed to store header: %w", hi.logPrefix, err)
	}
	if blockHeight > hi.headerProgress {
		hi.headerProgress = blockHeight
		if err = stages.SaveStageProgress(hi.batch, stages.Headers, blockHeight); err != nil {
			return fmt.Errorf("[%s] saving Headers progress: %w", hi.logPrefix, err)
		}
	}
	hi.prevHash = hash
	if blockHeight > hi.highest {
		hi.highest = blockHeight
	}
	return nil
}

func (hi *HeaderInserter) GetHighest() uint64 {
	return hi.highest
}

func (hi *HeaderInserter) UnwindPoint() uint64 {
	return hi.unwindPoint
}

//nolint:interfacer
func (hd *HeaderDownload) ProcessSegment(segment *ChainSegment) {
	hd.lock.Lock()
	defer hd.lock.Unlock()
	log.Info("processSegment", "from", segment.Headers[0].Number.Uint64(), "to", segment.Headers[len(segment.Headers)-1].Number.Uint64())
	foundAnchor, start, anchorParent, invalidAnchors := hd.findAnchors(segment)
	if len(invalidAnchors) > 0 {
		if _, err := hd.invalidateAnchors(anchorParent, invalidAnchors); err != nil {
			log.Error("Invalidation of anchor failed", "error", err)
		}
		log.Warn(fmt.Sprintf("Invalidated anchors %v for %x", invalidAnchors, anchorParent))
	}
	foundTip, end, penalty := hd.findTip(segment, start) // We ignore penalty because we will check it as part of PoW check
	if penalty != NoPenalty {
		log.Error(fmt.Sprintf("FindTip penalty %d", penalty))
		return
	}
	if end == 0 {
		log.Info("Duplicate segment")
		return
	}
	currentTime := uint64(time.Now().Unix())
	if hd.hardCodedPhaseDone {
		if err := hd.verifySeals(segment, start, end); err != nil {
			log.Error("VerifySeals", "error", err)
			return
		}
	}
	if err := hd.flushBuffer(); err != nil {
		log.Error("Could not flush the buffer, will discard the data", "error", err)
		return
	}
	if !hd.hardCodedPhaseDone && !foundAnchor {
		// During the first phase (downloading hard-coded headers), only extension from an anchor is allowed
		log.Info("Only anchor extensions allowed in the first phase", "from", segment.Headers[0].Number.Uint64(), "to", segment.Headers[len(segment.Headers)-1].Number.Uint64())
		return
	}
	// There are 4 cases
	if foundAnchor {
		if foundTip {
			// Connect
			if err1 := hd.connect(segment, start, end, currentTime); err1 != nil {
				log.Error("Connect failed", "error", err1)
			} else {
				hd.addSegmentToBuffer(segment, start, end)
				log.Info("Connected", "start", start, "end", end)
			}
		} else {
			// ExtendDown
			if err1 := hd.extendDown(segment, start, end, !hd.hardCodedPhaseDone, currentTime); err1 != nil {
				log.Error("ExtendDown failed", "error", err1)
			} else {
				hd.addSegmentToBuffer(segment, start, end)
				log.Info("Extended Down", "start", start, "end", end)
			}
		}
	} else if foundTip {
		if end > 0 {
			// ExtendUp
			if err1 := hd.extendUp(segment, start, end, currentTime); err1 != nil {
				log.Error("ExtendUp failed", "error", err1)
			} else {
				hd.addSegmentToBuffer(segment, start, end)
				log.Info("Extended Up", "start", start, "end", end)
			}
		}
	} else {
		// NewAnchor
		if err1 := hd.newAnchor(segment, start, end, currentTime); err1 != nil {
			log.Error("NewAnchor failed", "error", err1)
		} else {
			hd.addSegmentToBuffer(segment, start, end)
			log.Info("NewAnchor", "start", start, "end", end)
		}
	}
	log.Info(hd.anchorState())
}
