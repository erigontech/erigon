package headerdownload

import (
	"fmt"
	"math/big"
	"sort"

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
func (hd *HeaderDownload) FindAnchors(segment *ChainSegment) (found bool, start int, invalidAnchors []int) {
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
			return true, i, invalidAnchors
		}
	}
	return false, 0, nil
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
						tipItem := &TipItem{tipHash: tipHash, cumulativeDifficulty: hd.tips[tipHash].cumulativeDifficulty}
						delete(hd.tips, tipHash)
						hd.tipLimiter.Delete(tipItem)
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
		if tip, attaching := hd.tips[header.ParentHash]; attaching {
			// Before attaching, we must check the parent-child relationship
			if valid, penalty := hd.childTipValid(header, header.ParentHash, tip); !valid {
				return true, i + 1, penalty
			}
			return true, i + 1, NoPenalty
		}
	}
	return false, len(segment.headers), NoPenalty
}

// VerifySeals verifies Proof Of Work for the part of the given chain segement
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
	attachingHeader := segment.headers[end-1]
	if attachmentTip, attaching := hd.tips[attachingHeader.ParentHash]; attaching {
		if attachmentTip.noPrepend {
			return fmt.Errorf("extendUp attachment tip had noPrepend flag on for %x", attachingHeader.ParentHash)
		}
		newAnchor := attachmentTip.anchor
		for _, header := range segment.headers[start:end] {
			// TODO: compute cumulative difficulty of the tip
			if err := hd.addHeaderAsTip(header, newAnchor, nil); err != nil {
				return fmt.Errorf("extendUp addHeaderAsTip for %x: %v", header.Hash(), err)
			}
		}
	} else {
		return fmt.Errorf("extendUp attachment tip not found for %x", attachingHeader.ParentHash)
	}
	return nil
}

// ExtendDown extends some working trees down from the anchor, using given chain segment
func (hb *HeaderDownload) ExtendDown(segment *ChainSegment, start, end int) error {
	return nil
}

// Connect connects some working trees using anchors of some, and a tip of another
func (hb *HeaderDownload) Connect(segment *ChainSegment, start, end int) error {
	return nil
}

// Prepend attempts to find a suitable tip within the working chain segments to prepend the given (new) chain segment to
// The first return value is true if the prepending was done, false if a suitable tip was not found, or there is a
// penalty or error
func (hd *HeaderDownload) Prepend(chainSegment *ChainSegment, peerHandle PeerHandle) (bool, *PeerPenalty, error) {
	if len(chainSegment.headers) == 0 {
		return false, nil, fmt.Errorf("chainSegment must not be empty for Prepend, len %d", len(chainSegment.headers))
	}
	// Attachment may happen not just via the root (first element of the chainSegment), but via
	// any other header in the chainSegement, but the headers closer to the root are preferred
	// Since the headers in the chainSegment are topologically sorted, we will be checking their
	// potential attachment in their order, which will satisfy the preference described above.
	// We do not break out of the loop as soon as we found a suitable attachment point,
	// because we also need to verify that we are not trying to attach to a hard-coded
	// chain segment
	var attachmentTip *Tip
	var attachmentHeader *types.Header
	var attachingFrom int // Index of the header in the chainSegment.headers that we will be attaching from
	for i, header := range chainSegment.headers {
		if tip, attaching := hd.tips[header.ParentHash]; attaching {
			if tip.noPrepend {
				// We hit the hard-coded segment, there is no point checking other attachment points
				return false, nil, nil
			}
			// Before attaching, we must check the parent-child relationship
			if valid, penalty := hd.childTipValid(header, header.ParentHash, tip); !valid {
				return false, &PeerPenalty{peerHandle: peerHandle, penalty: penalty}, nil
			}
			if attachmentTip == nil {
				// No overwriting of a tip that already found, to make sure we prefer to attach from
				// a header closest to the root (or root itself)
				attachmentTip = tip
				attachmentHeader = header
				attachingFrom = i
			}
		}
		if attachmentTip != nil {
			if err := hd.verifySealFunc(header); err != nil {
				return false, &PeerPenalty{peerHandle: peerHandle, penalty: InvalidSealPenalty, err: err}, nil
			}
		}
	}
	if attachmentTip == nil {
		return false, nil, nil
	}
	// Go through the headers again, and filter out the headers that are not connected to the attachment header
	anchor := attachmentTip.anchor
	connectedHeaders := make(map[common.Hash]*uint256.Int)
	diff, overflow := uint256.FromBig(attachmentHeader.Difficulty)
	if overflow {
		return false, nil, fmt.Errorf("overflow when converting attachmentHeader.Difficulty to uint256: %s", attachmentHeader.Difficulty)
	}
	cumulativeDifficulty := new(uint256.Int).Add(&attachmentTip.cumulativeDifficulty, diff)
	connectedHeaders[attachmentHeader.Hash()] = cumulativeDifficulty
	if err := hd.addHeaderAsTip(attachmentHeader, anchor, cumulativeDifficulty); err != nil {
		return false, nil, err
	}
	for _, header := range chainSegment.headers[attachingFrom+1:] {
		if cumDiff, connected := connectedHeaders[header.ParentHash]; connected {
			diff, overflow = uint256.FromBig(header.Difficulty)
			if overflow {
				return false, nil, fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
			}
			newCumDiff := new(uint256.Int).Add(cumDiff, diff)
			connectedHeaders[header.Hash()] = newCumDiff
			if err := hd.addHeaderAsTip(header, anchor, newCumDiff); err != nil {
				return false, nil, err
			}
		}
	}
	return true, nil, nil
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

// addHeaderAsTip adds given header as a tip belonging to a given anchorParent
func (hd *HeaderDownload) addHeaderAsTip(header *types.Header, anchor *Anchor, cumulativeDifficulty *uint256.Int) error {
	diff, overflow := uint256.FromBig(header.Difficulty)
	if overflow {
		return fmt.Errorf("overflow when converting header.Difficulty to uint256: %s", header.Difficulty)
	}
	tip := &Tip{
		anchor:               anchor,
		cumulativeDifficulty: *cumulativeDifficulty,
		timestamp:            header.Time,
		difficulty:           *diff,
		blockHeight:          header.Number.Uint64(),
		uncleHash:            header.UncleHash,
		noPrepend:            false,
	}
	hd.tips[header.Hash()] = tip
	tipItem := &TipItem{
		tipHash:              header.Hash(),
		cumulativeDifficulty: *cumulativeDifficulty,
	}
	hd.tipLimiter.ReplaceOrInsert(tipItem)
	// Enforce the limit
	for hd.tipLimiter.Len() > hd.tipLimit {
		deleted := hd.tipLimiter.DeleteMin()
		deletedItem := deleted.(*TipItem)
		delete(hd.tips, deletedItem.tipHash)
	}
	return nil
}

// addHardCodedTip adds a hard-coded tip for which cimulative difficulty is known and no prepend is allowed
func (hd *HeaderDownload) addHardCodedTip(blockHeight uint64, timestamp uint64, hash common.Hash, anchor *Anchor, cumulativeDifficulty *uint256.Int) error {
	tip := &Tip{
		anchor:               anchor,
		cumulativeDifficulty: *cumulativeDifficulty,
		timestamp:            timestamp,
		blockHeight:          blockHeight,
		noPrepend:            true,
	}
	hd.tips[hash] = tip
	return nil
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

// Append attempts to find a suitable anchor within the working chain segments to append the given (new) chain segment to
// The first return value is true if the appending was done, false if a anchor tip was not found, or there is a
// penalty or error
func (hd *HeaderDownload) Append(chainSegment *ChainSegment) (bool, []common.Hash, error) {
	if len(chainSegment.headers) == 0 {
		return false, nil, fmt.Errorf("chainSegment must not be empty for Append, len %d", len(chainSegment.headers))
	}
	var newAnchor *Anchor
	//totalDifficulties := make(map[common.Hash]uint256.Int)
	var tombstones []common.Hash
	for i := len(chainSegment.headers) - 1; i >= 0; i-- {
		header := chainSegment.headers[i]
		anchorParent := header.Hash()
		if anchors, attaching := hd.anchors[anchorParent]; attaching {
			var removedAnchors []int
			for anchorIdx, anchor := range anchors {
				if valid := hd.anchorParentValid(anchor, header); valid {
					if newAnchor == nil {
						newPowDepth := anchor.powDepth
						heightDiff := int(anchor.blockHeight - chainSegment.headers[0].Number.Uint64())
						if newPowDepth > heightDiff {
							newPowDepth -= heightDiff
						} else {
							newPowDepth = 0
						}
						//newTotalDifficulty := anchor.totalDifficulty

						newAnchor = &Anchor{
							powDepth: newPowDepth,
						}
					}
				} else {
					// Invalidate the entire chain segment that starts at anchor
					for _, tipHash := range anchor.tips {
						tipItem := &TipItem{tipHash: tipHash, cumulativeDifficulty: hd.tips[tipHash].cumulativeDifficulty}
						delete(hd.tips, tipHash)
						hd.tipLimiter.Delete(tipItem)
					}
					tombstones = append(tombstones, anchor.hash)
					removedAnchors = append(removedAnchors, anchorIdx)
				}
			}
			if len(removedAnchors) > 0 {
				j := 0
				var filteredAnchors []*Anchor
				for k, anchor := range anchors {
					if j < len(removedAnchors) && removedAnchors[j] == k {
						// Skip this one
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
			}
		}
	}
	return false, tombstones, nil
}

// anchorParentValid checks whether child-parent relationship between an anchor and
// its extension (parent) is correct
// (excluding Proof Of Work validity)
func (hd *HeaderDownload) anchorParentValid(anchor *Anchor, parent *types.Header) bool {
	if anchor.blockHeight+1 != parent.Number.Uint64() {
		//TODO: Log the reason
		return false
	}
	childDifficulty := hd.calcDifficultyFunc(anchor.timestamp, parent.Time, parent.Difficulty, parent.Number, parent.Hash(), parent.UncleHash)
	if anchor.difficulty.ToBig().Cmp(childDifficulty) != 0 {
		//TODO: Log the reason
		return false
	}
	return true
}
