package stagedsync

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
	return h[i].Number.Cmp(h[j].Number) < 0
}

func (h HeadersByBlockHeight) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// HandleHeadersMsg converts message containing headers into a collection of chain segments
func (hd *HeaderDownload) HandleHeadersMsg(msg []*types.Header, peerHandle PeerHandle) ([]*ChainSegment, *PeerPenalty, error) {
	sort.Sort(HeadersByBlockHeight(msg))
	// Now all headers are order from the lowest block height to the highest
	vertices := make(map[common.Hash]*types.Header)
	treeMembership := make(map[common.Hash]*ChainSegment)
	var trees []*ChainSegment
	for _, header := range msg {
		headerHash := header.Hash()
		if _, bad := hd.badHeaders[headerHash]; bad {
			return nil, &PeerPenalty{peerHandle: peerHandle, penalty: BadBlockPenalty}, nil
		}
		if _, alreadyMember := treeMembership[headerHash]; alreadyMember {
			return nil, &PeerPenalty{peerHandle: peerHandle, penalty: DuplicateHeaderPenalty}, nil
		}
		if tree, hasEdge := treeMembership[header.ParentHash]; hasEdge {
			if edgeEnd, hasVertex := vertices[header.ParentHash]; hasVertex {
				if valid, penalty := hd.childParentValid(header, edgeEnd); !valid {
					return nil, &PeerPenalty{peerHandle: peerHandle, penalty: penalty}, nil
				}
			} else {
				return nil, nil, fmt.Errorf("unexpected condition, tree membership true but not vertex hash for header %x and parentHash %x", headerHash, header.ParentHash)
			}
			tree.headers = append(tree.headers, header)
			treeMembership[headerHash] = tree
		} else {
			tree := &ChainSegment{headers: []*types.Header{header}}
			trees = append(trees, tree)
			treeMembership[headerHash] = tree
		}
		vertices[headerHash] = header
	}
	return trees, nil, nil
}

// HandleNewBlockMsg converts message containing 1 header into one singleton chain segment
func (hd *HeaderDownload) HandleNewBlockMsg(header *types.Header, peerHandle PeerHandle) ([]*ChainSegment, *PeerPenalty, error) {
	headerHash := header.Hash()
	if _, bad := hd.badHeaders[headerHash]; bad {
		return nil, &PeerPenalty{peerHandle: peerHandle, penalty: BadBlockPenalty}, nil
	}
	return []*ChainSegment{{headers: []*types.Header{header}}}, nil, nil
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
	anchorParent := attachmentTip.anchorParent
	connectedHeaders := make(map[common.Hash]*uint256.Int)
	diff, ok := uint256.FromBig(attachmentHeader.Difficulty)
	if !ok {
		return false, nil, fmt.Errorf("could not convert attachmentHeader.Difficulty to uint256: %s", attachmentHeader.Difficulty)
	}
	cumulativeDifficulty := new(uint256.Int).Add(&attachmentTip.cumulativeDifficulty, diff)
	connectedHeaders[attachmentHeader.Hash()] = cumulativeDifficulty
	if err := hd.addHeaderAsTip(attachmentHeader, anchorParent, cumulativeDifficulty); err != nil {
		return false, nil, err
	}
	for _, header := range chainSegment.headers[attachingFrom+1:] {
		if cumDiff, connected := connectedHeaders[header.ParentHash]; connected {
			diff, ok = uint256.FromBig(header.Difficulty)
			if !ok {
				return false, nil, fmt.Errorf("could not convert header.Difficulty to uint256: %s", header.Difficulty)
			}
			newCumDiff := new(uint256.Int).Add(cumDiff, diff)
			connectedHeaders[header.Hash()] = newCumDiff
			if err := hd.addHeaderAsTip(header, anchorParent, newCumDiff); err != nil {
				return false, nil, err
			}
		}
	}
	return true, nil, nil
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

// Checks whether child-tip relationship between two headers is correct
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

func (hd *HeaderDownload) addHeaderAsTip(header *types.Header, anchorParent common.Hash, cumulativeDifficulty *uint256.Int) error {
	diff, ok := uint256.FromBig(header.Difficulty)
	if !ok {
		return fmt.Errorf("could not convert header.Difficulty to uint256: %s", header.Difficulty)
	}
	tip := &Tip{
		anchorParent:         anchorParent,
		cumulativeDifficulty: *cumulativeDifficulty,
		timestamp:            header.Time,
		difficulty:           *diff,
		blockHeight:          header.Number.Uint64(),
		uncleHash:            header.UncleHash,
		noPrepend:            false, // TODO: Check
	}
	tipItem := &TipItem{
		tipHash:              header.Hash(),
		cumulativeDifficulty: *cumulativeDifficulty,
	}
	hd.tipLimiter.ReplaceOrInsert(tipItem)
	hd.tips[header.Hash()] = tip
	// Enforce the limit
	for hd.tipLimiter.Len() > hd.tipLimit {
		deleted := hd.tipLimiter.DeleteMin()
		deletedItem := deleted.(*TipItem)
		delete(hd.tips, deletedItem.tipHash)
	}
	return nil
}
