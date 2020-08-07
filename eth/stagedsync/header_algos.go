package stagedsync

import (
	"fmt"
	"sort"

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

// Checks whether child-parent relationship between two headers is correct
// (excluding Proof Of Work validity)
func (hd *HeaderDownload) childParentValid(child, parent *types.Header) (bool, Penalty) {
	if parent.Number.Uint64()+1 != child.Number.Uint64() {
		return false, WrongChildBlockHeightPenalty
	}
	childDifficulty := hd.calcDifficultyFunc(child.Time, parent)
	if child.Difficulty.Cmp(childDifficulty) != 0 {
		return false, WrongChildDifficultyPenalty
	}
	return true, NoPenalty
}
