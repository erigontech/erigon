package forkchoice

import (
	"bytes"
	"sort"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

func (f *ForkChoiceStore) GetHead() libcommon.Hash {
	head := f.justifiedCheckpoint.Root
	blocks := f.getFilteredBlockTree(head)
	justifiedSlot := f.computeStartSlotAtEpoch(f.justifiedCheckpoint.Epoch)

	for {
		// Filter out current head children.
		unfilteredChildren := f.forkGraph.GetChildren(head)
		children := []libcommon.Hash{}
		for _, child := range unfilteredChildren {
			if _, ok := blocks[child]; ok {
				children = append(children, child)
			}
		}
		// Stop if we dont have any more children
		if len(children) == 0 {
			return head
		}
		// Sort children by lexigographical order
		sort.Slice(children, func(i, j int) bool {
			childA := children[i]
			childB := children[j]
			return bytes.Compare(childA[:], childB[:]) < 0
		})
		// After sorting is done determine best fit.

	}
	return libcommon.Hash{}
}

// getFilteredBlockTree filters out dumb blocks.
func (f *ForkChoiceStore) getFilteredBlockTree(base libcommon.Hash) map[libcommon.Hash]*cltypes.BeaconBlockHeader {
	blocks := make(map[libcommon.Hash]*cltypes.BeaconBlockHeader)
	return blocks
}

// getFilterBlockTree recursively traverses the block tree to identify viable blocks.
// It takes a block hash and a map of viable blocks as input parameters, and returns a boolean value indicating
// whether the current block is viable.
func (f *ForkChoiceStore) getFilterBlockTree(blockRoot libcommon.Hash, blocks map[libcommon.Hash]*cltypes.BeaconBlockHeader) bool {
	header, has := f.forkGraph.GetHeader(blockRoot)
	if !has {
		return false
	}
	children := f.forkGraph.GetChildren(blockRoot)
	// If there are children iterate down recursively and see which branches are viable.
	if len(children) > 0 {
		isAnyViable := false
		for _, child := range children {
			if f.getFilterBlockTree(child, blocks) {
				isAnyViable = true
			}
		}
		if isAnyViable {
			blocks[blockRoot] = header
		}
		return isAnyViable
	}
	currentJustifiedCheckpoint, has := f.forkGraph.GetCurrentJustifiedCheckpoint(blockRoot)
	if !has {
		return false
	}
	finalizedJustifiedCheckpoint, has := f.forkGraph.GetFinalizedCheckpoint(blockRoot)
	if !has {
		return false
	}

	genesisEpoch := f.forkGraph.Config().GenesisEpoch
	if (f.justifiedCheckpoint.Epoch == genesisEpoch || currentJustifiedCheckpoint.Equal(f.justifiedCheckpoint)) &&
		(f.finalizedCheckpoint.Epoch == genesisEpoch || finalizedJustifiedCheckpoint.Equal(f.finalizedCheckpoint)) {
		blocks[blockRoot] = header
		return true
	}
	return false
}
