package forkchoice

import (
	"bytes"
	"fmt"
	"sort"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

// GetHead fetches the current head.
func (f *ForkChoiceStore) GetHead() (libcommon.Hash, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getHead()
}

func (f *ForkChoiceStore) getHead() (libcommon.Hash, uint64, error) {
	// Retrieve att
	head := f.justifiedCheckpoint.Root
	blocks := f.getFilteredBlockTree(head)
	// See which validators can be used for attestation score
	justificationState, err := f.getCheckpointState(*f.justifiedCheckpoint)
	if err != nil {
		return libcommon.Hash{}, 0, err
	}
	// Filter all validators deemed as bad
	filteredIndicies := f.filterValidatorSetForAttestationScores(justificationState.validators, justificationState.epoch)
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
			header, hasHeader := f.forkGraph.GetHeader(head)
			if !hasHeader {
				return libcommon.Hash{}, 0, fmt.Errorf("no slot for head is stored")
			}
			return head, header.Slot, nil
		}
		// Average case scenario.
		if len(children) == 1 {
			head = children[0]
			continue
		}
		// Sort children by lexigographical order
		sort.Slice(children, func(i, j int) bool {
			childA := children[i]
			childB := children[j]
			return bytes.Compare(childA[:], childB[:]) < 0
		})

		// After sorting is done determine best fit.
		head = children[0]
		maxWeight := f.getWeight(children[0], filteredIndicies, justificationState)
		for i := 1; i < len(children); i++ {
			weight := f.getWeight(children[i], filteredIndicies, justificationState)
			// Lexicographical order is king.
			if weight >= maxWeight {
				head = children[i]
				maxWeight = weight
			}
		}
	}
}

// filterValidatorSetForAttestationScores preliminarly filter the validator set obliging to consensus rules.
func (f *ForkChoiceStore) filterValidatorSetForAttestationScores(validatorSet []*checkpointValidator, epoch uint64) []uint64 {
	filtered := make([]uint64, 0, len(validatorSet))
	for validatorIndex, validator := range validatorSet {
		if !validator.active(epoch) || validator.slashed {
			continue
		}
		if _, hasLatestMessage := f.latestMessages[uint64(validatorIndex)]; !hasLatestMessage {
			continue
		}
		if _, isUnequivocating := f.equivocatingIndicies[uint64(validatorIndex)]; isUnequivocating {
			continue
		}
		filtered = append(filtered, uint64(validatorIndex))
	}
	return filtered
}

// getWeight computes weight in head decision of canonical chain.
func (f *ForkChoiceStore) getWeight(root libcommon.Hash, indicies []uint64, state *checkpointState) uint64 {
	header, has := f.forkGraph.GetHeader(root)
	if !has {
		return 0
	}
	validators := state.validators
	// Compute attestation score
	var attestationScore uint64
	for _, validatorIndex := range indicies {
		if f.Ancestor(f.latestMessages[validatorIndex].Root, header.Slot) != root {
			continue
		}
		attestationScore += validators[validatorIndex].balance
	}
	if f.proposerBoostRoot == (libcommon.Hash{}) {
		return attestationScore
	}

	// Boost is applied if root is an ancestor of proposer_boost_root
	if f.Ancestor(f.proposerBoostRoot, header.Slot) == root {
		committeeWeight := state.activeBalance / state.beaconConfig.SlotsPerEpoch
		attestationScore += (committeeWeight * state.beaconConfig.ProposerScoreBoost) / 100
	}
	return attestationScore
}

// getFilteredBlockTree filters out dumb blocks.
func (f *ForkChoiceStore) getFilteredBlockTree(base libcommon.Hash) map[libcommon.Hash]*cltypes.BeaconBlockHeader {
	blocks := make(map[libcommon.Hash]*cltypes.BeaconBlockHeader)
	f.getFilterBlockTree(base, blocks)
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
