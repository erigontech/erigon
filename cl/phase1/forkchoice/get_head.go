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

// accountWeights updates the weights of the validators, given the vote and given an head leaf.
func (f *ForkChoiceStore) accountWeights(votes, weights map[libcommon.Hash]uint64, justifedRoot, leaf libcommon.Hash) {
	curr := leaf
	accumulated := uint64(0)
	for curr != justifedRoot {
		accumulated += votes[curr]
		votes[curr] = 0 // make sure we don't double count
		weights[curr] += accumulated
		header, has := f.forkGraph.GetHeader(curr)
		if !has {
			return
		}
		curr = header.ParentRoot
	}
	return
}

func (f *ForkChoiceStore) getHead() (libcommon.Hash, uint64, error) {
	if f.headHash != (libcommon.Hash{}) {
		return f.headHash, f.headSlot, nil
	}
	// Retrieve att
	f.headHash = f.justifiedCheckpoint.BlockRoot()
	blocks := f.getFilteredBlockTree(f.headHash)
	// See which validators can be used for attestation score
	justificationState, err := f.getCheckpointState(f.justifiedCheckpoint)
	if err != nil {
		return libcommon.Hash{}, 0, err
	}
	// Do a simple scan to determine the fork votes.
	votes := make(map[libcommon.Hash]uint64)
	for validatorIndex, message := range f.latestMessages {
		if message == (LatestMessage{}) {
			continue
		}
		if !readFromBitset(justificationState.actives, validatorIndex) || readFromBitset(justificationState.slasheds, validatorIndex) {
			continue
		}
		if _, hasLatestMessage := f.getLatestMessage(uint64(validatorIndex)); !hasLatestMessage {
			continue
		}
		if f.isUnequivocating(uint64(validatorIndex)) {
			continue
		}
		votes[message.Root] += justificationState.balances[validatorIndex]
	}
	if f.proposerBoostRoot != (libcommon.Hash{}) {
		boost := justificationState.activeBalance / justificationState.beaconConfig.SlotsPerEpoch
		votes[f.proposerBoostRoot] += (boost * justificationState.beaconConfig.ProposerScoreBoost) / 100
	}
	// Account for weights on each head fork
	f.weights = make(map[libcommon.Hash]uint64)
	for head := range f.headSet {
		f.accountWeights(votes, f.weights, f.justifiedCheckpoint.BlockRoot(), head)
	}

	for {
		// Filter out current head children.
		unfilteredChildren := f.children(f.headHash)
		children := []libcommon.Hash{}
		for _, child := range unfilteredChildren {
			if _, ok := blocks[child]; ok {
				children = append(children, child)
			}
		}
		// Stop if we dont have any more children
		if len(children) == 0 {
			header, hasHeader := f.forkGraph.GetHeader(f.headHash)
			if !hasHeader {
				return libcommon.Hash{}, 0, fmt.Errorf("no slot for head is stored")
			}
			f.headSlot = header.Slot
			return f.headHash, f.headSlot, nil
		}
		// Average case scenario.
		if len(children) == 1 {
			f.headHash = children[0]
			continue
		}
		// Sort children by lexigographical order
		sort.Slice(children, func(i, j int) bool {
			childA := children[i]
			childB := children[j]
			return bytes.Compare(childA[:], childB[:]) < 0
		})

		// After sorting is done determine best fit.
		f.headHash = children[0]
		maxWeight := f.weights[children[0]]
		for i := 1; i < len(children); i++ {
			weight := f.weights[children[i]]
			// Lexicographical order is king.
			if weight >= maxWeight {
				f.headHash = children[i]
				maxWeight = weight
			}
		}
	}
}

// filterValidatorSetForAttestationScores preliminarly filter the validator set obliging to consensus rules.
func (f *ForkChoiceStore) filterValidatorSetForAttestationScores(c *checkpointState, epoch uint64) []uint64 {
	filtered := make([]uint64, 0, c.validatorSetSize)
	for validatorIndex := 0; validatorIndex < c.validatorSetSize; validatorIndex++ {
		if !readFromBitset(c.actives, validatorIndex) || readFromBitset(c.slasheds, validatorIndex) {
			continue
		}
		if _, hasLatestMessage := f.getLatestMessage(uint64(validatorIndex)); !hasLatestMessage {
			continue
		}
		if f.isUnequivocating(uint64(validatorIndex)) {
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
	// Compute attestation score
	var attestationScore uint64
	for _, validatorIndex := range indicies {
		if f.Ancestor(f.latestMessages[validatorIndex].Root, header.Slot) != root {
			continue
		}
		attestationScore += state.balances[validatorIndex]
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
	children := f.children(blockRoot)
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

	genesisEpoch := f.beaconCfg.GenesisEpoch
	if (f.justifiedCheckpoint.Epoch() == genesisEpoch || currentJustifiedCheckpoint.Equal(f.justifiedCheckpoint)) &&
		(f.finalizedCheckpoint.Epoch() == genesisEpoch || finalizedJustifiedCheckpoint.Equal(f.finalizedCheckpoint)) {
		blocks[blockRoot] = header
		return true
	}
	return false
}
