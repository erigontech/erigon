// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package forkchoice

import (
	"bytes"
	"errors"
	"math/rand"
	"sort"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

// accountWeights updates the weights of the validators, given the vote and given an head leaf.
func (f *ForkChoiceStore) accountWeights(votes, weights map[common.Hash]uint64, justifedRoot, leaf common.Hash) {
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

const (
	sampleFactor = 100
	sampleBasis  = 80
)

func (f *ForkChoiceStore) computeVotes(justifiedCheckpoint solid.Checkpoint, checkpointState *checkpointState, auxilliaryState *state.CachingBeaconState) map[common.Hash]uint64 {
	votes := make(map[common.Hash]uint64)
	// make an rng generator
	gen := rand.New(rand.NewSource(time.Now().UnixNano()))
	if auxilliaryState != nil {
		startIdx := 0
		step := 1
		if f.probabilisticHeadGetter {
			startIdx = gen.Intn(sampleBasis)
			step = sampleBasis + gen.Intn(sampleFactor)
		}
		for validatorIndex := startIdx; validatorIndex < f.latestMessages.latestMessagesCount(); validatorIndex += step {
			message, _ := f.latestMessages.get(validatorIndex)
			v := auxilliaryState.ValidatorSet().Get(validatorIndex)
			if !v.Active(justifiedCheckpoint.Epoch) || v.Slashed() {
				continue
			}
			if _, hasLatestMessage := f.getLatestMessage(uint64(validatorIndex)); !hasLatestMessage || f.isUnequivocating(uint64(validatorIndex)) {
				continue
			}
			votes[message.Root] += v.EffectiveBalance()
		}
		boostRoot := f.proposerBoostRoot.Load().(common.Hash)
		if boostRoot != (common.Hash{}) {
			boost := auxilliaryState.GetTotalActiveBalance() / auxilliaryState.BeaconConfig().SlotsPerEpoch
			votes[boostRoot] += (boost * auxilliaryState.BeaconConfig().ProposerScoreBoost) / 100
		}
	} else {
		for validatorIndex := 0; validatorIndex < f.latestMessages.latestMessagesCount(); validatorIndex++ {
			message, _ := f.latestMessages.get(validatorIndex)
			if message == (LatestMessage{}) {
				continue
			}
			if !readFromBitset(checkpointState.actives, validatorIndex) || readFromBitset(checkpointState.slasheds, validatorIndex) {
				continue
			}
			if _, hasLatestMessage := f.getLatestMessage(uint64(validatorIndex)); !hasLatestMessage {
				continue
			}
			if f.isUnequivocating(uint64(validatorIndex)) {
				continue
			}
			votes[message.Root] += checkpointState.balances[validatorIndex]
		}
		boostRoot := f.proposerBoostRoot.Load().(common.Hash)
		if boostRoot != (common.Hash{}) {
			boost := checkpointState.activeBalance / checkpointState.beaconConfig.SlotsPerEpoch
			votes[boostRoot] += (boost * checkpointState.beaconConfig.ProposerScoreBoost) / 100
		}
	}

	return votes
}

// GetHead returns the head of the fork choice store.
// it can take an optional auxilliary state to determine the current weights instead of computing the justified state.
func (f *ForkChoiceStore) GetHead(auxilliaryState *state.CachingBeaconState) (common.Hash, uint64, error) {
	f.mu.RLock()
	if f.headHash != (common.Hash{}) {
		f.mu.RUnlock()
		return f.headHash, f.headSlot, nil
	}
	f.mu.RUnlock()
	justifiedCheckpoint := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	var justificationState *checkpointState
	var err error
	// Take write lock here
	f.mu.Lock()
	defer f.mu.Unlock()
	if auxilliaryState == nil {
		// See which validators can be used for attestation score
		justificationState, err = f.getCheckpointState(justifiedCheckpoint)
		if err != nil {
			return common.Hash{}, 0, err
		}
	}

	// Retrieve att
	f.headHash = justifiedCheckpoint.Root
	blocks := f.getFilteredBlockTree(f.headHash)
	// Do a simple scan to determine the fork votes.
	votes := f.computeVotes(justifiedCheckpoint, justificationState, auxilliaryState)
	// Account for weights on each head fork
	f.weights = make(map[common.Hash]uint64)
	for head := range f.headSet {
		f.accountWeights(votes, f.weights, justifiedCheckpoint.Root, head)
	}

	for {
		// Filter out current head children.
		unfilteredChildren := f.children(f.headHash)
		children := []common.Hash{}
		for _, child := range unfilteredChildren {
			if _, ok := blocks[child]; ok {
				children = append(children, child)
			}
		}
		// Stop if we dont have any more children
		if len(children) == 0 {
			header, hasHeader := f.forkGraph.GetHeader(f.headHash)
			if !hasHeader {
				return common.Hash{}, 0, errors.New("no slot for head is stored")
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

// getFilteredBlockTree filters out dumb blocks.
func (f *ForkChoiceStore) getFilteredBlockTree(base common.Hash) map[common.Hash]*cltypes.BeaconBlockHeader {
	blocks := make(map[common.Hash]*cltypes.BeaconBlockHeader)
	f.getFilterBlockTree(base, blocks)
	return blocks
}

// getFilterBlockTree recursively traverses the block tree to identify viable blocks.
// It takes a block hash and a map of viable blocks as input parameters, and returns a boolean value indicating
// whether the current block is viable.
func (f *ForkChoiceStore) getFilterBlockTree(blockRoot common.Hash, blocks map[common.Hash]*cltypes.BeaconBlockHeader) bool {
	header, has := f.forkGraph.GetHeader(blockRoot)
	if !has {
		return false
	}
	finalizedCheckpoint := f.finalizedCheckpoint.Load().(solid.Checkpoint)
	justifiedCheckpoint := f.justifiedCheckpoint.Load().(solid.Checkpoint)
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
	if (justifiedCheckpoint.Epoch == genesisEpoch || currentJustifiedCheckpoint.Equal(justifiedCheckpoint)) &&
		(finalizedCheckpoint.Epoch == genesisEpoch || finalizedJustifiedCheckpoint.Equal(finalizedCheckpoint)) {
		blocks[blockRoot] = header
		return true
	}
	return false
}
