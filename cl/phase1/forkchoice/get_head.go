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
	"slices"
	"time"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	log "github.com/erigontech/erigon/common/log/v3"
)

type auxiliaryStateView func(synced_data.ViewHeadStateFn) error

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
}

const (
	sampleFactor = 100
	sampleBasis  = 80
)

func (f *ForkChoiceStore) computeVotes(justifiedCheckpoint solid.Checkpoint, justificationState *checkpointState, auxilliaryState *state.CachingBeaconState) map[common.Hash]uint64 {
	votes := make(map[common.Hash]uint64)
	// make an rng generator
	gen := rand.New(rand.NewSource(time.Now().UnixNano()))
	if auxilliaryState != nil {
		count := f.latestMessages.latestMessagesCount()
		if validatorCount := auxilliaryState.ValidatorSet().Length(); validatorCount < count {
			count = validatorCount
		}
		startIdx, step := voteSampleBounds(count, f.probabilisticHeadGetter, gen)
		for validatorIndex := startIdx; validatorIndex < count; validatorIndex += step {
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
			activeBalance := uint64(0)
			if cachedState, ok := f.checkpointStates.Load(justifiedCheckpoint); ok {
				activeBalance = cachedState.(*checkpointState).unslashedActiveBalance
			} else {
				activeBalance = getUnslashedActiveBalance(auxilliaryState)
			}
			boost := activeBalance / f.beaconCfg.SlotsPerEpoch
			votes[boostRoot] += (boost * f.beaconCfg.ProposerScoreBoost) / 100
		}
	} else {
		votes = f.voteTally.votes(f, justifiedCheckpoint, justificationState)
		boostRoot := f.proposerBoostRoot.Load().(common.Hash)
		if boostRoot != (common.Hash{}) {
			boost := justificationState.unslashedActiveBalance / justificationState.beaconConfig.SlotsPerEpoch
			votes[boostRoot] += (boost * justificationState.beaconConfig.ProposerScoreBoost) / 100
		}
	}

	return votes
}

func voteSampleBounds(count int, probabilistic bool, gen *rand.Rand) (int, int) {
	if !probabilistic || count == 0 {
		return 0, 1
	}
	startLimit := min(count, sampleBasis)
	return gen.Intn(startLimit), sampleBasis + gen.Intn(sampleFactor)
}

// GetHead returns the head of the fork choice store.
// Dispatches to GLOAS or pre-GLOAS implementation based on current epoch.
func (f *ForkChoiceStore) GetHead(auxilliaryState *state.CachingBeaconState) (common.Hash, uint64, error) {
	var viewAuxiliaryState auxiliaryStateView
	if auxilliaryState != nil {
		viewAuxiliaryState = func(fn synced_data.ViewHeadStateFn) error {
			return fn(auxilliaryState)
		}
	}
	head, slot, err := f.getHeadNode(viewAuxiliaryState)
	return head.Root, slot, err
}

// GetHeadWithSyncedHeadState resolves the head using the synced head state when needed.
// Callers must not hold a synced-data lock: the fork-choice lock must come first.
func (f *ForkChoiceStore) GetHeadWithSyncedHeadState() (ForkChoiceNode, uint64, error) {
	return f.getHeadNode(f.syncedDataManager.ViewHeadState)
}

func (f *ForkChoiceStore) getHeadNode(viewAuxiliaryState auxiliaryStateView) (ForkChoiceNode, uint64, error) {
	f.mu.RLock()
	if f.headHash != (common.Hash{}) {
		head := ForkChoiceNode{Root: f.headHash, PayloadStatus: f.headPayloadStatus}
		headSlot := f.headSlot
		f.publishSelectedHead(head.Root, headSlot)
		f.mu.RUnlock()
		return head, headSlot, nil
	}
	f.mu.RUnlock()

	// After checkpoint sync the justified root may predate the anchor and
	// not exist in the fork graph. Both getHead and getHeadGloas depend on
	// the justified root being present (for getCheckpointState and
	// getFilteredNodeTree respectively). Return the anchor as head until
	// forward sync produces a resolvable justified checkpoint.
	justifiedCheckpoint := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	if _, hasJustified := f.forkGraph.GetHeader(justifiedCheckpoint.Root); !hasJustified {
		log.Debug("GetHead: justified root not in fork graph, using anchor as head",
			"justifiedRoot", justifiedCheckpoint.Root)
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.headHash != (common.Hash{}) {
			head := ForkChoiceNode{Root: f.headHash, PayloadStatus: f.headPayloadStatus}
			f.publishSelectedHead(head.Root, f.headSlot)
			return head, f.headSlot, nil
		}
		headRoot, headSlot := f.forkGraph.AnchorRoot(), f.forkGraph.AnchorSlot()
		f.headHash = headRoot
		f.headSlot = headSlot
		f.headPayloadStatus = cltypes.PayloadStatusPending
		f.publishSelectedHead(f.headHash, f.headSlot)
		return ForkChoiceNode{Root: f.headHash, PayloadStatus: f.headPayloadStatus}, f.headSlot, nil
	}

	currentEpoch := f.computeEpochAtSlot(f.Slot())
	if f.beaconCfg.GetCurrentStateVersion(currentEpoch) >= clparams.GloasVersion {
		return f.getHeadGloas()
	}
	return f.getHead(viewAuxiliaryState)
}

// GetHeadNode returns the root, payload status, and slot from one head snapshot.
// It recomputes an invalidated cache and publishes the selected head. Reading the status
// separately could pair the root with another head's status or a cache invalidation's PENDING value.
func (f *ForkChoiceStore) GetHeadNode() (ForkChoiceNode, uint64, error) {
	return f.getHeadNode(nil)
}

// getHeadGloas returns the head using GLOAS fork choice rules.
// [New in Gloas:EIP7732]
func (f *ForkChoiceStore) getHeadGloas() (ForkChoiceNode, uint64, error) {
	for {
		justifiedCheckpoint := f.justifiedCheckpoint.Load().(solid.Checkpoint)
		cs, err := f.gloasHeadCheckpointState(justifiedCheckpoint, f.getCheckpointState)
		if err != nil {
			return ForkChoiceNode{}, 0, err
		}

		head, headSlot, ok, err := func() (ForkChoiceNode, uint64, bool, error) {
			f.mu.Lock()
			defer f.mu.Unlock()

			// Another reader may have filled the cache while this call waited.
			if f.headHash != (common.Hash{}) {
				head := ForkChoiceNode{Root: f.headHash, PayloadStatus: f.headPayloadStatus}
				f.publishSelectedHead(head.Root, f.headSlot)
				return head, f.headSlot, true, nil
			}
			if f.justifiedCheckpoint.Load().(solid.Checkpoint) != justifiedCheckpoint {
				return ForkChoiceNode{}, 0, false, nil
			}
			head, slot, err := f.computeHeadGloasWithAnchorFallback(justifiedCheckpoint, cs)
			if err != nil {
				return ForkChoiceNode{}, 0, false, err
			}
			f.headHash = head.Root
			f.headSlot = slot
			f.headPayloadStatus = head.PayloadStatus
			f.publishSelectedHead(f.headHash, f.headSlot)
			return head, f.headSlot, true, nil
		}()
		if err != nil {
			return ForkChoiceNode{}, 0, err
		}
		if ok {
			return head, headSlot, nil
		}
	}
}

func (f *ForkChoiceStore) gloasHeadCheckpointState(justifiedCheckpoint solid.Checkpoint, getCheckpointState func(solid.Checkpoint) (*checkpointState, error)) (*checkpointState, error) {
	cs, err := getCheckpointState(justifiedCheckpoint)
	if err != nil {
		if _, hasJustified := f.forkGraph.GetHeader(justifiedCheckpoint.Root); !hasJustified {
			return nil, nil
		}
	}
	return cs, err
}

func (f *ForkChoiceStore) computeHeadGloasWithAnchorFallback(justifiedCheckpoint solid.Checkpoint, cs *checkpointState) (ForkChoiceNode, uint64, error) {
	if _, hasJustified := f.forkGraph.GetHeader(justifiedCheckpoint.Root); !hasJustified {
		log.Debug("GetHead: justified root not in fork graph, using anchor as head",
			"justifiedRoot", justifiedCheckpoint.Root)
		return ForkChoiceNode{
			Root:          f.forkGraph.AnchorRoot(),
			PayloadStatus: cltypes.PayloadStatusPending,
		}, f.forkGraph.AnchorSlot(), nil
	}
	return f.computeHeadGloas(justifiedCheckpoint, cs)
}

func (f *ForkChoiceStore) computeHeadGloas(justifiedCheckpoint solid.Checkpoint, cs *checkpointState) (ForkChoiceNode, uint64, error) {
	// OnTick updates time before taking f.mu. Keep viability checks, weights, and
	// payload tiebreakers on the same slot even when the clock advances during this walk.
	currentSlot := f.Slot()
	nodes, nodeChildren := f.getFilteredNodeTree(justifiedCheckpoint.Root, justifiedCheckpoint, currentSlot)
	f.headIsFallback = len(nodes) == 0
	ws := f.gloasWeightTree.prepare(justifiedCheckpoint, cs)
	if len(nodes) == 0 {
		header, hasHeader := f.forkGraph.GetHeader(justifiedCheckpoint.Root)
		if !hasHeader {
			return ForkChoiceNode{}, 0, errors.New("no slot for head is stored")
		}
		return ForkChoiceNode{
			Root:          justifiedCheckpoint.Root,
			PayloadStatus: cltypes.PayloadStatusEmpty,
		}, header.Slot, nil
	}

	// Start from justified checkpoint with PENDING status
	head := ForkChoiceNode{
		Root:          justifiedCheckpoint.Root,
		PayloadStatus: cltypes.PayloadStatusPending,
	}

	for {
		children := make([]ForkChoiceNode, 0, 2)
		for _, child := range nodeChildren[head] {
			if _, ok := nodes[child]; ok {
				children = append(children, child)
			}
		}
		if len(children) == 0 {
			// No children, head is the result
			header, hasHeader := f.forkGraph.GetHeader(head.Root)
			if !hasHeader {
				return ForkChoiceNode{}, 0, errors.New("no slot for head is stored")
			}
			return head, header.Slot, nil
		}

		// Find best child: max(children, key=(weight, root, tiebreaker))
		bestChild := children[0]
		bestWeight := ws.GetWeight(bestChild, currentSlot)
		bestTiebreaker := f.getPayloadStatusTiebreaker(bestChild, currentSlot)

		for i := 1; i < len(children); i++ {
			child := children[i]
			weight := ws.GetWeight(child, currentSlot)
			tiebreaker := f.getPayloadStatusTiebreaker(child, currentSlot)

			// Compare: weight first, then root, then tiebreaker
			if weight > bestWeight {
				bestChild = child
				bestWeight = weight
				bestTiebreaker = tiebreaker
			} else if weight == bestWeight {
				// Compare by root (lexicographically greater wins)
				rootCmp := bytes.Compare(child.Root[:], bestChild.Root[:])
				if rootCmp > 0 {
					bestChild = child
					bestWeight = weight
					bestTiebreaker = tiebreaker
				} else if rootCmp == 0 {
					// Same root, compare by tiebreaker
					if tiebreaker > bestTiebreaker {
						bestChild = child
						bestWeight = weight
						bestTiebreaker = tiebreaker
					}
				}
			}
		}

		head = bestChild
	}
}

func (f *ForkChoiceStore) getFilteredNodeTree(base common.Hash, justifiedCheckpoint solid.Checkpoint, currentSlot uint64) (map[ForkChoiceNode]struct{}, map[ForkChoiceNode][]ForkChoiceNode) {
	nodes := make(map[ForkChoiceNode]struct{})
	nodeChildren := make(map[ForkChoiceNode][]ForkChoiceNode)
	currentEpoch := f.computeEpochAtSlot(currentSlot)
	baseHeader, hasBase := f.forkGraph.GetHeader(base)
	if !hasBase {
		return nodes, nodeChildren
	}
	finalizedCheckpoint := f.finalizedCheckpoint.Load().(solid.Checkpoint)
	genesisEpoch := f.beaconCfg.GenesisEpoch
	finalizedSlot := f.computeStartSlotAtEpoch(finalizedCheckpoint.Epoch)
	checkpointRoot := base
	if finalizedCheckpoint.Epoch != genesisEpoch && baseHeader.Slot > finalizedSlot {
		checkpointRoot = f.Ancestor(base, finalizedSlot).Root
	}
	type frame struct {
		node           ForkChoiceNode
		children       []ForkChoiceNode
		checkpointRoot common.Hash
	}
	stack := []frame{{
		node:           ForkChoiceNode{Root: base, PayloadStatus: cltypes.PayloadStatusPending},
		checkpointRoot: checkpointRoot,
	}}
	for len(stack) > 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		if current.children != nil {
			for _, child := range current.children {
				if _, viable := nodes[child]; viable {
					nodes[current.node] = struct{}{}
					break
				}
			}
			continue
		}
		header, hasHeader := f.forkGraph.GetHeader(current.node.Root)
		if !hasHeader {
			continue
		}
		if finalizedCheckpoint.Epoch != genesisEpoch && header.Slot <= finalizedSlot {
			current.checkpointRoot = current.node.Root
		}
		children := f.getNodeChildren(current.node)
		nodeChildren[current.node] = children
		if len(children) == 0 {
			finalizedOK := finalizedCheckpoint.Epoch == genesisEpoch || finalizedCheckpoint.Root == current.checkpointRoot
			if finalizedOK && f.hasCorrectJustification(current.node.Root, header, currentEpoch, justifiedCheckpoint) {
				nodes[current.node] = struct{}{}
			}
			continue
		}
		stack = append(stack, frame{node: current.node, children: children, checkpointRoot: current.checkpointRoot})
		for _, child := range slices.Backward(children) {
			stack = append(stack, frame{node: child, checkpointRoot: current.checkpointRoot})
		}
	}
	return nodes, nodeChildren
}

// getHead returns the head using pre-GLOAS fork choice rules.
func (f *ForkChoiceStore) getHead(viewAuxiliaryState auxiliaryStateView) (ForkChoiceNode, uint64, error) {
	for {
		justifiedCheckpoint := f.justifiedCheckpoint.Load().(solid.Checkpoint)
		head, headSlot, ok, err := f.getHeadOnce(viewAuxiliaryState, justifiedCheckpoint)
		if err != nil {
			return ForkChoiceNode{}, 0, err
		}
		if ok {
			return head, headSlot, nil
		}
	}
}

// getHeadOnce computes the head for one snapshot of the justified checkpoint. It reports
// ok=false when the checkpoint moved while it waited for f.mu, so the caller retries:
// caching a head computed from a superseded checkpoint would leave it stale until the next
// attestation, tick or block clears it.
func (f *ForkChoiceStore) getHeadOnce(viewAuxiliaryState auxiliaryStateView, justifiedCheckpoint solid.Checkpoint) (ForkChoiceNode, uint64, bool, error) {
	var justificationState *checkpointState
	var err error
	if viewAuxiliaryState == nil {
		// Compute checkpoint state BEFORE acquiring f.mu: this operation
		// can read a large state from disk, which would block the OnTick
		// goroutine if done under the lock.
		justificationState, err = f.getCheckpointState(justifiedCheckpoint)
		if err != nil {
			return ForkChoiceNode{}, 0, false, err
		}
	} else if _, cached := f.checkpointStates.Load(justifiedCheckpoint); !cached {
		_, _ = f.getCheckpointState(justifiedCheckpoint)
	}
	f.mu.Lock()
	currentSlot := f.Slot()
	// Checkpoint I/O can cross the fork boundary. Do not replace a Gloas head
	// with a result from the pre-fork algorithm.
	if f.beaconCfg.GetCurrentStateVersion(f.computeEpochAtSlot(currentSlot)) >= clparams.GloasVersion {
		f.mu.Unlock()
		// getHeadGloas runs its own checkpoint retry, so its result is final.
		head, headSlot, err := f.getHeadGloas()
		return head, headSlot, true, err
	}
	defer f.mu.Unlock()
	if f.justifiedCheckpoint.Load().(solid.Checkpoint) != justifiedCheckpoint {
		return ForkChoiceNode{}, 0, false, nil
	}

	var votes map[common.Hash]uint64
	if viewAuxiliaryState == nil {
		votes = f.computeVotes(justifiedCheckpoint, justificationState, nil)
	} else if err := viewAuxiliaryState(func(auxiliaryState *state.CachingBeaconState) error {
		votes = f.computeVotes(justifiedCheckpoint, justificationState, auxiliaryState)
		return nil
	}); err != nil {
		return ForkChoiceNode{}, 0, false, err
	}

	// Retrieve att
	f.headHash = justifiedCheckpoint.Root
	f.headPayloadStatus = cltypes.PayloadStatusPending
	blocks := f.getFilteredBlockTree(f.headHash, justifiedCheckpoint, currentSlot)
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
				return ForkChoiceNode{}, 0, false, errors.New("no slot for head is stored")
			}
			f.headSlot = header.Slot
			f.publishSelectedHead(f.headHash, f.headSlot)
			return ForkChoiceNode{Root: f.headHash, PayloadStatus: f.headPayloadStatus}, f.headSlot, true, nil
		}

		// Average case scenario.
		if len(children) == 1 {
			f.headHash = children[0]
			continue
		}
		// Sort children by lexigographical order
		slices.SortFunc(children, func(a, b common.Hash) int {
			return bytes.Compare(a[:], b[:])
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

func (f *ForkChoiceStore) publishSelectedHead(root common.Hash, slot uint64) {
	status := f.headPayloadStatus
	if f.headIsFallback {
		// get_head did not choose this variant over the other one, so neither rules the other out.
		status = cltypes.PayloadStatusPending
	}
	f.selectedHead.Store(&ForkChoiceNode{Root: root, PayloadStatus: status})
	if f.syncedDataManager != nil {
		f.syncedDataManager.OnSelectedHead(root, slot)
	}
}

// getFilteredBlockTree keeps branches that pass the justification and finalization checks.
func (f *ForkChoiceStore) getFilteredBlockTree(base common.Hash, justifiedCheckpoint solid.Checkpoint, currentSlot uint64) map[common.Hash]*cltypes.BeaconBlockHeader {
	blocks := make(map[common.Hash]*cltypes.BeaconBlockHeader)
	currentEpoch := f.computeEpochAtSlot(currentSlot)
	f.getFilterBlockTree(base, blocks, currentEpoch, justifiedCheckpoint)
	return blocks
}

// getFilterBlockTree recursively traverses the block tree to identify viable blocks.
// It takes a block hash and a map of viable blocks as input parameters, and returns a boolean value indicating
// whether the current block is viable.
func (f *ForkChoiceStore) getFilterBlockTree(blockRoot common.Hash, blocks map[common.Hash]*cltypes.BeaconBlockHeader, currentEpoch uint64, justifiedCheckpoint solid.Checkpoint) bool {
	header, has := f.forkGraph.GetHeader(blockRoot)
	if !has {
		return false
	}
	children := f.children(blockRoot)
	// If there are children iterate down recursively and see which branches are viable.
	if len(children) > 0 {
		isAnyViable := false
		for _, child := range children {
			if f.getFilterBlockTree(child, blocks, currentEpoch, justifiedCheckpoint) {
				isAnyViable = true
			}
		}
		if isAnyViable {
			blocks[blockRoot] = header
		}
		return isAnyViable
	}
	if f.isViableLeaf(blockRoot, header, currentEpoch, justifiedCheckpoint) {
		blocks[blockRoot] = header
		return true
	}
	return false
}

func (f *ForkChoiceStore) isViableLeaf(blockRoot common.Hash, header *cltypes.BeaconBlockHeader, currentEpoch uint64, justifiedCheckpoint solid.Checkpoint) bool {
	finalizedCheckpoint := f.finalizedCheckpoint.Load().(solid.Checkpoint)
	justifiedOk := f.hasCorrectJustification(blockRoot, header, currentEpoch, justifiedCheckpoint)

	genesisEpoch := f.beaconCfg.GenesisEpoch

	// Spec correct_finalized:
	//   store.finalized_checkpoint.epoch == GENESIS_EPOCH
	//   || store.finalized_checkpoint.root == get_checkpoint_block(block_root, finalized.epoch)
	finalizedOk := finalizedCheckpoint.Epoch == genesisEpoch
	if !finalizedOk {
		finalizedSlot := f.computeStartSlotAtEpoch(finalizedCheckpoint.Epoch)
		finalizedOk = finalizedCheckpoint.Root == f.Ancestor(blockRoot, finalizedSlot).Root
	}

	return justifiedOk && finalizedOk
}

func (f *ForkChoiceStore) hasCorrectJustification(blockRoot common.Hash, header *cltypes.BeaconBlockHeader, currentEpoch uint64, justifiedCheckpoint solid.Checkpoint) bool {
	blockEpoch := f.computeEpochAtSlot(header.Slot)
	var votingSource solid.Checkpoint
	var has bool
	if currentEpoch > blockEpoch {
		votingSource, has = f.getUnrealizedJustification(blockRoot)
	} else {
		votingSource, has = f.forkGraph.GetCurrentJustifiedCheckpoint(blockRoot)
	}
	if !has {
		return false
	}

	genesisEpoch := f.beaconCfg.GenesisEpoch

	// Spec correct_justified:
	//   store.justified_checkpoint.epoch == GENESIS_EPOCH
	//   || voting_source.epoch == store.justified_checkpoint.epoch
	//   || voting_source.epoch + 2 >= current_epoch
	justifiedOk := justifiedCheckpoint.Epoch == genesisEpoch ||
		votingSource.Epoch == justifiedCheckpoint.Epoch ||
		votingSource.Epoch+2 >= currentEpoch
	return justifiedOk
}
