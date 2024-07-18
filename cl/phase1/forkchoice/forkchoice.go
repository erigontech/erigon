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
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/optimistic"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/utils/eth_clock"

	lru "github.com/hashicorp/golang-lru/v2"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
)

// ForkNode is a struct that represents a node in the fork choice tree.
type ForkNode struct {
	Slot           uint64         `json:"slot,string"`
	BlockRoot      libcommon.Hash `json:"block_root"`
	ParentRoot     libcommon.Hash `json:"parent_root"`
	JustifiedEpoch uint64         `json:"justified_epoch,string"`
	FinalizedEpoch uint64         `json:"finalized_epoch,string"`
	Weight         uint64         `json:"weight,string"`
	Validity       string         `json:"validity"`
	ExecutionBlock libcommon.Hash `json:"execution_block_hash"`
}

type checkpointComparable string

const (
	checkpointsPerCache = 1024
	allowedCachedStates = 8
)

type randaoDelta struct {
	epoch uint64
	delta libcommon.Hash
}

type finalityCheckpoints struct {
	finalizedCheckpoint         solid.Checkpoint
	currentJustifiedCheckpoint  solid.Checkpoint
	previousJustifiedCheckpoint solid.Checkpoint
}

type preverifiedAppendListsSizes struct {
	validatorLength           uint64
	historicalRootsLength     uint64
	historicalSummariesLength uint64
}

type ForkChoiceStore struct {
	time        atomic.Uint64
	highestSeen atomic.Uint64
	// all of *solid.Checkpoint type
	justifiedCheckpoint           atomic.Value
	finalizedCheckpoint           atomic.Value
	unrealizedJustifiedCheckpoint atomic.Value
	unrealizedFinalizedCheckpoint atomic.Value

	proposerBoostRoot     atomic.Value
	headHash              libcommon.Hash
	headSlot              uint64
	genesisTime           uint64
	genesisValidatorsRoot libcommon.Hash
	weights               map[libcommon.Hash]uint64
	headSet               map[libcommon.Hash]struct{}
	hotSidecars           map[libcommon.Hash][]*cltypes.BlobSidecar // Set of sidecars that are not yet processed.
	// childrens
	childrens sync.Map

	// Use go map because this is actually an unordered set
	equivocatingIndicies []byte
	forkGraph            fork_graph.ForkGraph
	blobStorage          blob_storage.BlobStorage
	// I use the cache due to the convenient auto-cleanup feauture.
	checkpointStates sync.Map // We keep ssz snappy of it as the full beacon state is full of rendundant data.

	latestMessages    []LatestMessage
	anchorPublicKeys  []byte
	syncedDataManager *synced_data.SyncedDataManager
	// We keep track of them so that we can forkchoice with EL.
	eth2Roots *lru.Cache[libcommon.Hash, libcommon.Hash] // ETH2 root -> ETH1 hash
	// preverifid sizes and other data collection
	preverifiedSizes    *lru.Cache[libcommon.Hash, preverifiedAppendListsSizes]
	finalityCheckpoints *lru.Cache[libcommon.Hash, finalityCheckpoints]
	totalActiveBalances *lru.Cache[libcommon.Hash, uint64]
	nextBlockProposers  *lru.Cache[libcommon.Hash, []uint64]
	// Randao mixes
	randaoMixesLists *lru.Cache[libcommon.Hash, solid.HashListSSZ] // limited randao mixes full list (only 16 elements)
	randaoDeltas     *lru.Cache[libcommon.Hash, randaoDelta]       // small entry can be lots of elements.
	// participation tracking
	participation *lru.Cache[uint64, *solid.BitList] // epoch -> [partecipation]

	mu sync.RWMutex

	// EL
	engine execution_client.ExecutionEngine

	// operations pool
	operationsPool pool.OperationsPool
	beaconCfg      *clparams.BeaconChainConfig

	emitters *beaconevents.Emitters
	synced   atomic.Bool

	ethClock        eth_clock.EthereumClock
	optimisticStore optimistic.OptimisticStore
}

type LatestMessage struct {
	Epoch uint64
	Root  libcommon.Hash
}

type childrens struct {
	childrenHashes []libcommon.Hash
	parentSlot     uint64 // we keep this one for pruning
}

// NewForkChoiceStore initialize a new store from the given anchor state, either genesis or checkpoint sync state.
func NewForkChoiceStore(
	ethClock eth_clock.EthereumClock,
	anchorState *state2.CachingBeaconState,
	engine execution_client.ExecutionEngine,
	operationsPool pool.OperationsPool,
	forkGraph fork_graph.ForkGraph,
	emitters *beaconevents.Emitters,
	syncedDataManager *synced_data.SyncedDataManager,
	blobStorage blob_storage.BlobStorage,
) (*ForkChoiceStore, error) {
	anchorRoot, err := anchorState.BlockRoot()
	if err != nil {
		return nil, err
	}
	anchorCheckpoint := solid.NewCheckpointFromParameters(
		anchorRoot,
		state2.Epoch(anchorState.BeaconState),
	)

	eth2Roots, err := lru.New[libcommon.Hash, libcommon.Hash](checkpointsPerCache)
	if err != nil {
		return nil, err
	}

	randaoMixesLists, err := lru.New[libcommon.Hash, solid.HashListSSZ](allowedCachedStates)
	if err != nil {
		return nil, err
	}

	randaoDeltas, err := lru.New[libcommon.Hash, randaoDelta](checkpointsPerCache)
	if err != nil {
		return nil, err
	}

	finalityCheckpoints, err := lru.New[libcommon.Hash, finalityCheckpoints](checkpointsPerCache)
	if err != nil {
		return nil, err
	}

	anchorPublicKeys := make([]byte, anchorState.ValidatorLength()*length.Bytes48)
	for idx := 0; idx < anchorState.ValidatorLength(); idx++ {
		pk, err := anchorState.ValidatorPublicKey(idx)
		if err != nil {
			return nil, err
		}
		copy(anchorPublicKeys[idx*length.Bytes48:], pk[:])
	}

	preverifiedSizes, err := lru.New[libcommon.Hash, preverifiedAppendListsSizes](checkpointsPerCache * 10)
	if err != nil {
		return nil, err
	}
	preverifiedSizes.Add(anchorRoot, preverifiedAppendListsSizes{
		validatorLength:           uint64(anchorState.ValidatorLength()),
		historicalRootsLength:     anchorState.HistoricalRootsLength(),
		historicalSummariesLength: anchorState.HistoricalSummariesLength(),
	})

	totalActiveBalances, err := lru.New[libcommon.Hash, uint64](checkpointsPerCache * 10)
	if err != nil {
		return nil, err
	}

	participation, err := lru.New[uint64, *solid.BitList](16)
	if err != nil {
		return nil, err
	}

	nextBlockProposers, err := lru.New[libcommon.Hash, []uint64](checkpointsPerCache * 10)
	if err != nil {
		return nil, err
	}

	participation.Add(state.Epoch(anchorState.BeaconState), anchorState.CurrentEpochParticipation().Copy())

	totalActiveBalances.Add(anchorRoot, anchorState.GetTotalActiveBalance())
	r := solid.NewHashVector(int(anchorState.BeaconConfig().EpochsPerHistoricalVector))
	anchorState.RandaoMixes().CopyTo(r)
	randaoMixesLists.Add(anchorRoot, r)
	headSet := make(map[libcommon.Hash]struct{})
	headSet[anchorRoot] = struct{}{}
	f := &ForkChoiceStore{
		forkGraph:             forkGraph,
		equivocatingIndicies:  make([]byte, anchorState.ValidatorLength(), anchorState.ValidatorLength()*2),
		latestMessages:        make([]LatestMessage, anchorState.ValidatorLength(), anchorState.ValidatorLength()*2),
		eth2Roots:             eth2Roots,
		engine:                engine,
		operationsPool:        operationsPool,
		anchorPublicKeys:      anchorPublicKeys,
		beaconCfg:             anchorState.BeaconConfig(),
		preverifiedSizes:      preverifiedSizes,
		finalityCheckpoints:   finalityCheckpoints,
		totalActiveBalances:   totalActiveBalances,
		randaoMixesLists:      randaoMixesLists,
		randaoDeltas:          randaoDeltas,
		headSet:               headSet,
		weights:               make(map[libcommon.Hash]uint64),
		participation:         participation,
		emitters:              emitters,
		genesisTime:           anchorState.GenesisTime(),
		syncedDataManager:     syncedDataManager,
		nextBlockProposers:    nextBlockProposers,
		genesisValidatorsRoot: anchorState.GenesisValidatorsRoot(),
		hotSidecars:           make(map[libcommon.Hash][]*cltypes.BlobSidecar),
		blobStorage:           blobStorage,
		ethClock:              ethClock,
		optimisticStore:       optimistic.NewOptimisticStore(),
	}
	f.justifiedCheckpoint.Store(anchorCheckpoint.Copy())
	f.finalizedCheckpoint.Store(anchorCheckpoint.Copy())
	f.unrealizedFinalizedCheckpoint.Store(anchorCheckpoint.Copy())
	f.unrealizedJustifiedCheckpoint.Store(anchorCheckpoint.Copy())
	f.proposerBoostRoot.Store(libcommon.Hash{})

	f.highestSeen.Store(anchorState.Slot())
	f.time.Store(anchorState.GenesisTime() + anchorState.BeaconConfig().SecondsPerSlot*anchorState.Slot())
	return f, nil
}

// Highest seen returns highest seen slot
func (f *ForkChoiceStore) HighestSeen() uint64 {
	return f.highestSeen.Load()
}

func (f *ForkChoiceStore) children(parent libcommon.Hash) []libcommon.Hash {
	children, ok := f.childrens.Load(parent)
	if !ok {
		return nil
	}
	return children.(childrens).childrenHashes
}

// updateChildren adds a new child to the parent node hash.
func (f *ForkChoiceStore) updateChildren(parentSlot uint64, parent, child libcommon.Hash) {
	cI, ok := f.childrens.Load(parent)
	var c childrens
	if ok {
		c = cI.(childrens)
	}
	c.parentSlot = parentSlot // can be inaccurate.
	if slices.Contains(c.childrenHashes, child) {
		return
	}
	c.childrenHashes = append(c.childrenHashes, child)
	f.childrens.Store(parent, c)
}

// Time returns current time
func (f *ForkChoiceStore) Time() uint64 {
	return f.time.Load()
}

// ProposerBoostRoot returns proposer boost root
func (f *ForkChoiceStore) ProposerBoostRoot() libcommon.Hash {
	return f.proposerBoostRoot.Load().(libcommon.Hash)
}

// JustifiedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) JustifiedCheckpoint() solid.Checkpoint {
	return f.justifiedCheckpoint.Load().(solid.Checkpoint)
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) JustifiedSlot() uint64 {
	return f.computeStartSlotAtEpoch(f.justifiedCheckpoint.Load().(solid.Checkpoint).Epoch())
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) FinalizedCheckpoint() solid.Checkpoint {
	return f.finalizedCheckpoint.Load().(solid.Checkpoint)
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) FinalizedSlot() uint64 {
	return f.computeStartSlotAtEpoch(f.finalizedCheckpoint.Load().(solid.Checkpoint).Epoch()) + (f.beaconCfg.SlotsPerEpoch - 1)
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) Engine() execution_client.ExecutionEngine {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.engine
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) GetEth1Hash(eth2Root libcommon.Hash) libcommon.Hash {
	f.mu.RLock()
	defer f.mu.RUnlock()
	ret, _ := f.eth2Roots.Get(eth2Root)
	return ret
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) AnchorSlot() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.forkGraph.AnchorSlot()
}

func (f *ForkChoiceStore) GetStateAtBlockRoot(blockRoot libcommon.Hash, alwaysCopy bool) (*state2.CachingBeaconState, error) {
	if !alwaysCopy {
		f.mu.RLock()
		defer f.mu.RUnlock()
	}
	return f.forkGraph.GetState(blockRoot, alwaysCopy)
}

func (f *ForkChoiceStore) PreverifiedValidator(blockRoot libcommon.Hash) uint64 {
	if ret, ok := f.preverifiedSizes.Get(blockRoot); ok {
		return ret.validatorLength
	}
	return 0
}

func (f *ForkChoiceStore) PreverifiedHistoricalRoots(blockRoot libcommon.Hash) uint64 {
	if ret, ok := f.preverifiedSizes.Get(blockRoot); ok {
		return ret.historicalRootsLength
	}
	return 0
}

func (f *ForkChoiceStore) PreverifiedHistoricalSummaries(blockRoot libcommon.Hash) uint64 {
	if ret, ok := f.preverifiedSizes.Get(blockRoot); ok {
		return ret.historicalSummariesLength
	}
	return 0
}

func (f *ForkChoiceStore) GetFinalityCheckpoints(blockRoot libcommon.Hash) (bool, solid.Checkpoint, solid.Checkpoint, solid.Checkpoint) {
	if ret, ok := f.finalityCheckpoints.Get(blockRoot); ok {
		return true, ret.finalizedCheckpoint, ret.currentJustifiedCheckpoint, ret.previousJustifiedCheckpoint
	}
	return false, solid.Checkpoint{}, solid.Checkpoint{}, solid.Checkpoint{}
}

func (f *ForkChoiceStore) GetSyncCommittees(period uint64) (*solid.SyncCommittee, *solid.SyncCommittee, bool) {
	return f.forkGraph.GetSyncCommittees(period)
}

func (f *ForkChoiceStore) GetBeaconCommitee(slot, committeeIndex uint64) ([]uint64, error) {
	return f.syncedDataManager.HeadState().GetBeaconCommitee(slot, committeeIndex)
}

func (f *ForkChoiceStore) BlockRewards(root libcommon.Hash) (*eth2.BlockRewardsCollector, bool) {
	return f.forkGraph.GetBlockRewards(root)
}

func (f *ForkChoiceStore) TotalActiveBalance(root libcommon.Hash) (uint64, bool) {
	return f.totalActiveBalances.Get(root)
}

func (f *ForkChoiceStore) LowestAvaiableSlot() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.forkGraph.LowestAvaiableSlot()
}

func (f *ForkChoiceStore) RandaoMixes(blockRoot libcommon.Hash, out solid.HashListSSZ) bool {
	relevantDeltas := map[uint64]randaoDelta{}
	currentBlockRoot := blockRoot
	var currentSlot uint64
	for {
		h, ok := f.forkGraph.GetHeader(currentBlockRoot)
		if !ok {
			return false
		}
		currentSlot = h.Slot
		if f.randaoMixesLists.Contains(currentBlockRoot) {
			break
		}
		randaoDelta, ok := f.randaoDeltas.Get(currentBlockRoot)
		if !ok {
			return false
		}
		currentBlockRoot = h.ParentRoot
		if _, ok := relevantDeltas[currentSlot/f.beaconCfg.SlotsPerEpoch]; !ok {
			relevantDeltas[currentSlot/f.beaconCfg.SlotsPerEpoch] = randaoDelta
		}
	}
	randaoMixes, ok := f.randaoMixesLists.Get(currentBlockRoot)
	if !ok {
		return false
	}
	randaoMixes.CopyTo(out)
	for epoch, delta := range relevantDeltas {
		out.Set(int(epoch%f.beaconCfg.EpochsPerHistoricalVector), delta.delta)
	}
	return true
}

func (f *ForkChoiceStore) Partecipation(epoch uint64) (*solid.BitList, bool) {
	return f.participation.Get(epoch)
}

func (f *ForkChoiceStore) ForkNodes() []ForkNode {
	f.mu.RLock()
	defer f.mu.RUnlock()
	forkNodes := make([]ForkNode, 0, len(f.weights))
	for blockRoot, weight := range f.weights {
		header, has := f.forkGraph.GetHeader(blockRoot)
		if !has {
			continue
		}
		justifiedCheckpoint, has := f.forkGraph.GetCurrentJustifiedCheckpoint(blockRoot)
		if !has {
			continue
		}
		finalizedCheckpoint, has := f.forkGraph.GetFinalizedCheckpoint(blockRoot)
		if !has {
			continue
		}
		blockHash, _ := f.eth2Roots.Get(blockRoot)

		forkNodes = append(forkNodes, ForkNode{
			Weight:         weight,
			BlockRoot:      blockRoot,
			ParentRoot:     header.ParentRoot,
			JustifiedEpoch: justifiedCheckpoint.Epoch(),
			FinalizedEpoch: finalizedCheckpoint.Epoch(),
			Slot:           header.Slot,
			Validity:       "valid",
			ExecutionBlock: blockHash,
		})
	}
	sort.Slice(forkNodes, func(i, j int) bool {
		return forkNodes[i].Slot < forkNodes[j].Slot
	})
	return forkNodes
}

func (f *ForkChoiceStore) Synced() bool {
	return f.synced.Load()
}

func (f *ForkChoiceStore) SetSynced(s bool) {
	f.synced.Store(s)
}

func (f *ForkChoiceStore) GetLightClientBootstrap(blockRoot libcommon.Hash) (*cltypes.LightClientBootstrap, bool) {
	return f.forkGraph.GetLightClientBootstrap(blockRoot)
}

func (f *ForkChoiceStore) NewestLightClientUpdate() *cltypes.LightClientUpdate {
	return f.forkGraph.NewestLightClientUpdate()
}

func (f *ForkChoiceStore) GetLightClientUpdate(period uint64) (*cltypes.LightClientUpdate, bool) {
	return f.forkGraph.GetLightClientUpdate(period)
}

func (f *ForkChoiceStore) GetHeader(blockRoot libcommon.Hash) (*cltypes.BeaconBlockHeader, bool) {
	return f.forkGraph.GetHeader(blockRoot)
}

func (f *ForkChoiceStore) GetBalances(blockRoot libcommon.Hash) (solid.Uint64ListSSZ, error) {
	return f.forkGraph.GetBalances(blockRoot)
}

func (f *ForkChoiceStore) GetInactivitiesScores(blockRoot libcommon.Hash) (solid.Uint64ListSSZ, error) {
	return f.forkGraph.GetInactivitiesScores(blockRoot)
}

func (f *ForkChoiceStore) GetPreviousPartecipationIndicies(blockRoot libcommon.Hash) (*solid.BitList, error) {
	return f.forkGraph.GetPreviousPartecipationIndicies(blockRoot)
}

func (f *ForkChoiceStore) GetValidatorSet(blockRoot libcommon.Hash) (*solid.ValidatorSet, error) {
	return f.forkGraph.GetValidatorSet(blockRoot)
}

func (f *ForkChoiceStore) GetCurrentPartecipationIndicies(blockRoot libcommon.Hash) (*solid.BitList, error) {
	return f.forkGraph.GetCurrentPartecipationIndicies(blockRoot)
}

func (f *ForkChoiceStore) IsRootOptimistic(root libcommon.Hash) bool {
	return f.optimisticStore.IsOptimistic(root)
}

func (f *ForkChoiceStore) IsHeadOptimistic() bool {
	if f.ethClock.GetCurrentEpoch() < f.beaconCfg.BellatrixForkEpoch {
		return false
	}

	headState := f.syncedDataManager.HeadState()
	if headState == nil {
		return true
	}
	// get latest root
	latestRoot := headState.LatestBlockHeader().Root
	return f.optimisticStore.IsOptimistic(latestRoot)
}

func (f *ForkChoiceStore) DumpBeaconStateOnDisk(bs *state.CachingBeaconState) error {
	anchorRoot, err := bs.BlockRoot()
	if err != nil {
		return err
	}
	return f.forkGraph.DumpBeaconStateOnDisk(anchorRoot, bs, false)
}
