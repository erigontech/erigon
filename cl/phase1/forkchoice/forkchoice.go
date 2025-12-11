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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/optimistic"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"

	lru "github.com/hashicorp/golang-lru/v2"
)

// ForkNode is a struct that represents a node in the fork choice tree.
type ForkNode struct {
	Slot           uint64      `json:"slot,string"`
	BlockRoot      common.Hash `json:"block_root"`
	ParentRoot     common.Hash `json:"parent_root"`
	JustifiedEpoch uint64      `json:"justified_epoch,string"`
	FinalizedEpoch uint64      `json:"finalized_epoch,string"`
	Weight         uint64      `json:"weight,string"`
	Validity       string      `json:"validity"`
	ExecutionBlock common.Hash `json:"execution_block_hash"`
}

const (
	checkpointsPerCache = 1024
	allowedCachedStates = 8
	queueCacheSize      = 128
)

type randaoDelta struct {
	epoch uint64
	delta common.Hash
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

	proposerBoostRoot        atomic.Value
	headHash                 common.Hash
	headSlot                 uint64
	genesisTime              uint64
	genesisValidatorsRoot    common.Hash
	weights                  map[common.Hash]uint64
	headSet                  map[common.Hash]struct{}
	hotSidecars              map[common.Hash][]*cltypes.BlobSidecar // Set of sidecars that are not yet processed.
	verifiedExecutionPayload *lru.Cache[common.Hash, struct{}]
	// childrens
	childrens sync.Map

	// Use go map because this is actually an unordered set
	equivocatingIndicies []byte
	forkGraph            fork_graph.ForkGraph
	blobStorage          blob_storage.BlobStorage
	peerDas              das.PeerDas
	// I use the cache due to the convenient auto-cleanup feauture.
	checkpointStates   sync.Map // We keep ssz snappy of it as the full beacon state is full of rendundant data.
	publicKeysRegistry public_keys_registry.PublicKeyRegistry
	localValidators    *validator_params.ValidatorParams

	latestMessages    *latestMessagesStore
	syncedDataManager *synced_data.SyncedDataManager
	// We keep track of them so that we can forkchoice with EL.
	eth2Roots *lru.Cache[common.Hash, common.Hash] // ETH2 root -> ETH1 hash
	// preverifid sizes and other data collection
	preverifiedSizes    *lru.Cache[common.Hash, preverifiedAppendListsSizes]
	finalityCheckpoints *lru.Cache[common.Hash, finalityCheckpoints]
	totalActiveBalances *lru.Cache[common.Hash, uint64]
	nextBlockProposers  *lru.Cache[common.Hash, []uint64]
	// Randao mixes
	randaoMixesLists *lru.Cache[common.Hash, solid.HashListSSZ] // limited randao mixes full list (only 16 elements)
	randaoDeltas     *lru.Cache[common.Hash, randaoDelta]       // small entry can be lots of elements.
	// participation tracking
	participation *lru.Cache[uint64, *solid.ParticipationBitList] // epoch -> [participation]

	// consolidations/pending deposits/withdrawals
	pendingConsolidations *lru.Cache[common.Hash, *solid.ListSSZ[*solid.PendingConsolidation]]
	pendingDeposits       *lru.Cache[common.Hash, *solid.ListSSZ[*solid.PendingDeposit]]
	partialWithdrawals    *lru.Cache[common.Hash, *solid.ListSSZ[*solid.PendingPartialWithdrawal]]

	proposerLookahead *lru.Cache[uint64, solid.Uint64VectorSSZ]

	mu sync.RWMutex

	// EL
	engine execution_client.ExecutionEngine

	// operations pool
	operationsPool pool.OperationsPool
	beaconCfg      *clparams.BeaconChainConfig

	emitters *beaconevents.EventEmitter
	synced   atomic.Bool

	ethClock                eth_clock.EthereumClock
	optimisticStore         optimistic.OptimisticStore
	probabilisticHeadGetter bool
}

type LatestMessage struct {
	Epoch uint64
	Root  common.Hash
}

type childrens struct {
	childrenHashes []common.Hash
	parentSlot     uint64 // we keep this one for pruning
}

// NewForkChoiceStore initialize a new store from the given anchor state, either genesis or checkpoint sync state.
func NewForkChoiceStore(
	ethClock eth_clock.EthereumClock,
	anchorState *state2.CachingBeaconState,
	engine execution_client.ExecutionEngine,
	operationsPool pool.OperationsPool,
	forkGraph fork_graph.ForkGraph,
	emitters *beaconevents.EventEmitter,
	syncedDataManager *synced_data.SyncedDataManager,
	blobStorage blob_storage.BlobStorage,
	publicKeysRegistry public_keys_registry.PublicKeyRegistry,
	localValidators *validator_params.ValidatorParams,
	probabilisticHeadGetter bool,
) (*ForkChoiceStore, error) {
	anchorRoot, err := anchorState.BlockRoot()
	if err != nil {
		return nil, err
	}

	anchorCheckpoint := solid.Checkpoint{
		Root:  anchorRoot,
		Epoch: state2.Epoch(anchorState.BeaconState),
	}

	verifiedExecutionPayload, err := lru.New[common.Hash, struct{}](1024)
	if err != nil {
		return nil, err
	}

	eth2Roots, err := lru.New[common.Hash, common.Hash](checkpointsPerCache)
	if err != nil {
		return nil, err
	}

	randaoMixesLists, err := lru.New[common.Hash, solid.HashListSSZ](allowedCachedStates)
	if err != nil {
		return nil, err
	}

	randaoDeltas, err := lru.New[common.Hash, randaoDelta](checkpointsPerCache)
	if err != nil {
		return nil, err
	}

	finalityCheckpoints, err := lru.New[common.Hash, finalityCheckpoints](checkpointsPerCache)
	if err != nil {
		return nil, err
	}

	preverifiedSizes, err := lru.New[common.Hash, preverifiedAppendListsSizes](checkpointsPerCache * 10)
	if err != nil {
		return nil, err
	}
	preverifiedSizes.Add(anchorRoot, preverifiedAppendListsSizes{
		validatorLength:           uint64(anchorState.ValidatorLength()),
		historicalRootsLength:     anchorState.HistoricalRootsLength(),
		historicalSummariesLength: anchorState.HistoricalSummariesLength(),
	})

	totalActiveBalances, err := lru.New[common.Hash, uint64](checkpointsPerCache * 10)
	if err != nil {
		return nil, err
	}

	participation, err := lru.New[uint64, *solid.ParticipationBitList](16)
	if err != nil {
		return nil, err
	}

	nextBlockProposers, err := lru.New[common.Hash, []uint64](checkpointsPerCache * 10)
	if err != nil {
		return nil, err
	}

	partialWithdrawals, err := lru.New[common.Hash, *solid.ListSSZ[*solid.PendingPartialWithdrawal]](queueCacheSize)
	if err != nil {
		return nil, err
	}

	pendingConsolidations, err := lru.New[common.Hash, *solid.ListSSZ[*solid.PendingConsolidation]](queueCacheSize)
	if err != nil {
		return nil, err
	}

	pendingDeposits, err := lru.New[common.Hash, *solid.ListSSZ[*solid.PendingDeposit]](queueCacheSize)
	if err != nil {
		return nil, err
	}
	proposerLookahead, err := lru.New[uint64, solid.Uint64VectorSSZ](queueCacheSize)
	if err != nil {
		return nil, err
	}

	publicKeysRegistry.ResetAnchor(anchorState)
	participation.Add(state.Epoch(anchorState.BeaconState), anchorState.CurrentEpochParticipation().Copy())

	totalActiveBalances.Add(anchorRoot, anchorState.GetTotalActiveBalance())
	r := solid.NewHashVector(int(anchorState.BeaconConfig().EpochsPerHistoricalVector))
	anchorState.RandaoMixes().CopyTo(r)
	randaoMixesLists.Add(anchorRoot, r)
	headSet := make(map[common.Hash]struct{})
	headSet[anchorRoot] = struct{}{}
	f := &ForkChoiceStore{
		forkGraph:                forkGraph,
		equivocatingIndicies:     make([]byte, anchorState.ValidatorLength(), anchorState.ValidatorLength()*2),
		latestMessages:           newLatestMessagesStore(anchorState.ValidatorLength()),
		eth2Roots:                eth2Roots,
		engine:                   engine,
		operationsPool:           operationsPool,
		beaconCfg:                anchorState.BeaconConfig(),
		preverifiedSizes:         preverifiedSizes,
		finalityCheckpoints:      finalityCheckpoints,
		totalActiveBalances:      totalActiveBalances,
		randaoMixesLists:         randaoMixesLists,
		randaoDeltas:             randaoDeltas,
		headSet:                  headSet,
		weights:                  make(map[common.Hash]uint64),
		participation:            participation,
		emitters:                 emitters,
		genesisTime:              anchorState.GenesisTime(),
		syncedDataManager:        syncedDataManager,
		nextBlockProposers:       nextBlockProposers,
		genesisValidatorsRoot:    anchorState.GenesisValidatorsRoot(),
		hotSidecars:              make(map[common.Hash][]*cltypes.BlobSidecar),
		blobStorage:              blobStorage,
		ethClock:                 ethClock,
		optimisticStore:          optimistic.NewOptimisticStore(),
		probabilisticHeadGetter:  probabilisticHeadGetter,
		publicKeysRegistry:       publicKeysRegistry,
		verifiedExecutionPayload: verifiedExecutionPayload,
		localValidators:          localValidators,
		pendingConsolidations:    pendingConsolidations,
		pendingDeposits:          pendingDeposits,
		partialWithdrawals:       partialWithdrawals,
		proposerLookahead:        proposerLookahead,
	}
	f.justifiedCheckpoint.Store(anchorCheckpoint)
	f.finalizedCheckpoint.Store(anchorCheckpoint)
	f.unrealizedFinalizedCheckpoint.Store(anchorCheckpoint)
	f.unrealizedJustifiedCheckpoint.Store(anchorCheckpoint)
	f.proposerBoostRoot.Store(common.Hash{})

	f.highestSeen.Store(anchorState.Slot())
	f.time.Store(anchorState.GenesisTime() + anchorState.BeaconConfig().SecondsPerSlot*anchorState.Slot())
	return f, nil
}

func (f *ForkChoiceStore) InitPeerDas(peerDas das.PeerDas) {
	// this is a hack to inject the peer das
	f.peerDas = peerDas
}

func (f *ForkChoiceStore) GetPeerDas() das.PeerDas {
	return f.peerDas
}

// Highest seen returns highest seen slot
func (f *ForkChoiceStore) HighestSeen() uint64 {
	return f.highestSeen.Load()
}

func (f *ForkChoiceStore) children(parent common.Hash) []common.Hash {
	children, ok := f.childrens.Load(parent)
	if !ok {
		return nil
	}
	return children.(childrens).childrenHashes
}

// updateChildren adds a new child to the parent node hash.
func (f *ForkChoiceStore) updateChildren(parentSlot uint64, parent, child common.Hash) {
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
func (f *ForkChoiceStore) ProposerBoostRoot() common.Hash {
	return f.proposerBoostRoot.Load().(common.Hash)
}

// JustifiedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) JustifiedCheckpoint() solid.Checkpoint {
	return f.justifiedCheckpoint.Load().(solid.Checkpoint)
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) JustifiedSlot() uint64 {
	return f.computeStartSlotAtEpoch(f.justifiedCheckpoint.Load().(solid.Checkpoint).Epoch)
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) FinalizedCheckpoint() solid.Checkpoint {
	return f.finalizedCheckpoint.Load().(solid.Checkpoint)
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) FinalizedSlot() uint64 {
	return f.computeStartSlotAtEpoch(f.finalizedCheckpoint.Load().(solid.Checkpoint).Epoch) + (f.beaconCfg.SlotsPerEpoch - 1)
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) Engine() execution_client.ExecutionEngine {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.engine
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) GetEth1Hash(eth2Root common.Hash) common.Hash {
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

func (f *ForkChoiceStore) GetStateAtBlockRoot(blockRoot common.Hash, alwaysCopy bool) (*state2.CachingBeaconState, error) {
	if !alwaysCopy {
		f.mu.RLock()
		defer f.mu.RUnlock()
	}
	return f.forkGraph.GetState(blockRoot, alwaysCopy)
}

func (f *ForkChoiceStore) PreverifiedValidator(blockRoot common.Hash) uint64 {
	if ret, ok := f.preverifiedSizes.Get(blockRoot); ok {
		return ret.validatorLength
	}
	return 0
}

func (f *ForkChoiceStore) PreverifiedHistoricalRoots(blockRoot common.Hash) uint64 {
	if ret, ok := f.preverifiedSizes.Get(blockRoot); ok {
		return ret.historicalRootsLength
	}
	return 0
}

func (f *ForkChoiceStore) PreverifiedHistoricalSummaries(blockRoot common.Hash) uint64 {
	if ret, ok := f.preverifiedSizes.Get(blockRoot); ok {
		return ret.historicalSummariesLength
	}
	return 0
}

func (f *ForkChoiceStore) GetFinalityCheckpoints(blockRoot common.Hash) (solid.Checkpoint, solid.Checkpoint, solid.Checkpoint, bool) {
	if ret, ok := f.finalityCheckpoints.Get(blockRoot); ok {
		return ret.finalizedCheckpoint, ret.currentJustifiedCheckpoint, ret.previousJustifiedCheckpoint, true
	}
	return solid.Checkpoint{}, solid.Checkpoint{}, solid.Checkpoint{}, false
}

func (f *ForkChoiceStore) GetSyncCommittees(period uint64) (*solid.SyncCommittee, *solid.SyncCommittee, bool) {
	return f.forkGraph.GetSyncCommittees(period)
}

func (f *ForkChoiceStore) BlockRewards(root common.Hash) (*eth2.BlockRewardsCollector, bool) {
	return f.forkGraph.GetBlockRewards(root)
}

func (f *ForkChoiceStore) TotalActiveBalance(root common.Hash) (uint64, bool) {
	return f.totalActiveBalances.Get(root)
}

func (f *ForkChoiceStore) LowestAvailableSlot() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.forkGraph.LowestAvailableSlot()
}

func (f *ForkChoiceStore) RandaoMixes(blockRoot common.Hash, out solid.HashListSSZ) bool {
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

func (f *ForkChoiceStore) Participation(epoch uint64) (*solid.ParticipationBitList, bool) {
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
			JustifiedEpoch: justifiedCheckpoint.Epoch,
			FinalizedEpoch: finalizedCheckpoint.Epoch,
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

func (f *ForkChoiceStore) GetLightClientBootstrap(blockRoot common.Hash) (*cltypes.LightClientBootstrap, bool) {
	return f.forkGraph.GetLightClientBootstrap(blockRoot)
}

func (f *ForkChoiceStore) NewestLightClientUpdate() *cltypes.LightClientUpdate {
	return f.forkGraph.NewestLightClientUpdate()
}

func (f *ForkChoiceStore) GetLightClientUpdate(period uint64) (*cltypes.LightClientUpdate, bool) {
	return f.forkGraph.GetLightClientUpdate(period)
}

func (f *ForkChoiceStore) GetHeader(blockRoot common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	return f.forkGraph.GetHeader(blockRoot)
}

func (f *ForkChoiceStore) GetBalances(blockRoot common.Hash) (solid.Uint64ListSSZ, error) {
	return f.forkGraph.GetBalances(blockRoot)
}

func (f *ForkChoiceStore) GetInactivitiesScores(blockRoot common.Hash) (solid.Uint64ListSSZ, error) {
	return f.forkGraph.GetInactivitiesScores(blockRoot)
}

func (f *ForkChoiceStore) GetPreviousParticipationIndicies(blockRoot common.Hash) (*solid.ParticipationBitList, error) {
	header, ok := f.GetHeader(blockRoot)
	if !ok {
		return nil, nil
	}
	return f.forkGraph.GetPreviousParticipationIndicies(header.Slot / f.beaconCfg.SlotsPerEpoch)
}

func (f *ForkChoiceStore) GetValidatorSet(blockRoot common.Hash) (*solid.ValidatorSet, error) {
	return f.forkGraph.GetValidatorSet(blockRoot)
}

func (f *ForkChoiceStore) GetCurrentParticipationIndicies(blockRoot common.Hash) (*solid.ParticipationBitList, error) {
	header, ok := f.GetHeader(blockRoot)
	if !ok {
		return nil, nil
	}
	return f.forkGraph.GetCurrentParticipationIndicies(header.Slot / f.beaconCfg.SlotsPerEpoch)
}

func (f *ForkChoiceStore) IsRootOptimistic(root common.Hash) bool {
	return f.optimisticStore.IsOptimistic(root)
}

func (f *ForkChoiceStore) IsHeadOptimistic() bool {
	if f.ethClock.GetCurrentEpoch() < f.beaconCfg.BellatrixForkEpoch {
		return false
	}

	return f.optimisticStore.IsOptimistic(f.syncedDataManager.HeadRoot())
}

func (f *ForkChoiceStore) DumpBeaconStateOnDisk(bs *state.CachingBeaconState) error {
	anchorRoot, err := bs.BlockRoot()
	if err != nil {
		return err
	}
	return f.forkGraph.DumpBeaconStateOnDisk(anchorRoot, bs, false)
}

func (f *ForkChoiceStore) addPendingConsolidations(blockRoot common.Hash, pendingConsolidations *solid.ListSSZ[*solid.PendingConsolidation]) {
	// first check if we already have the same list in the parent node.
	header, ok := f.forkGraph.GetHeader(blockRoot)
	// If there is no header, make a copy.
	if !ok {
		pendingConsolidationsCopy := solid.NewPendingConsolidationList(f.beaconCfg)
		for i := 0; i < pendingConsolidations.Len(); i++ {
			pendingConsolidationsCopy.Append(pendingConsolidations.Get(i))
		}
		f.pendingConsolidations.Add(blockRoot, pendingConsolidationsCopy)
		return
	}
	parentRoot := header.ParentRoot
	parentConsolidations, ok := f.pendingConsolidations.Get(parentRoot)
	if !ok {
		pendingConsolidationsCopy := solid.NewPendingConsolidationList(f.beaconCfg)
		for i := 0; i < pendingConsolidations.Len(); i++ {
			pendingConsolidationsCopy.Append(pendingConsolidations.Get(i))
		}
		f.pendingConsolidations.Add(blockRoot, pendingConsolidationsCopy)
		return
	}

	// check if the two lists are equal via their hashes.
	pendingConsolidationsHash, err := pendingConsolidations.HashSSZ()
	if err != nil {
		panic(err)
	}
	parentConsolidationsHash, err := parentConsolidations.HashSSZ()
	if err != nil {
		panic(err)
	}
	if pendingConsolidationsHash == parentConsolidationsHash {
		// If they are equal, we can just store the parent consolidations.
		f.pendingConsolidations.Add(blockRoot, parentConsolidations)
		return
	}
	pendingConsolidationsCopy := solid.NewPendingConsolidationList(f.beaconCfg)
	for i := 0; i < pendingConsolidations.Len(); i++ {
		pendingConsolidationsCopy.Append(pendingConsolidations.Get(i))
	}
	f.pendingConsolidations.Add(blockRoot, pendingConsolidationsCopy)
}

func (f *ForkChoiceStore) addPendingDeposits(blockRoot common.Hash, pendingDeposits *solid.ListSSZ[*solid.PendingDeposit]) {
	// first check if we already have the same list in the parent node.
	header, ok := f.forkGraph.GetHeader(blockRoot)
	// If there is no header, make a copy.
	if !ok {
		pendingDepositsCopy := solid.NewPendingDepositList(f.beaconCfg)
		for i := 0; i < pendingDeposits.Len(); i++ {
			pendingDepositsCopy.Append(pendingDeposits.Get(i))
		}
		f.pendingDeposits.Add(blockRoot, pendingDepositsCopy)
		return
	}
	parentRoot := header.ParentRoot
	parentDeposits, ok := f.pendingDeposits.Get(parentRoot)
	if !ok {
		pendingDepositsCopy := solid.NewPendingDepositList(f.beaconCfg)
		for i := 0; i < pendingDeposits.Len(); i++ {
			pendingDepositsCopy.Append(pendingDeposits.Get(i))
		}
		f.pendingDeposits.Add(blockRoot, pendingDepositsCopy)
		return
	}

	// check if the two lists are equal via their hashes.
	pendingDepositsHash, err := pendingDeposits.HashSSZ()
	if err != nil {
		panic(err)
	}
	parentDepositsHash, err := parentDeposits.HashSSZ()
	if err != nil {
		panic(err)
	}
	if pendingDepositsHash == parentDepositsHash {
		// If they are equal, we can just store the parent deposits.
		f.pendingDeposits.Add(blockRoot, parentDeposits)
		return
	}
	pendingDepositsCopy := solid.NewPendingDepositList(f.beaconCfg)
	for i := 0; i < pendingDeposits.Len(); i++ {
		pendingDepositsCopy.Append(pendingDeposits.Get(i))
	}
	f.pendingDeposits.Add(blockRoot, pendingDepositsCopy)
}

func (f *ForkChoiceStore) addPendingPartialWithdrawals(blockRoot common.Hash, pendingPartialWithdrawals *solid.ListSSZ[*solid.PendingPartialWithdrawal]) {
	// first check if we already have the same list in the parent node.
	header, ok := f.forkGraph.GetHeader(blockRoot)
	// If there is no header, make a copy.
	if !ok {
		pendingPartialWithdrawalsCopy := solid.NewPendingWithdrawalList(f.beaconCfg)
		for i := 0; i < pendingPartialWithdrawals.Len(); i++ {
			pendingPartialWithdrawalsCopy.Append(pendingPartialWithdrawals.Get(i))
		}
		f.partialWithdrawals.Add(blockRoot, pendingPartialWithdrawalsCopy)
		return
	}
	parentRoot := header.ParentRoot
	parentWithdrawals, ok := f.partialWithdrawals.Get(parentRoot)
	if !ok {
		pendingPartialWithdrawalsCopy := solid.NewPendingWithdrawalList(f.beaconCfg)
		for i := 0; i < pendingPartialWithdrawals.Len(); i++ {
			pendingPartialWithdrawalsCopy.Append(pendingPartialWithdrawals.Get(i))
		}
		f.partialWithdrawals.Add(blockRoot, pendingPartialWithdrawalsCopy)
		return
	}

	// check if the two lists are equal via their hashes.
	pendingPartialWithdrawalsHash, err := pendingPartialWithdrawals.HashSSZ()
	if err != nil {
		panic(err)
	}
	parentWithdrawalsHash, err := parentWithdrawals.HashSSZ()
	if err != nil {
		panic(err)
	}
	if pendingPartialWithdrawalsHash == parentWithdrawalsHash {
		// If they are equal, we can just store the parent withdrawals.
		f.partialWithdrawals.Add(blockRoot, parentWithdrawals)
		return
	}
	pendingPartialWithdrawalsCopy := solid.NewPendingWithdrawalList(f.beaconCfg)
	for i := 0; i < pendingPartialWithdrawals.Len(); i++ {
		pendingPartialWithdrawalsCopy.Append(pendingPartialWithdrawals.Get(i))
	}
	f.partialWithdrawals.Add(blockRoot, pendingPartialWithdrawalsCopy)
}

func (f *ForkChoiceStore) addProposerLookahead(slot uint64, proposerLookahead solid.Uint64VectorSSZ) {
	epoch := slot / f.beaconCfg.SlotsPerEpoch
	if _, ok := f.proposerLookahead.Get(epoch); !ok {
		pl := solid.NewUint64VectorSSZ(proposerLookahead.Length())
		proposerLookahead.CopyTo(pl)
		f.proposerLookahead.Add(epoch, pl)
	}
}

func (f *ForkChoiceStore) GetPendingConsolidations(blockRoot common.Hash) (*solid.ListSSZ[*solid.PendingConsolidation], bool) {
	return f.pendingConsolidations.Get(blockRoot)
}

func (f *ForkChoiceStore) GetPendingDeposits(blockRoot common.Hash) (*solid.ListSSZ[*solid.PendingDeposit], bool) {
	return f.pendingDeposits.Get(blockRoot)
}

func (f *ForkChoiceStore) GetPendingPartialWithdrawals(blockRoot common.Hash) (*solid.ListSSZ[*solid.PendingPartialWithdrawal], bool) {
	return f.partialWithdrawals.Get(blockRoot)
}

func (f *ForkChoiceStore) GetProposerLookahead(slot uint64) (solid.Uint64VectorSSZ, bool) {
	epoch := slot / f.beaconCfg.SlotsPerEpoch
	return f.proposerLookahead.Get(epoch)
}
