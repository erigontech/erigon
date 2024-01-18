package forkchoice

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/freezer"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	state2 "github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2"
	"golang.org/x/exp/slices"

	lru "github.com/hashicorp/golang-lru/v2"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
)

// Schema
/*
{
      "slot": "1",
      "block_root": "0xcf8e0d4e9587369b2301d0790347320302cc0943d5a1884560367e8208d920f2",
      "parent_root": "0xcf8e0d4e9587369b2301d0790347320302cc0943d5a1884560367e8208d920f2",
      "justified_epoch": "1",
      "finalized_epoch": "1",
      "weight": "1",
      "validity": "valid",
      "execution_block_hash": "0xcf8e0d4e9587369b2301d0790347320302cc0943d5a1884560367e8208d920f2",
      "extra_data": {}
    }
*/
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
	ctx                           context.Context
	time                          uint64
	highestSeen                   uint64
	justifiedCheckpoint           solid.Checkpoint
	finalizedCheckpoint           solid.Checkpoint
	unrealizedJustifiedCheckpoint solid.Checkpoint
	unrealizedFinalizedCheckpoint solid.Checkpoint
	proposerBoostRoot             libcommon.Hash
	// attestations that are not yet processed
	attestationSet sync.Map
	// head data
	headHash    libcommon.Hash
	headSlot    uint64
	genesisTime uint64
	weights     map[libcommon.Hash]uint64
	headSet     map[libcommon.Hash]struct{}
	// childrens
	childrens map[libcommon.Hash]childrens

	// Use go map because this is actually an unordered set
	equivocatingIndicies []byte
	forkGraph            fork_graph.ForkGraph
	// I use the cache due to the convenient auto-cleanup feauture.
	checkpointStates map[checkpointComparable]*checkpointState // We keep ssz snappy of it as the full beacon state is full of rendundant data.
	latestMessages   []LatestMessage
	anchorPublicKeys []byte
	// We keep track of them so that we can forkchoice with EL.
	eth2Roots *lru.Cache[libcommon.Hash, libcommon.Hash] // ETH2 root -> ETH1 hash
	// preverifid sizes and other data collection
	preverifiedSizes    *lru.Cache[libcommon.Hash, preverifiedAppendListsSizes]
	finalityCheckpoints *lru.Cache[libcommon.Hash, finalityCheckpoints]
	totalActiveBalances *lru.Cache[libcommon.Hash, uint64]
	// Randao mixes
	randaoMixesLists *lru.Cache[libcommon.Hash, solid.HashListSSZ] // limited randao mixes full list (only 16 elements)
	randaoDeltas     *lru.Cache[libcommon.Hash, randaoDelta]       // small entry can be lots of elements.
	// participation tracking
	participation *lru.Cache[uint64, *solid.BitList] // epoch -> [partecipation]

	mu sync.Mutex
	// EL
	engine execution_client.ExecutionEngine
	// freezer
	recorder freezer.Freezer
	// operations pool
	operationsPool pool.OperationsPool
	beaconCfg      *clparams.BeaconChainConfig

	emitters *beaconevents.Emitters
	synced   atomic.Bool
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
func NewForkChoiceStore(ctx context.Context, anchorState *state2.CachingBeaconState, engine execution_client.ExecutionEngine, recorder freezer.Freezer, operationsPool pool.OperationsPool, forkGraph fork_graph.ForkGraph, emitters *beaconevents.Emitters) (*ForkChoiceStore, error) {
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

	participation.Add(state.Epoch(anchorState.BeaconState), anchorState.CurrentEpochParticipation().Copy())

	totalActiveBalances.Add(anchorRoot, anchorState.GetTotalActiveBalance())
	r := solid.NewHashVector(int(anchorState.BeaconConfig().EpochsPerHistoricalVector))
	anchorState.RandaoMixes().CopyTo(r)
	randaoMixesLists.Add(anchorRoot, r)
	headSet := make(map[libcommon.Hash]struct{})
	headSet[anchorRoot] = struct{}{}
	return &ForkChoiceStore{
		ctx:                           ctx,
		highestSeen:                   anchorState.Slot(),
		time:                          anchorState.GenesisTime() + anchorState.BeaconConfig().SecondsPerSlot*anchorState.Slot(),
		justifiedCheckpoint:           anchorCheckpoint.Copy(),
		finalizedCheckpoint:           anchorCheckpoint.Copy(),
		unrealizedJustifiedCheckpoint: anchorCheckpoint.Copy(),
		unrealizedFinalizedCheckpoint: anchorCheckpoint.Copy(),
		forkGraph:                     forkGraph,
		equivocatingIndicies:          make([]byte, anchorState.ValidatorLength(), anchorState.ValidatorLength()*2),
		latestMessages:                make([]LatestMessage, anchorState.ValidatorLength(), anchorState.ValidatorLength()*2),
		checkpointStates:              make(map[checkpointComparable]*checkpointState),
		eth2Roots:                     eth2Roots,
		engine:                        engine,
		recorder:                      recorder,
		operationsPool:                operationsPool,
		anchorPublicKeys:              anchorPublicKeys,
		genesisTime:                   anchorState.GenesisTime(),
		beaconCfg:                     anchorState.BeaconConfig(),
		childrens:                     make(map[libcommon.Hash]childrens),
		preverifiedSizes:              preverifiedSizes,
		finalityCheckpoints:           finalityCheckpoints,
		totalActiveBalances:           totalActiveBalances,
		randaoMixesLists:              randaoMixesLists,
		randaoDeltas:                  randaoDeltas,
		headSet:                       headSet,
		weights:                       make(map[libcommon.Hash]uint64),
		participation:                 participation,
		emitters:                      emitters,
	}, nil
}

// Highest seen returns highest seen slot
func (f *ForkChoiceStore) HighestSeen() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.highestSeen
}

func (f *ForkChoiceStore) children(parent libcommon.Hash) []libcommon.Hash {
	children, ok := f.childrens[parent]
	if !ok {
		return nil
	}
	return children.childrenHashes
}

// updateChildren adds a new child to the parent node hash.
func (f *ForkChoiceStore) updateChildren(parentSlot uint64, parent, child libcommon.Hash) {
	c, ok := f.childrens[parent]
	if !ok {
		c = childrens{}
	}
	c.parentSlot = parentSlot // can be innacurate.
	if slices.Contains(c.childrenHashes, child) {
		return
	}
	c.childrenHashes = append(c.childrenHashes, child)
	f.childrens[parent] = c
}

// AdvanceHighestSeen advances the highest seen block by n and returns the new slot after the change
func (f *ForkChoiceStore) AdvanceHighestSeen(n uint64) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.highestSeen += n
	return f.highestSeen
}

// Time returns current time
func (f *ForkChoiceStore) Time() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.time
}

// ProposerBoostRoot returns proposer boost root
func (f *ForkChoiceStore) ProposerBoostRoot() libcommon.Hash {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.proposerBoostRoot
}

// JustifiedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) JustifiedCheckpoint() solid.Checkpoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.justifiedCheckpoint
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) JustifiedSlot() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.computeStartSlotAtEpoch(f.justifiedCheckpoint.Epoch())
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) FinalizedCheckpoint() solid.Checkpoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.finalizedCheckpoint
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) FinalizedSlot() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.computeStartSlotAtEpoch(f.finalizedCheckpoint.Epoch()) + (f.beaconCfg.SlotsPerEpoch - 1)
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) Engine() execution_client.ExecutionEngine {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.engine
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) GetEth1Hash(eth2Root libcommon.Hash) libcommon.Hash {
	f.mu.Lock()
	defer f.mu.Unlock()
	ret, _ := f.eth2Roots.Get(eth2Root)
	return ret
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) AnchorSlot() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.forkGraph.AnchorSlot()
}

func (f *ForkChoiceStore) GetStateAtBlockRoot(blockRoot libcommon.Hash, alwaysCopy bool) (*state2.CachingBeaconState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.forkGraph.GetState(blockRoot, alwaysCopy)
}
func (f *ForkChoiceStore) GetStateAtStateRoot(stateRoot libcommon.Hash, alwaysCopy bool) (*state2.CachingBeaconState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.forkGraph.GetState(stateRoot, alwaysCopy)
}
func (f *ForkChoiceStore) GetStateAtSlot(slot uint64, alwaysCopy bool) (*state.CachingBeaconState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.forkGraph.GetStateAtSlot(slot, alwaysCopy)
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
	f.mu.Lock()
	defer f.mu.Unlock()
	if ret, ok := f.finalityCheckpoints.Get(blockRoot); ok {
		return true, ret.finalizedCheckpoint, ret.currentJustifiedCheckpoint, ret.previousJustifiedCheckpoint
	}
	return false, solid.Checkpoint{}, solid.Checkpoint{}, solid.Checkpoint{}
}

func (f *ForkChoiceStore) GetSyncCommittees(blockRoot libcommon.Hash) (*solid.SyncCommittee, *solid.SyncCommittee, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.forkGraph.GetSyncCommittees(blockRoot)
}

func (f *ForkChoiceStore) BlockRewards(root libcommon.Hash) (*eth2.BlockRewardsCollector, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.forkGraph.GetBlockRewards(root)
}

func (f *ForkChoiceStore) TotalActiveBalance(root libcommon.Hash) (uint64, bool) {
	return f.totalActiveBalances.Get(root)
}

func (f *ForkChoiceStore) LowestAvaiableSlot() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.forkGraph.LowestAvaiableSlot()
}

func (f *ForkChoiceStore) RandaoMixes(blockRoot libcommon.Hash, out solid.HashListSSZ) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	f.mu.Lock()
	defer f.mu.Unlock()
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
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.synced.Load()
}

func (f *ForkChoiceStore) SetSynced(s bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.synced.Store(s)
}
