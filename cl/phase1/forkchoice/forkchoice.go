package forkchoice

import (
	"sync"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/freezer"
	state2 "github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/fork_graph"

	lru "github.com/hashicorp/golang-lru/v2"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

type checkpointComparable string

const (
	checkpointsPerCache = 1024
	allowedCachedStates = 4
)

type ForkChoiceStore struct {
	time                          uint64
	highestSeen                   uint64
	justifiedCheckpoint           solid.Checkpoint
	finalizedCheckpoint           solid.Checkpoint
	unrealizedJustifiedCheckpoint solid.Checkpoint
	unrealizedFinalizedCheckpoint solid.Checkpoint
	proposerBoostRoot             libcommon.Hash
	// Use go map because this is actually an unordered set
	equivocatingIndicies map[uint64]struct{}
	forkGraph            *fork_graph.ForkGraph
	// I use the cache due to the convenient auto-cleanup feauture.
	checkpointStates *lru.Cache[checkpointComparable, *checkpointState] // We keep ssz snappy of it as the full beacon state is full of rendundant data.
	latestMessages   map[uint64]*LatestMessage
	// We keep track of them so that we can forkchoice with EL.
	eth2Roots *lru.Cache[libcommon.Hash, libcommon.Hash] // ETH2 root -> ETH1 hash
	mu        sync.Mutex
	// EL
	engine execution_client.ExecutionEngine
	// freezer
	recorder freezer.Freezer
}

type LatestMessage struct {
	Epoch uint64
	Root  libcommon.Hash
}

// NewForkChoiceStore initialize a new store from the given anchor state, either genesis or checkpoint sync state.
func NewForkChoiceStore(anchorState *state2.CachingBeaconState, engine execution_client.ExecutionEngine, recorder freezer.Freezer, enabledPruning bool) (*ForkChoiceStore, error) {
	anchorRoot, err := anchorState.BlockRoot()
	if err != nil {
		return nil, err
	}
	anchorCheckpoint := solid.NewCheckpointFromParameters(
		anchorRoot,
		state2.Epoch(anchorState.BeaconState),
	)
	checkpointStates, err := lru.New[checkpointComparable, *checkpointState](allowedCachedStates)
	if err != nil {
		return nil, err
	}
	eth2Roots, err := lru.New[libcommon.Hash, libcommon.Hash](checkpointsPerCache)
	if err != nil {
		return nil, err
	}
	return &ForkChoiceStore{
		highestSeen:                   anchorState.Slot(),
		time:                          anchorState.GenesisTime() + anchorState.BeaconConfig().SecondsPerSlot*anchorState.Slot(),
		justifiedCheckpoint:           anchorCheckpoint.Copy(),
		finalizedCheckpoint:           anchorCheckpoint.Copy(),
		unrealizedJustifiedCheckpoint: anchorCheckpoint.Copy(),
		unrealizedFinalizedCheckpoint: anchorCheckpoint.Copy(),
		forkGraph:                     fork_graph.New(anchorState, enabledPruning),
		equivocatingIndicies:          map[uint64]struct{}{},
		latestMessages:                map[uint64]*LatestMessage{},
		checkpointStates:              checkpointStates,
		eth2Roots:                     eth2Roots,
		engine:                        engine,
		recorder:                      recorder,
	}, nil
}

// Highest seen returns highest seen slot
func (f *ForkChoiceStore) HighestSeen() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.highestSeen
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
func (f *ForkChoiceStore) FinalizedCheckpoint() solid.Checkpoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.finalizedCheckpoint
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) FinalizedSlot() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.computeStartSlotAtEpoch(f.finalizedCheckpoint.Epoch())
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
