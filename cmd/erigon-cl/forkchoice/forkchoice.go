package forkchoice

import (
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/execution_client"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/forkchoice/fork_graph"
)

const (
	checkpointsPerCache = 1024
	allowedCachedStates = 4
)

// We cache partial data of each validator instead of storing all checkpoint states
type partialValidator struct {
	index            uint32
	effectiveBalance uint64
}

type ForkChoiceStore struct {
	time                          uint64
	highestSeen                   uint64
	justifiedCheckpoint           *cltypes.Checkpoint
	finalizedCheckpoint           *cltypes.Checkpoint
	unrealizedJustifiedCheckpoint *cltypes.Checkpoint
	unrealizedFinalizedCheckpoint *cltypes.Checkpoint
	proposerBoostRoot             libcommon.Hash
	// Use go map because this is actually an unordered set
	equivocatingIndicies map[uint64]struct{}
	forkGraph            *fork_graph.ForkGraph
	// I use the cache due to the convenient auto-cleanup feauture.
	checkpointStates *lru.Cache[cltypes.Checkpoint, *state.BeaconState] // We keep ssz snappy of it as the full beacon state is full of rendundant data.
	latestMessages   map[uint64]*LatestMessage
	// We keep track of them so that we can forkchoice with EL.
	eth2Roots *lru.Cache[libcommon.Hash, libcommon.Hash] // ETH2 root -> ETH1 hash
	mu        sync.Mutex
	// EL
	engine execution_client.ExecutionEngine
}

type LatestMessage struct {
	Epoch uint64
	Root  libcommon.Hash
}

// NewForkChoiceStore initialize a new store from the given anchor state, either genesis or checkpoint sync state.
func NewForkChoiceStore(anchorState *state.BeaconState, engine execution_client.ExecutionEngine, enabledPruning bool) (*ForkChoiceStore, error) {
	anchorRoot, err := anchorState.BlockRoot()
	if err != nil {
		return nil, err
	}
	anchorCheckpoint := &cltypes.Checkpoint{
		Epoch: anchorState.Epoch(),
		Root:  anchorRoot,
	}
	checkpointStates, err := lru.New[cltypes.Checkpoint, *state.BeaconState](allowedCachedStates)
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
	}, nil
}

// Highest seen returns highest seen slot
func (f *ForkChoiceStore) HighestSeen() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
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
func (f *ForkChoiceStore) JustifiedCheckpoint() *cltypes.Checkpoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.justifiedCheckpoint
}

// FinalizedCheckpoint returns justified checkpoint
func (f *ForkChoiceStore) FinalizedCheckpoint() *cltypes.Checkpoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.finalizedCheckpoint
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
