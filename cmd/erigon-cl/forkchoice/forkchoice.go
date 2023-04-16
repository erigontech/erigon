package forkchoice

import (
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
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
	justifiedCheckpoint           *cltypes.Checkpoint
	finalizedCheckpoint           *cltypes.Checkpoint
	unrealizedJustifiedCheckpoint *cltypes.Checkpoint
	unrealizedFinalizedCheckpoint *cltypes.Checkpoint
	proposerBoostRoot             libcommon.Hash
	// Use go map because this is actually an unordered set
	equivocatingIndicies map[uint64]struct{}
	forkGraph            *fork_graph.ForkGraph
	// I use the cache due to the convenient auto-cleanup feauture.
	unrealizedJustifications *lru.Cache[libcommon.Hash, *cltypes.Checkpoint]
	checkpointStates         *lru.Cache[cltypes.Checkpoint, *state.BeaconState] // We keep ssz snappy of it as the full beacon state is full of rendundant data.
	latestMessages           map[uint64]*LatestMessage
	mu                       sync.Mutex
}

type LatestMessage struct {
	Epoch uint64
	Root  libcommon.Hash
}

// NewForkChoiceStore initialize a new store from the given anchor state, either genesis or checkpoint sync state.
func NewForkChoiceStore(anchorState *state.BeaconState) (*ForkChoiceStore, error) {
	anchorRoot, err := anchorState.BlockRoot()
	if err != nil {
		return nil, err
	}
	anchorCheckpoint := &cltypes.Checkpoint{
		Epoch: anchorState.Epoch(),
		Root:  anchorRoot,
	}
	unrealizedJustifications, err := lru.New[libcommon.Hash, *cltypes.Checkpoint](checkpointsPerCache)
	if err != nil {
		return nil, err
	}
	checkpointStates, err := lru.New[cltypes.Checkpoint, *state.BeaconState](allowedCachedStates)
	if err != nil {
		return nil, err
	}

	return &ForkChoiceStore{
		time:                          anchorState.GenesisTime() + anchorState.BeaconConfig().SecondsPerSlot*anchorState.Slot(),
		justifiedCheckpoint:           anchorCheckpoint.Copy(),
		finalizedCheckpoint:           anchorCheckpoint.Copy(),
		unrealizedJustifiedCheckpoint: anchorCheckpoint.Copy(),
		unrealizedFinalizedCheckpoint: anchorCheckpoint.Copy(),
		forkGraph:                     fork_graph.New(anchorState),
		unrealizedJustifications:      unrealizedJustifications,
		equivocatingIndicies:          map[uint64]struct{}{},
		latestMessages:                map[uint64]*LatestMessage{},
		checkpointStates:              checkpointStates,
	}, nil
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
