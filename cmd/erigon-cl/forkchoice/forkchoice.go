package forkchoice

import (
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/forkchoice/fork_graph"
)

const checkpointsPerCache = 1024

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
	}, nil
}
