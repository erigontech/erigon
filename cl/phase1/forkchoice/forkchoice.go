package forkchoice

import (
	"context"
	"sync"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/freezer"
	state2 "github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/ledgerwatch/erigon/cl/pool"
	"golang.org/x/exp/slices"

	lru "github.com/hashicorp/golang-lru/v2"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
)

type checkpointComparable string

const (
	checkpointsPerCache = 1024
	allowedCachedStates = 8
)

type ForkChoiceStore struct {
	ctx                           context.Context
	time                          uint64
	highestSeen                   uint64
	justifiedCheckpoint           solid.Checkpoint
	finalizedCheckpoint           solid.Checkpoint
	unrealizedJustifiedCheckpoint solid.Checkpoint
	unrealizedFinalizedCheckpoint solid.Checkpoint
	proposerBoostRoot             libcommon.Hash
	headHash                      libcommon.Hash
	headSlot                      uint64
	genesisTime                   uint64
	childrens                     map[libcommon.Hash]childrens

	// Use go map because this is actually an unordered set
	equivocatingIndicies map[uint64]struct{}
	forkGraph            fork_graph.ForkGraph
	// I use the cache due to the convenient auto-cleanup feauture.
	checkpointStates map[checkpointComparable]*checkpointState // We keep ssz snappy of it as the full beacon state is full of rendundant data.
	latestMessages   map[uint64]*LatestMessage
	anchorPublicKeys []byte
	// We keep track of them so that we can forkchoice with EL.
	eth2Roots *lru.Cache[libcommon.Hash, libcommon.Hash] // ETH2 root -> ETH1 hash
	mu        sync.Mutex
	// EL
	engine execution_client.ExecutionEngine
	// freezer
	recorder freezer.Freezer
	// operations pool
	operationsPool pool.OperationsPool
	beaconCfg      *clparams.BeaconChainConfig
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
func NewForkChoiceStore(ctx context.Context, anchorState *state2.CachingBeaconState, engine execution_client.ExecutionEngine, recorder freezer.Freezer, operationsPool pool.OperationsPool, forkGraph fork_graph.ForkGraph) (*ForkChoiceStore, error) {
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
	anchorPublicKeys := make([]byte, anchorState.ValidatorLength()*length.Bytes48)
	for idx := 0; idx < anchorState.ValidatorLength(); idx++ {
		pk, err := anchorState.ValidatorPublicKey(idx)
		if err != nil {
			return nil, err
		}
		copy(anchorPublicKeys[idx*length.Bytes48:], pk[:])
	}

	return &ForkChoiceStore{
		ctx:                           ctx,
		highestSeen:                   anchorState.Slot(),
		time:                          anchorState.GenesisTime() + anchorState.BeaconConfig().SecondsPerSlot*anchorState.Slot(),
		justifiedCheckpoint:           anchorCheckpoint.Copy(),
		finalizedCheckpoint:           anchorCheckpoint.Copy(),
		unrealizedJustifiedCheckpoint: anchorCheckpoint.Copy(),
		unrealizedFinalizedCheckpoint: anchorCheckpoint.Copy(),
		forkGraph:                     forkGraph,
		equivocatingIndicies:          map[uint64]struct{}{},
		latestMessages:                map[uint64]*LatestMessage{},
		checkpointStates:              make(map[checkpointComparable]*checkpointState),
		eth2Roots:                     eth2Roots,
		engine:                        engine,
		recorder:                      recorder,
		operationsPool:                operationsPool,
		anchorPublicKeys:              anchorPublicKeys,
		genesisTime:                   anchorState.GenesisTime(),
		beaconCfg:                     anchorState.BeaconConfig(),
		childrens:                     make(map[libcommon.Hash]childrens),
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
