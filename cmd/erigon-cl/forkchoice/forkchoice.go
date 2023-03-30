package forkchoice

import (
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/forkchoice/fork_graph"
)

type ForkChoiceStore struct {
	time                          uint64
	justifiedCheckpoint           *cltypes.Checkpoint
	finalizedCheckpoint           *cltypes.Checkpoint
	unrealizedJustifiedCheckpoint *cltypes.Checkpoint
	unrealizedFinalizedCheckpoint *cltypes.Checkpoint
	proposerBoostRoot             libcommon.Hash
	equivocatingIndicies          []uint64
	forkGraph                     *fork_graph.ForkGraph
	// I use the cache due to the convenient auto-cleanup feauture.
	unrealizedJustifications lru.Cache[libcommon.Hash, *cltypes.Checkpoint]

	mu sync.Mutex
}
