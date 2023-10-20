package fork_graph

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

/*
* The state store process is related to graph theory in the sense that the Ethereum blockchain can be thought of as a directed graph,
* where each block represents a node and the links between blocks represent directed edges.
* In this context, rolling back the state of Ethereum to a previous state can be thought of as traversing the graph in reverse,
* from the current state to a previous state.
* The process of reverting the state involves undoing the changes made in the blocks that have been added to the blockchain since the previous state.
* This can be thought of as "reversing the edges" in the graph, effectively undoing the changes made to the state of Ethereum.
* By thinking of the Ethereum blockchain as a graph, we can use graph theory concepts, such as traversal algorithms,
* to analyze and manipulate the state of the blockchain.
 */
type ForkGraph interface {
	AddChainSegment(signedBlock *cltypes.SignedBeaconBlock, fullValidation bool) (*state.CachingBeaconState, ChainSegmentInsertionResult, error)
	GetHeader(blockRoot libcommon.Hash) (*cltypes.BeaconBlockHeader, bool)
	GetState(blockRoot libcommon.Hash, alwaysCopy bool) (*state.CachingBeaconState, error)
	GetCurrentJustifiedCheckpoint(blockRoot libcommon.Hash) (solid.Checkpoint, bool)
	GetFinalizedCheckpoint(blockRoot libcommon.Hash) (solid.Checkpoint, bool)
	MarkHeaderAsInvalid(blockRoot libcommon.Hash)
	AnchorSlot() uint64
	Prune(uint64) error
}
