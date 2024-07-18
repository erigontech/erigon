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

package fork_graph

import (
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
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
	GetSyncCommittees(period uint64) (*solid.SyncCommittee, *solid.SyncCommittee, bool)
	MarkHeaderAsInvalid(blockRoot libcommon.Hash)
	AnchorSlot() uint64
	Prune(uint64) error
	GetBlockRewards(blockRoot libcommon.Hash) (*eth2.BlockRewardsCollector, bool)
	LowestAvaiableSlot() uint64
	GetLightClientBootstrap(blockRoot libcommon.Hash) (*cltypes.LightClientBootstrap, bool)
	NewestLightClientUpdate() *cltypes.LightClientUpdate
	GetLightClientUpdate(period uint64) (*cltypes.LightClientUpdate, bool)
	GetBalances(blockRoot libcommon.Hash) (solid.Uint64ListSSZ, error)
	GetInactivitiesScores(blockRoot libcommon.Hash) (solid.Uint64ListSSZ, error)
	GetPreviousPartecipationIndicies(blockRoot libcommon.Hash) (*solid.BitList, error)
	GetValidatorSet(blockRoot libcommon.Hash) (*solid.ValidatorSet, error)
	GetCurrentPartecipationIndicies(blockRoot libcommon.Hash) (*solid.BitList, error)
	DumpBeaconStateOnDisk(blockRoot libcommon.Hash, state *state.CachingBeaconState, forced bool) error
}
