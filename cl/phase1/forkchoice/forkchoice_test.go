// Copyright 2026 The Erigon Authors
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
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/common"
)

func TestGetFinalizedExecutionHash(t *testing.T) {
	cache, err := lru.New[common.Hash, common.Hash](16)
	require.NoError(t, err)

	gloasBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	gloasBlock.Block.Slot = 1
	gloasRoot := common.Hash{0x01}
	gloasParentBlockHash := common.Hash{0xaa}
	gloasFallbackHash := common.Hash{0xbb}
	gloasBlock.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash = gloasParentBlockHash
	cache.Add(gloasRoot, gloasFallbackHash)

	preGloasBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	preGloasBlock.Block.Slot = 2
	preGloasRoot := common.Hash{0x02}
	preGloasExecutionHash := common.Hash{0xcc}
	cache.Add(preGloasRoot, preGloasExecutionHash)

	missingRoot := common.Hash{0x03}
	missingExecutionHash := common.Hash{0xdd}
	cache.Add(missingRoot, missingExecutionHash)

	store := &ForkChoiceStore{
		forkGraph: &getFinalizedExecutionHashForkGraph{
			blocks: map[common.Hash]*cltypes.SignedBeaconBlock{
				gloasRoot:    gloasBlock,
				preGloasRoot: preGloasBlock,
			},
		},
		eth2Roots: cache,
	}

	require.Equal(t, gloasParentBlockHash, store.GetFinalizedExecutionHash(gloasRoot))
	require.Equal(t, preGloasExecutionHash, store.GetFinalizedExecutionHash(preGloasRoot))
	require.Equal(t, missingExecutionHash, store.GetFinalizedExecutionHash(missingRoot))
}

type getFinalizedExecutionHashForkGraph struct {
	blocks map[common.Hash]*cltypes.SignedBeaconBlock
}

func (g *getFinalizedExecutionHashForkGraph) AddChainSegment(*cltypes.SignedBeaconBlock, bool) (*state.CachingBeaconState, fork_graph.ChainSegmentInsertionResult, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetHeader(common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetBlock(blockRoot common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	block := g.blocks[blockRoot]
	return block, block != nil
}

func (g *getFinalizedExecutionHashForkGraph) GetState(common.Hash, bool) (*state.CachingBeaconState, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetCurrentJustifiedCheckpoint(common.Hash) (solid.Checkpoint, bool) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetFinalizedCheckpoint(common.Hash) (solid.Checkpoint, bool) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetSyncCommittees(uint64) (*solid.SyncCommittee, *solid.SyncCommittee, bool) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) MarkHeaderAsInvalid(common.Hash) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) AnchorSlot() uint64 {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) AnchorRoot() common.Hash {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) Prune(uint64) error {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetBlockRewards(common.Hash) (*eth2.BlockRewardsCollector, bool) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) LowestAvailableSlot() uint64 {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetLightClientBootstrap(common.Hash) (*cltypes.LightClientBootstrap, bool) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) NewestLightClientUpdate() *cltypes.LightClientUpdate {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetLightClientUpdate(uint64) (*cltypes.LightClientUpdate, bool) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetBalances(common.Hash) (solid.Uint64ListSSZ, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetInactivitiesScores(common.Hash) (solid.Uint64ListSSZ, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetValidatorSet(common.Hash) (*solid.ValidatorSet, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetCurrentParticipationIndicies(uint64) (*solid.ParticipationBitList, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) GetPreviousParticipationIndicies(uint64) (*solid.ParticipationBitList, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) DumpBeaconStateOnDisk(common.Hash, *state.CachingBeaconState, bool) error {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) DumpEnvelopeOnDisk(common.Hash, *cltypes.SignedExecutionPayloadEnvelope) error {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	panic("not used")
}

func (g *getFinalizedExecutionHashForkGraph) HasEnvelope(common.Hash) bool {
	panic("not used")
}
