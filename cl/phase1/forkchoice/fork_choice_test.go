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

package forkchoice_test

import (
	"context"
	_ "embed"
	"math"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

//go:embed test_data/anchor_state.ssz_snappy
var anchorStateEncoded []byte

//go:embed test_data/block_0x3af8b5b42ca135c75b32abb32b3d71badb73695d3dc638bacfb6c8b7bcbee1a9.ssz_snappy
var block3aEncoded []byte

//go:embed test_data/block_0xc2788d6005ee2b92c3df2eff0aeab0374d155fa8ca1f874df305fa376ce334cf.ssz_snappy
var blockc2Encoded []byte

//go:embed test_data/block_0xd4503d46e43df56de4e19acb0f93b3b52087e422aace49a7c3816cf59bafb0ad.ssz_snappy
var blockd4Encoded []byte

//go:embed test_data/attestation_0xfb924d35b2888d9cd70e6879c1609e6cad7ea3b028a501967747d96e49068cb6.ssz_snappy
var attestationEncoded []byte

// this is consensus spec test altair/forkchoice/ex_ante/ex_ante_attestations_is_greater_than_proposer_boost_with_boost
func TestForkChoiceBasic(t *testing.T) {
	ctx := context.Background()
	expectedCheckpoint := &solid.Checkpoint{
		Root:  common.HexToHash("0x564d76d91f66c1fb2977484a6184efda2e1c26dd01992e048353230e10f83201"),
		Epoch: 0,
	}
	sd := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	// Decode test blocks
	block0x3a, block0xc2, block0xd4 := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion),
		cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion),
		cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	require.NoError(t, utils.DecodeSSZSnappy(block0x3a, block3aEncoded, int(clparams.AltairVersion)))
	require.NoError(t, utils.DecodeSSZSnappy(block0xc2, blockc2Encoded, int(clparams.AltairVersion)))
	require.NoError(t, utils.DecodeSSZSnappy(block0xd4, blockd4Encoded, int(clparams.AltairVersion)))
	// decode test attestation
	testAttestation := &solid.Attestation{}
	require.NoError(t, utils.DecodeSSZSnappy(testAttestation, attestationEncoded, int(clparams.AltairVersion)))
	// Initialize forkchoice store
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchorStateEncoded, int(clparams.AltairVersion)))
	pool := pool.NewOperationsPool(&clparams.MainnetBeaconConfig)
	emitters := beaconevents.NewEventEmitter()

	// Create required components
	genesisState, err := initial_state.GetGenesisState(1) // Mainnet
	require.NoError(t, err)
	ethClock := eth_clock.NewEthereumClock(genesisState.GenesisTime(), genesisState.GenesisValidatorsRoot(), &clparams.MainnetBeaconConfig)
	blobStorage := blob_storage.NewBlobStore(memdb.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs(), math.MaxUint64, &clparams.MainnetBeaconConfig, ethClock)
	localValidators := validator_params.NewValidatorParams()

	store, err := forkchoice.NewForkChoiceStore(
		ethClock,
		anchorState,
		nil, // execution engine
		pool,
		fork_graph.NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{}, emitters),
		emitters,
		sd,
		blobStorage,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		localValidators,
		false, // probabilisticHeadGetter
		nil,   // db: no KV persistence in tests
	)
	require.NoError(t, err)
	// first steps
	store.OnTick(0)
	store.OnTick(12)
	require.NoError(t, store.OnBlock(ctx, block0x3a, false, true, false))
	// Check if we get correct status (1)
	require.Equal(t, uint64(12), store.Time())
	require.Equal(t, store.ProposerBoostRoot(), common.HexToHash("0xc9bd7bcb6dfa49dc4e5a67ca75e89062c36b5c300bc25a1b31db4e1a89306071"))
	require.Equal(t, *expectedCheckpoint, store.JustifiedCheckpoint())
	require.Equal(t, *expectedCheckpoint, store.FinalizedCheckpoint())
	headRoot, headSlot, err := store.GetHead(nil)
	require.NoError(t, err)
	require.Equal(t, headRoot, common.HexToHash("0xc9bd7bcb6dfa49dc4e5a67ca75e89062c36b5c300bc25a1b31db4e1a89306071"))
	require.Equal(t, uint64(1), headSlot)
	// process another tick and another block
	store.OnTick(36)
	require.NoError(t, store.OnBlock(ctx, block0xc2, false, true, false))
	// Check if we get correct status (2)
	require.Equal(t, uint64(36), store.Time())
	require.Equal(t, store.ProposerBoostRoot(), common.HexToHash("0x744cc484f6503462f0f3a5981d956bf4fcb3e57ab8687ed006467e05049ee033"))
	require.Equal(t, *expectedCheckpoint, store.JustifiedCheckpoint())
	require.Equal(t, *expectedCheckpoint, store.FinalizedCheckpoint())
	headRoot, headSlot, err = store.GetHead(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3), headSlot)
	require.Equal(t, headRoot, common.HexToHash("0x744cc484f6503462f0f3a5981d956bf4fcb3e57ab8687ed006467e05049ee033"))
	// last block
	require.NoError(t, store.OnBlock(ctx, block0xd4, false, true, false))
	require.Equal(t, uint64(36), store.Time())
	require.Equal(t, store.ProposerBoostRoot(), common.HexToHash("0x744cc484f6503462f0f3a5981d956bf4fcb3e57ab8687ed006467e05049ee033"))
	require.Equal(t, *expectedCheckpoint, store.JustifiedCheckpoint())
	require.Equal(t, *expectedCheckpoint, store.FinalizedCheckpoint())
	headRoot, headSlot, err = store.GetHead(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3), headSlot)
	require.Equal(t, headRoot, common.HexToHash("0x744cc484f6503462f0f3a5981d956bf4fcb3e57ab8687ed006467e05049ee033"))
	// lastly do attestation
	require.NoError(t, store.OnAttestation(testAttestation, false, false))
	bs, err := store.GetStateAtBlockRoot(headRoot, true)
	require.NoError(t, err)
	sd.OnHeadState(bs)

	require.NoError(t, err)
}

func TestForkChoiceChainBellatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	ctx := context.Background()
	blocks, anchorState, _ := tests.GetBellatrixRandom()
	cfg := clparams.MainnetBeaconConfig

	intermediaryState, err := anchorState.Copy()
	require.NoError(t, err)

	intermediaryBlockRoot := blocks[0].Block.ParentRoot
	for i := 0; i < 35; i++ {
		require.NoError(t, transition.TransitionState(intermediaryState, blocks[i], nil, false))
		intermediaryBlockRoot, err = blocks[i].Block.HashSSZ()
		require.NoError(t, err)
	}
	// Initialize forkchoice store
	pool := pool.NewOperationsPool(&clparams.MainnetBeaconConfig)
	emitters := beaconevents.NewEventEmitter()
	sd := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)

	// Create required components
	genesisState, err := initial_state.GetGenesisState(1) // Mainnet
	require.NoError(t, err)
	ethClock := eth_clock.NewEthereumClock(genesisState.GenesisTime(), genesisState.GenesisValidatorsRoot(), &clparams.MainnetBeaconConfig)
	blobStorage := blob_storage.NewBlobStore(memdb.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs(), math.MaxUint64, &clparams.MainnetBeaconConfig, ethClock)
	localValidators := validator_params.NewValidatorParams()

	store, err := forkchoice.NewForkChoiceStore(
		ethClock,
		anchorState,
		nil, // execution engine
		pool,
		fork_graph.NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{
			Beacon: true,
		}, emitters),
		emitters,
		sd,
		blobStorage,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		localValidators,
		false, // probabilisticHeadGetter
		nil,   // db: no KV persistence in tests
	)
	store.OnTick(2000)
	require.NoError(t, err)
	for _, block := range blocks {
		require.NoError(t, store.OnBlock(ctx, block, false, true, false))
	}
	root1, err := blocks[20].Block.HashSSZ()
	require.NoError(t, err)

	rewards, ok := store.BlockRewards(common.Hash(root1))
	require.True(t, ok)
	require.Equal(t, uint64(0x511ad), rewards.Attestations)
	// test randao mix
	mixes := solid.NewHashVector(int(clparams.MainnetBeaconConfig.EpochsPerHistoricalVector))
	require.True(t, store.RandaoMixes(intermediaryBlockRoot, mixes))
	for i := 0; i < mixes.Length(); i++ {
		require.Equal(t, mixes.Get(i), intermediaryState.RandaoMixes().Get(i), "mixes mismatch at index %d, have: %x, expected: %x", i, mixes.Get(i), intermediaryState.RandaoMixes().Get(i))
	}
	currentIntermediarySyncCommittee, nextIntermediarySyncCommittee, ok := store.GetSyncCommittees(cfg.SyncCommitteePeriod(store.HighestSeen()))
	require.True(t, ok)

	require.Equal(t, intermediaryState.CurrentSyncCommittee(), currentIntermediarySyncCommittee)
	require.Equal(t, intermediaryState.NextSyncCommittee(), nextIntermediarySyncCommittee)

	bs, has := store.GetLightClientBootstrap(intermediaryBlockRoot)
	require.True(t, has)
	bsRoot, err := bs.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(bsRoot), common.HexToHash("0x58a3f366bcefe6c30fb3a6506bed726f9a51bb272c77a8a3ed88c34435d44cb7"))
}

// TestGetHeadAfterProcessingBlocks verifies that GetHead succeeds after
// processing blocks through OnBlock. This exercises the full path:
// GetHead → getCheckpointState → getState (backward walk + state file read).
// This is the exact path that fails in production with "baseState not found".
func TestGetHeadAfterProcessingBlocks(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	ctx := context.Background()
	blocks, anchorState, _ := tests.GetBellatrixRandom()

	pool := pool.NewOperationsPool(&clparams.MainnetBeaconConfig)
	emitters := beaconevents.NewEventEmitter()
	sd := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)

	genesisState, err := initial_state.GetGenesisState(1)
	require.NoError(t, err)
	ethClock := eth_clock.NewEthereumClock(genesisState.GenesisTime(), genesisState.GenesisValidatorsRoot(), &clparams.MainnetBeaconConfig)
	blobStorage := blob_storage.NewBlobStore(memdb.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs(), math.MaxUint64, &clparams.MainnetBeaconConfig, ethClock)
	localValidators := validator_params.NewValidatorParams()

	store, err := forkchoice.NewForkChoiceStore(
		ethClock,
		anchorState,
		nil,
		pool,
		fork_graph.NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{
			Beacon: true,
		}, emitters),
		emitters,
		sd,
		blobStorage,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		localValidators,
		false,
		nil,
	)
	require.NoError(t, err)

	// Advance store time so blocks are not in the future.
	store.OnTick(2000)

	// Process all 96 blocks (3 epochs).
	for i, block := range blocks {
		err := store.OnBlock(ctx, block, false, true, false)
		require.NoError(t, err, "OnBlock failed at block %d (slot %d)", i, block.Block.Slot)
	}

	// Log the justified/finalized state for debugging.
	justified := store.JustifiedCheckpoint()
	finalized := store.FinalizedCheckpoint()
	t.Logf("After processing %d blocks:", len(blocks))
	t.Logf("  Justified: epoch=%d root=%x", justified.Epoch, justified.Root)
	t.Logf("  Finalized: epoch=%d root=%x", finalized.Epoch, finalized.Root)
	t.Logf("  HighestSeen: %d", store.HighestSeen())

	// Verify the justified root is accessible via GetStateAtBlockRoot.
	justifiedState, err := store.GetStateAtBlockRoot(justified.Root, true)
	require.NoError(t, err, "GetStateAtBlockRoot failed for justified root %x", justified.Root)
	require.NotNil(t, justifiedState, "GetStateAtBlockRoot returned nil for justified root %x", justified.Root)
	t.Logf("  Justified state slot: %d", justifiedState.Slot())

	// Now call GetHead — this is the exact call that fails in production.
	headRoot, headSlot, err := store.GetHead(nil)
	require.NoError(t, err, "GetHead failed: %v", err)
	t.Logf("  Head: slot=%d root=%x", headSlot, headRoot)

	// The head should be at or near the last processed block.
	lastBlock := blocks[len(blocks)-1]
	lastRoot, err := lastBlock.Block.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(lastRoot), headRoot, "head should be the last block")
}

// TestGetStateAfterPrune verifies that getState still works after Prune
// deletes old blocks and state files. This simulates the production scenario
// where onNewFinalized calls Prune, and then getCheckpointState needs to
// recover the justified checkpoint state from disk.
func TestGetStateAfterPrune(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	ctx := context.Background()
	blocks, anchorState, _ := tests.GetBellatrixRandom()

	pool := pool.NewOperationsPool(&clparams.MainnetBeaconConfig)
	emitters := beaconevents.NewEventEmitter()
	sd := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)

	genesisState, err := initial_state.GetGenesisState(1)
	require.NoError(t, err)
	ethClock := eth_clock.NewEthereumClock(genesisState.GenesisTime(), genesisState.GenesisValidatorsRoot(), &clparams.MainnetBeaconConfig)
	blobStorage := blob_storage.NewBlobStore(memdb.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs(), math.MaxUint64, &clparams.MainnetBeaconConfig, ethClock)
	localValidators := validator_params.NewValidatorParams()

	fg := fork_graph.NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{
		Beacon: true,
	}, emitters)

	store, err := forkchoice.NewForkChoiceStore(
		ethClock,
		anchorState,
		nil,
		pool,
		fg,
		emitters,
		sd,
		blobStorage,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		localValidators,
		false,
		nil,
	)
	require.NoError(t, err)
	store.OnTick(2000)

	// Process all 96 blocks.
	for i, block := range blocks {
		err := store.OnBlock(ctx, block, false, true, false)
		require.NoError(t, err, "OnBlock failed at block %d (slot %d)", i, block.Block.Slot)
	}

	justified := store.JustifiedCheckpoint()
	finalized := store.FinalizedCheckpoint()
	t.Logf("Justified: epoch=%d root=%x", justified.Epoch, justified.Root)
	t.Logf("Finalized: epoch=%d root=%x", finalized.Epoch, finalized.Root)

	// Verify GetState works before prune.
	preState, err := store.GetStateAtBlockRoot(justified.Root, true)
	require.NoError(t, err)
	require.NotNil(t, preState, "pre-prune: state should exist for justified root")
	t.Logf("Pre-prune justified state slot: %d", preState.Slot())

	// Prune blocks below a slot that's below the justified root but high enough
	// to delete some early blocks. The justified root is at epoch 3 (slot 96).
	// Prune at slot 80 to delete old blocks while keeping justified root intact.
	pruneSlot := uint64(80)
	t.Logf("Pruning at slot %d", pruneSlot)
	require.NoError(t, fg.Prune(pruneSlot))

	// After prune, the justified root (slot 96) should still be in the fork graph.
	_, hasHeader := fg.GetHeader(justified.Root)
	require.True(t, hasHeader, "justified root header should survive prune (slot 96 > prune slot 80)")

	// GetState should still work for the justified root.
	postState, err := store.GetStateAtBlockRoot(justified.Root, true)
	require.NoError(t, err)
	require.NotNil(t, postState, "post-prune: state should still be retrievable for justified root")
	t.Logf("Post-prune justified state slot: %d", postState.Slot())

	// GetHead should still work.
	headRoot, headSlot, err := store.GetHead(nil)
	require.NoError(t, err, "GetHead failed after prune")
	t.Logf("Post-prune head: slot=%d root=%x", headSlot, headRoot)
}

// TestGetStateWalkAfterAggressivePrune verifies that getState's backward walk
// works correctly when pruning removes most blocks, forcing the walk to traverse
// through blocks close to the justified root. This tests:
// 1. State files exist at slot % 4 == 0 boundaries (from AddChainSegment dump)
// 2. The backward walk correctly finds and reads these state files
// 3. GetHead (→ getCheckpointState → getState) still works after Prune
func TestGetStateWalkAfterAggressivePrune(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	ctx := context.Background()
	blocks, anchorState, _ := tests.GetBellatrixRandom()

	pool := pool.NewOperationsPool(&clparams.MainnetBeaconConfig)
	emitters := beaconevents.NewEventEmitter()
	sd := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)

	genesisState, err := initial_state.GetGenesisState(1)
	require.NoError(t, err)
	ethClock := eth_clock.NewEthereumClock(genesisState.GenesisTime(), genesisState.GenesisValidatorsRoot(), &clparams.MainnetBeaconConfig)
	blobStorage := blob_storage.NewBlobStore(memdb.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs(), math.MaxUint64, &clparams.MainnetBeaconConfig, ethClock)
	localValidators := validator_params.NewValidatorParams()

	fg := fork_graph.NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{
		Beacon: true,
	}, emitters)

	store, err := forkchoice.NewForkChoiceStore(
		ethClock,
		anchorState,
		nil,
		pool,
		fg,
		emitters,
		sd,
		blobStorage,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		localValidators,
		false,
		nil,
	)
	require.NoError(t, err)
	store.OnTick(2000)

	// Process all 96 blocks.
	for i, block := range blocks {
		err := store.OnBlock(ctx, block, false, true, false)
		require.NoError(t, err, "OnBlock failed at block %d (slot %d)", i, block.Block.Slot)
	}

	justified := store.JustifiedCheckpoint()
	t.Logf("Justified: epoch=%d root=%x", justified.Epoch, justified.Root)

	justifiedHeader, hasJH := fg.GetHeader(justified.Root)
	require.True(t, hasJH, "justified root must be in headers")
	t.Logf("Justified block slot: %d, slot%%4=%d", justifiedHeader.Slot, justifiedHeader.Slot%4)

	// Prune aggressively — up to 2 slots below the justified root.
	// This simulates a production scenario where the prune boundary is close to
	// but still below the justified checkpoint. The justified root (and its nearby
	// ancestors with state files) must survive.
	pruneSlot := justifiedHeader.Slot - 2
	t.Logf("Pruning at slot %d (justified at slot %d)", pruneSlot, justifiedHeader.Slot)
	require.NoError(t, fg.Prune(pruneSlot))

	// Verify GetState still works for the justified root.
	state, err := store.GetStateAtBlockRoot(justified.Root, true)
	require.NoError(t, err, "GetStateAtBlockRoot failed for justified root after aggressive prune")
	require.NotNil(t, state, "state nil for justified root (slot %d) after prune at slot %d",
		justifiedHeader.Slot, pruneSlot)
	t.Logf("Post-prune justified state slot: %d", state.Slot())

	// GetHead should succeed — this is the exact path that fails in production.
	headRoot, headSlot, err := store.GetHead(nil)
	require.NoError(t, err, "GetHead failed after aggressive prune")
	t.Logf("Post-prune head: slot=%d root=%x", headSlot, headRoot)
}
