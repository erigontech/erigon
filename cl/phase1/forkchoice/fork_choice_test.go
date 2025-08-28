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

	"github.com/erigontech/erigon-lib/common"
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
	"github.com/erigontech/erigon/db/kv"
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
	blobStorage := blob_storage.NewBlobStore(memdb.NewTestDB(t, kv.ChainDB), afero.NewMemMapFs(), math.MaxUint64, &clparams.MainnetBeaconConfig, ethClock)
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
		t.Skip("too slow for testing.Short")
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
	blobStorage := blob_storage.NewBlobStore(memdb.NewTestDB(t, kv.ChainDB), afero.NewMemMapFs(), math.MaxUint64, &clparams.MainnetBeaconConfig, ethClock)
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
