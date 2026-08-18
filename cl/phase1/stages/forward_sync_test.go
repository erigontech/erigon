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

package stages

import (
	"errors"
	"math"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/devgenesis"
	"github.com/erigontech/erigon/cl/cltypes"
	dasmock "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	clutils "github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func TestForwardSyncValidatesBlockBeforeAcquiringColumns(t *testing.T) {
	stageCfg, block, _ := newSignedFuluForwardSyncFixture(t)
	block.Signature = common.Bytes96{}

	_, err := processDownloadedBlockBatches(t.Context(), log.Root(), stageCfg, 0, false, []*cltypes.SignedBeaconBlock{block}, nil)
	require.ErrorContains(t, err, "signature")
}

func TestForwardSyncValidatesForkSchemaBeforeAcquiringColumns(t *testing.T) {
	stageCfg, validBlock, _ := newSignedFuluForwardSyncFixture(t)
	block := cltypes.NewSignedBeaconBlock(stageCfg.beaconCfg, clparams.GloasVersion)
	block.Block.Slot = validBlock.Block.Slot
	block.Block.ParentRoot = validBlock.Block.ParentRoot
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	require.True(t, requiresRecentBlockDataAvailability(stageCfg, block))
	require.NotZero(t, block.GetBlobKzgCommitments().Len())

	_, err := processDownloadedBlockBatches(t.Context(), log.Root(), stageCfg, 0, false, []*cltypes.SignedBeaconBlock{block}, nil)
	require.ErrorIs(t, err, forkchoice.ErrForkSchemaSlotMismatch)
}

func TestForwardSyncDoesNotBlameBlockPeerForColumnAcquisitionError(t *testing.T) {
	stageCfg, block, blockRoot := newSignedFuluForwardSyncFixture(t)
	localErr := errors.New("column storage temporarily unavailable")
	stageCfg.peerDas.(*dasmock.MockPeerDas).EXPECT().
		IsDataAvailable(block.Block.Slot, blockRoot).
		Return(false, localErr)

	progress, err := processDownloadedBlockBatches(t.Context(), log.Root(), stageCfg, 0, false, []*cltypes.SignedBeaconBlock{block}, nil)
	require.NoError(t, err)
	require.Zero(t, progress)
}

func TestForwardSyncSkipsKnownOverlapBeforeAcquiringColumns(t *testing.T) {
	stageCfg, block := newKnownFuluForwardSyncFixture(t)

	progress, err := processDownloadedBlockBatches(t.Context(), log.Root(), stageCfg, 2, false, []*cltypes.SignedBeaconBlock{block}, nil)
	require.NoError(t, err)
	require.Equal(t, block.Block.Slot, progress)
}

func newSignedFuluForwardSyncFixture(t *testing.T) (*Cfg, *cltypes.SignedBeaconBlock, common.Hash) {
	t.Helper()
	cfg := testFuluConfig()
	anchorState, keys, err := devgenesis.BuildGenesisState("forward-sync-test", 1, &cfg, 0, common.Hash{})
	require.NoError(t, err)
	stageCfg, anchorRoot := newForwardSyncTestCfg(t, &cfg, anchorState)

	block := newFuluChainTipTestBlock(&cfg, 1, anchorRoot)
	block.Block.ProposerIndex = 0
	domain, err := fork.ComputeDomain(
		cfg.DomainBeaconProposer[:],
		clutils.Uint32ToBytes4(cfg.GetForkVersionByVersion(clparams.FuluVersion)),
		anchorState.GenesisValidatorsRoot(),
	)
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(block.Block, domain)
	require.NoError(t, err)
	copy(block.Signature[:], keys[0].Sign(signingRoot[:]).Bytes())
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	stageCfg.forkChoice.OnTick(block.Block.Slot * cfg.SecondsPerSlot)

	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(block.Block.Slot).AnyTimes()
	peerDas := dasmock.NewMockPeerDas(ctrl)
	stageCfg.ethClock = clock
	stageCfg.peerDas = peerDas
	stageCfg.forkChoice.InitPeerDas(peerDas)
	return stageCfg, block, common.Hash(blockRoot)
}

func newKnownFuluForwardSyncFixture(t *testing.T) (*Cfg, *cltypes.SignedBeaconBlock) {
	t.Helper()
	cfg := testFuluConfig()
	anchorState, _, err := devgenesis.BuildGenesisState("known-forward-sync-test", 1, &cfg, 0, common.Hash{})
	require.NoError(t, err)
	block := newFuluChainTipTestBlock(&cfg, 4, common.Hash{})
	block.Block.ProposerIndex = 0
	anchorState.SetSlot(block.Block.Slot)
	anchorState.SetLatestBlockHeader(block.SignedBeaconBlockHeader().Header)
	stateRoot, err := anchorState.HashSSZ()
	require.NoError(t, err)
	block.Block.StateRoot = stateRoot
	stageCfg, _ := newForwardSyncTestCfg(t, &cfg, anchorState)

	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(block.Block.Slot).AnyTimes()
	peerDas := dasmock.NewMockPeerDas(ctrl)
	stageCfg.ethClock = clock
	stageCfg.peerDas = peerDas
	stageCfg.forkChoice.InitPeerDas(peerDas)
	return stageCfg, block
}

func newForwardSyncTestCfg(t *testing.T, cfg *clparams.BeaconChainConfig, anchorState *state.CachingBeaconState) (*Cfg, common.Hash) {
	t.Helper()
	anchorRoot, err := anchorState.BlockRoot()
	require.NoError(t, err)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	clock := eth_clock.NewEthereumClock(0, anchorState.GenesisValidatorsRoot(), cfg)
	store, err := forkchoice.NewForkChoiceStore(
		clock,
		anchorState,
		nil,
		pool.NewOperationsPool(cfg),
		fork_graph.NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{}),
		beaconevents.NewEventEmitter(),
		synced_data.NewSyncedDataManager(cfg, true),
		blob_storage.NewBlobStore(db, afero.NewMemMapFs(), math.MaxUint64, cfg, clock),
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		validator_params.NewValidatorParams(),
		false,
		nil,
	)
	require.NoError(t, err)
	return &Cfg{beaconCfg: cfg, forkChoice: store, indiciesDB: db}, anchorRoot
}

func testFuluConfig() clparams.BeaconChainConfig {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()
	return cfg
}
