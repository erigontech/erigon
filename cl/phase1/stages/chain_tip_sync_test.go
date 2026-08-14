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
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	dasmock "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func TestChainTipSyncChecksFuluDataAvailability(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.InitializeForkSchedule()

	anchorState := state.New(&cfg)
	anchorState.SetVersion(clparams.FuluVersion)
	anchorRoot, err := anchorState.BlockRoot()
	require.NoError(t, err)

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	store, err := forkchoice.NewForkChoiceStore(
		clock,
		anchorState,
		nil,
		pool.NewOperationsPool(&cfg),
		fork_graph.NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{}),
		beaconevents.NewEventEmitter(),
		synced_data.NewSyncedDataManager(&cfg, true),
		blob_storage.NewBlobStore(db, afero.NewMemMapFs(), math.MaxUint64, &cfg, clock),
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		validator_params.NewValidatorParams(),
		false,
		nil,
	)
	require.NoError(t, err)

	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = 1
	block.Block.ParentRoot = anchorRoot
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	store.OnTick(block.Block.Slot * cfg.SecondsPerSlot)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	errCheckReached := errors.New("data availability check reached")
	errCh := make(chan error, 1)
	ctrl := gomock.NewController(t)
	peerDas := dasmock.NewMockPeerDas(ctrl)
	peerDas.EXPECT().
		IsDataAvailable(block.Block.Slot, common.Hash(blockRoot)).
		DoAndReturn(func(uint64, common.Hash) (bool, error) {
			errCh <- errCheckReached
			return false, nil
		})
	store.InitPeerDas(peerDas)

	respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1)
	respCh <- &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{Data: []*cltypes.SignedBeaconBlock{block}}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err = listenToIncomingBlocksUntilANewBlockIsReceived(ctx, log.Root(), &Cfg{
		beaconCfg:  &cfg,
		forkChoice: store,
		indiciesDB: db,
	}, Args{targetSlot: block.Block.Slot}, respCh, errCh)
	require.ErrorIs(t, err, errCheckReached)
	_, imported := store.GetHeader(common.Hash(blockRoot))
	require.False(t, imported)
}
