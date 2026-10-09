package forkchoice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	das_mock "github.com/erigontech/erigon/cl/das/mock_services"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
)

// Fulu fork choice admits a block only once this node holds its custody columns (on_block asserts is_data_available).
// Admitting it earlier makes the node attest to and serve the block while peers that ask it for the block's columns
// get a partial answer and score it down.
func TestPreGloasBlockDataAvailabilityRequiresFuluCustodyColumns(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch, cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch = 0, 0, 0, 0, 0, 0
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = 5
	block.Block.Body.BlobKzgCommitments.Append(new(cltypes.KZGCommitment))
	root := common.Hash{1}
	versionedHashes := []common.Hash{{2}}

	// A node following the chain, not one still syncing.
	syncedData := synced_data.NewSyncedDataManager(&cfg, true)
	headState := state2.New(&cfg)
	headState.SetVersion(clparams.FuluVersion)
	require.NoError(t, headState.SetSlot(block.Block.Slot-1))
	require.NoError(t, syncedData.OnHeadState(headState))
	require.False(t, syncedData.Syncing())

	for _, tt := range []struct {
		name            string
		columnsStored   bool
		elHasBlobs      bool
		wantUnavailable bool
	}{
		{name: "custody columns missing", wantUnavailable: true},
		{name: "custody columns missing but the EL has the blobs", elHasBlobs: true, wantUnavailable: true},
		{name: "custody columns stored", columnsStored: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			peerDas := das_mock.NewMockPeerDas(ctrl)
			peerDas.EXPECT().IsArchivedMode().Return(false).AnyTimes()
			peerDas.EXPECT().IsDataAvailable(block.Block.Slot, root).Return(tt.columnsStored, nil).AnyTimes()
			if tt.wantUnavailable {
				peerDas.EXPECT().SyncColumnDataLater(block).Return(nil)
			}
			engine := execution_client.NewMockExecutionEngine(ctrl)
			engine.EXPECT().GetBlobs(gomock.Any(), versionedHashes, clparams.FuluVersion).DoAndReturn(
				func(_ context.Context, hashes []common.Hash, _ clparams.StateVersion) ([][]byte, [][][]byte, error) {
					if !tt.elHasBlobs {
						return nil, nil, nil
					}
					return make([][]byte, len(hashes)), make([][][]byte, len(hashes)), nil
				}).AnyTimes()
			f := &ForkChoiceStore{beaconCfg: &cfg, engine: engine, peerDas: peerDas, syncedDataManager: syncedData}

			err := f.checkPreGloasBlockDataAvailability(t.Context(), block, root, versionedHashes)
			if tt.wantUnavailable {
				require.ErrorIs(t, err, ErrEIP7594ColumnDataNotAvailable)
				return
			}
			require.NoError(t, err)
		})
	}
}
