package forkchoice

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	das_mock "github.com/erigontech/erigon/cl/das/mock_services"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
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

			err := f.checkPreGloasBlockDataAvailability(t.Context(), block, root, versionedHashes, tt.columnsStored, nil)
			if tt.wantUnavailable {
				require.ErrorIs(t, err, ErrEIP7594ColumnDataNotAvailable)
				return
			}
			require.NoError(t, err)
		})
	}
}

// newFuluStoreWithBlock returns a store anchored at slot 1 whose clock is at genesis, and an unhashed Fulu block with
// one blob for slot 2.
func newFuluStoreWithBlock(t *testing.T) (*ForkChoiceStore, *cltypes.SignedBeaconBlock) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch, cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch = 0, 0, 0, 0, 0, 0
	cfg.InitializeForkSchedule()

	anchor := state2.New(&cfg)
	anchor.SetVersion(clparams.FuluVersion)
	require.NoError(t, anchor.SetSlot(1))
	anchorRoot, err := anchor.BlockRoot()
	require.NoError(t, err)
	parent := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	parent.Block.Slot = 1
	graph := &getFinalizedExecutionHashForkGraph{
		blocks:     map[common.Hash]*cltypes.SignedBeaconBlock{anchorRoot: parent},
		headers:    map[common.Hash]*cltypes.BeaconBlockHeader{anchorRoot: {Slot: 1}},
		anchorRoot: anchorRoot,
		anchorSlot: 1,
	}
	store, err := NewForkChoiceStore(eth_clock.NewEthereumClock(0, common.Hash{}, &cfg), anchor, nil, pool.NewOperationsPool(&cfg), graph,
		beaconevents.NewEventEmitter(), synced_data.NewSyncedDataManager(&cfg, true), nil,
		public_keys_registry.NewInMemoryPublicKeysRegistry(), validator_params.NewValidatorParams(), false, nil)
	require.NoError(t, err)

	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
	block.Block.Slot = 2
	block.Block.ParentRoot = anchorRoot
	block.Block.Body.BlobKzgCommitments.Append(new(cltypes.KZGCommitment))
	return store, block
}

func TestOnBlockReadsColumnAvailabilityWithoutForkChoiceLock(t *testing.T) {
	store, block := newFuluStoreWithBlock(t)
	store.OnTick(2 * store.beaconCfg.SecondsPerSlot * 1000)

	lockFree := false
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, mustRoot(t, block)).DoAndReturn(func(uint64, common.Hash) (bool, error) {
		if lockFree = store.mu.TryLock(); lockFree {
			store.mu.Unlock()
		}
		return false, nil
	})
	store.peerDas = peerDas

	require.ErrorIs(t, store.OnBlock(t.Context(), block, false, false, true), ErrEIP7594ColumnDataNotAvailable)
	require.True(t, lockFree, "column availability was read while holding f.mu")
}

func TestOnBlockAdmitsBlockWithAvailableColumns(t *testing.T) {
	store, block := newFuluStoreWithBlock(t)
	store.OnTick(2 * store.beaconCfg.SecondsPerSlot * 1000)

	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, mustRoot(t, block)).Return(true, nil)
	store.peerDas = peerDas
	graph := store.forkGraph.(*getFinalizedExecutionHashForkGraph)
	graph.addChainSegmentErr = errors.New("stop after data availability")

	require.ErrorIs(t, store.OnBlock(t.Context(), block, false, false, true), graph.addChainSegmentErr)
	require.True(t, graph.addChainSegmentCalled, "block with available columns must reach the state transition")
}

// Admission verdicts decide whether gossip retries or rejects a block, so a failed column read must not hide them.
func TestOnBlockAdmissionErrorTakesPriorityOverColumnReadError(t *testing.T) {
	store, block := newFuluStoreWithBlock(t)

	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, mustRoot(t, block)).Return(false, errors.New("stat failed"))
	store.peerDas = peerDas

	require.ErrorIs(t, store.OnBlock(t.Context(), block, false, false, true), ErrBlockTooEarly)
}

func TestOnBlockSkipsColumnReadForNilCommitments(t *testing.T) {
	store, block := newFuluStoreWithBlock(t)
	block.Block.Body.BlobKzgCommitments = nil
	store.peerDas = das_mock.NewMockPeerDas(gomock.NewController(t))

	require.ErrorIs(t, store.OnBlock(t.Context(), block, false, false, true), ErrBlockTooEarly)
}

func TestOnBlockReturnsColumnReadErrorAfterAdmission(t *testing.T) {
	store, block := newFuluStoreWithBlock(t)
	store.OnTick(2 * store.beaconCfg.SecondsPerSlot * 1000)

	readErr := errors.New("stat failed")
	peerDas := das_mock.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().IsDataAvailable(block.Block.Slot, mustRoot(t, block)).Return(false, readErr)
	store.peerDas = peerDas

	require.ErrorIs(t, store.OnBlock(t.Context(), block, false, false, true), readErr)
}
