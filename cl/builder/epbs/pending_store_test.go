package epbs

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

type failingPendingPayloadStore struct{}

func (failingPendingPayloadStore) Save(context.Context, pendingPayloadKey, *pendingPayload, common.Bytes48) error {
	return errors.New("disk unavailable")
}
func (failingPendingPayloadStore) Delete(context.Context, pendingPayloadKey) error { return nil }
func (failingPendingPayloadStore) Load(context.Context) ([]storedPendingPayload, error) {
	return nil, nil
}

func TestBuilderLoopDoesNotPublishWithoutDurablePendingPayload(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	loop.pendingStore = failingPendingPayloadStore{}
	sc := testSlotContext()
	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	require.NoError(t, loop.OnNewHead(t.Context(), sc))
	prefsWatch.OnPreferencesReceived(sc.Slot, &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: sc.Slot, TargetGasLimit: 30_000_000,
	}})

	require.ErrorContains(t, loop.OnSlot(t.Context(), sc), "disk unavailable")
	require.Empty(t, submitter.submittedBids)
	require.Empty(t, loop.pendingPayloads)
	require.Zero(t, loop.manager.reservedBidValue)
}

func TestBuilderLoopRestoresPublishedPendingPayloadAfterRestart(t *testing.T) {
	loop, exec, _, prefsWatch := setupBuilderLoop(t)
	store := newFilePendingPayloadStore(t.TempDir(), loop.beaconCfg)
	loop.pendingStore = store
	sc := testSlotContext()
	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	require.NoError(t, loop.OnNewHead(t.Context(), sc))
	prefsWatch.OnPreferencesReceived(sc.Slot, &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: sc.Slot, TargetGasLimit: 30_000_000,
	}})
	require.NoError(t, loop.OnSlot(t.Context(), sc))

	restarted, _, submitter, _ := setupBuilderLoop(t)
	restarted.pendingStore = store
	require.NoError(t, restarted.restorePendingPayloads(t.Context()))
	require.Len(t, restarted.pendingPayloads, 1)
	require.NotZero(t, restarted.manager.reservedBidValue)
	root := common.HexToHash("0xbeef")
	block := cltypes.NewSignedBeaconBlock(restarted.beaconCfg, clparams.GloasVersion)
	block.Block.Slot = sc.Slot
	block.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		Slot: sc.Slot, BuilderIndex: 42, ParentBlockHash: sc.Parent.ExecutionHash,
		ParentBlockRoot: sc.Parent.BlockRoot, BlockHash: common.HexToHash("0xb10c"),
	}}
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix())-sc.Slot*restarted.beaconCfg.SecondsPerSlot, common.Hash{}, restarted.beaconCfg)
	scheduler := newRevealScheduler(t.Context(), 1, 1)
	require.NoError(t, scheduleImportedBlockReveal(
		&beaconevents.BlockData{Slot: sc.Slot, Block: root}, testImportedBlockReader{block: block},
		clock, restarted.beaconCfg, restarted, scheduler,
	))
	scheduler.Wait()
	require.Len(t, submitter.broadcasts, 1)
	restarted.pruneBeforeSlot(sc.Slot + 1)
	records, err := store.Load(t.Context())
	require.NoError(t, err)
	require.Empty(t, records)
}
