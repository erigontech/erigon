package epbs

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/stretchr/testify/require"
)

type failingPendingPayloadStore struct{}

func (failingPendingPayloadStore) Save(context.Context, pendingPayloadKey, *pendingPayload, common.Bytes48) error {
	return errors.New("disk unavailable")
}

func TestDecodeStoredPendingPayloadRejectsBlobCommitmentMismatch(t *testing.T) {
	loop, _, _, _ := setupBuilderLoop(t)
	key := pendingPayloadKey{
		slot: 100, parentBlockHash: common.HexToHash("0xdead"),
		parentBlockRoot: common.HexToHash("0xbeef"), blockHash: common.HexToHash("0xb10c"),
	}
	var blob goethkzg.Blob
	commitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof(&blob, commitment, 0)
	require.NoError(t, err)
	pending := &pendingPayload{
		slot: 100, builderIndex: 42, bidValue: 1, parent: testParentInfo(),
		assembled: makeTestPayload(t, big.NewInt(1)),
		execReqs:  cltypes.NewExecutionRequestsWithVersion(loop.beaconCfg, clparams.GloasVersion),
	}
	pending.assembled.BlobsBundle = &eladapter.BlobsBundle{
		Blobs: [][]byte{append([]byte(nil), blob[:]...)}, Commitments: [][]byte{append([]byte(nil), commitment[:]...)},
		Proofs: [][]byte{append([]byte(nil), proof[:]...)},
	}
	record, err := encodeStoredPendingPayload(key, pending, loop.manager.Pubkey())
	require.NoError(t, err)
	record.Blobs[0][len(record.Blobs[0])-1] ^= 1

	_, _, err = decodeStoredPendingPayload(record, loop.beaconCfg)
	require.ErrorContains(t, err, "KZG")
}

func TestFilePendingPayloadStoreSyncsDirectoryAfterSaveAndDelete(t *testing.T) {
	loop, _, _, _ := setupBuilderLoop(t)
	store := newFilePendingPayloadStore(t.TempDir(), loop.beaconCfg)
	key := pendingPayloadKey{slot: 100, parentBlockHash: common.HexToHash("0xdead"), parentBlockRoot: common.HexToHash("0xbeef"), blockHash: common.HexToHash("0xb10c")}
	pending := &pendingPayload{
		slot: 100, builderIndex: 42, bidValue: 1, parent: testParentInfo(),
		assembled: makeTestPayload(t, big.NewInt(1)),
		execReqs:  cltypes.NewExecutionRequestsWithVersion(loop.beaconCfg, clparams.GloasVersion),
	}
	var saveSynced, deleteSynced bool
	store.syncDir = func(string) error {
		_, err := os.Stat(store.path(key))
		if errors.Is(err, os.ErrNotExist) {
			deleteSynced = true
		} else {
			require.NoError(t, err)
			saveSynced = true
		}
		return nil
	}
	require.NoError(t, store.Save(t.Context(), key, pending, loop.manager.Pubkey()))
	require.True(t, saveSynced)
	require.NoError(t, store.Delete(t.Context(), key))
	require.True(t, deleteSynced)
}
func (failingPendingPayloadStore) Delete(context.Context, pendingPayloadKey) error { return nil }
func (failingPendingPayloadStore) Load(context.Context) ([]storedPendingPayload, error) {
	return nil, nil
}

type rollbackFailingPendingPayloadStore struct {
	saves       int
	failSaveAt  int
	failSaveErr error
}

func (s *rollbackFailingPendingPayloadStore) Save(context.Context, pendingPayloadKey, *pendingPayload, common.Bytes48) error {
	s.saves++
	if s.saves == s.failSaveAt {
		if s.failSaveErr != nil {
			return s.failSaveErr
		}
		return errors.New("rollback unavailable")
	}
	return nil
}

func (*rollbackFailingPendingPayloadStore) Delete(context.Context, pendingPayloadKey) error {
	return errors.New("rollback unavailable")
}

func (*rollbackFailingPendingPayloadStore) Load(context.Context) ([]storedPendingPayload, error) {
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

func TestBuilderLoopRetainsOwnershipWhenDurableRollbackFails(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	store := &rollbackFailingPendingPayloadStore{}
	loop.pendingStore = store
	sc := testSlotContext()
	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	require.NoError(t, loop.OnNewHead(t.Context(), sc))
	prefsWatch.OnPreferencesReceived(sc.Slot, &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: sc.Slot, TargetGasLimit: 30_000_000,
	}})
	submitter.submitBidErr = fmt.Errorf("%w: rejected before publication", ErrBidNotPublished)

	err := loop.OnSlot(t.Context(), sc)
	require.ErrorContains(t, err, "rollback unavailable")
	require.Len(t, loop.pendingPayloads, 1)
	require.NotZero(t, loop.manager.reservedBidValue)
}

func TestBuilderLoopReleasesAllReplacementReservationsAfterRollbackFailure(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	store := &rollbackFailingPendingPayloadStore{failSaveAt: 3}
	loop.pendingStore = store
	sc := testSlotContext()
	prefs := &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: sc.Slot, TargetGasLimit: 30_000_000,
	}}

	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	require.NoError(t, loop.OnNewHead(t.Context(), sc))
	prefsWatch.OnPreferencesReceived(sc.Slot, prefs)
	require.NoError(t, loop.OnSlot(t.Context(), sc))
	firstReservation := loop.manager.reservedBidValue
	require.NotZero(t, firstReservation)

	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	require.NoError(t, loop.OnNewHead(t.Context(), sc))
	prefsWatch.OnPreferencesReceived(sc.Slot, prefs)
	submitter.submitBidErr = fmt.Errorf("%w: rejected before publication", ErrBidNotPublished)
	require.ErrorContains(t, loop.OnSlot(t.Context(), sc), "rollback unavailable")
	require.Greater(t, loop.manager.reservedBidValue, firstReservation)

	loop.releaseReservationsBeforeSlot(sc.Slot + 1)
	require.Zero(t, loop.manager.reservedBidValue)
}

func TestBuilderLoopReleasesAllReplacementReservationsAfterUncertainSave(t *testing.T) {
	loop, exec, _, prefsWatch := setupBuilderLoop(t)
	store := &rollbackFailingPendingPayloadStore{
		failSaveAt:  2,
		failSaveErr: fmt.Errorf("%w: sync unavailable", ErrPendingPayloadMayExist),
	}
	loop.pendingStore = store
	sc := testSlotContext()
	prefs := &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: sc.Slot, TargetGasLimit: 30_000_000,
	}}

	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	require.NoError(t, loop.OnNewHead(t.Context(), sc))
	prefsWatch.OnPreferencesReceived(sc.Slot, prefs)
	require.NoError(t, loop.OnSlot(t.Context(), sc))
	firstReservation := loop.manager.reservedBidValue
	require.NotZero(t, firstReservation)

	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	require.NoError(t, loop.OnNewHead(t.Context(), sc))
	prefsWatch.OnPreferencesReceived(sc.Slot, prefs)
	require.ErrorContains(t, loop.OnSlot(t.Context(), sc), "sync unavailable")
	require.Greater(t, loop.manager.reservedBidValue, firstReservation)

	loop.releaseReservationsBeforeSlot(sc.Slot + 1)
	require.Zero(t, loop.manager.reservedBidValue)
}

func TestBuilderLoopKeepsReservationWhenDurablePruneFails(t *testing.T) {
	loop, _, _, _ := setupBuilderLoop(t)
	loop.pendingStore = &rollbackFailingPendingPayloadStore{}
	const bidValue = uint64(10)
	require.True(t, loop.manager.ReserveBidWithStatus(BalanceStatus{Active: true, Balance: bidValue}, bidValue))
	key := pendingPayloadKey{slot: 1, blockHash: common.HexToHash("0xb10c")}
	loop.pendingPayloads[key] = &pendingPayload{slot: 1, bidValue: bidValue}

	loop.pruneBeforeSlot(2)
	require.Contains(t, loop.pendingPayloads, key)
	require.Equal(t, bidValue, loop.manager.reservedBidValue)
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
	require.NoError(t, restarted.restorePendingPayloads(t.Context(), sc.Slot))
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
