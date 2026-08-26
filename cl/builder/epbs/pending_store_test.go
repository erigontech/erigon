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
	"github.com/erigontech/erigon/cl/cltypes/solid"
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

func TestDecodeStoredPendingPayloadRejectsExecutionRequestsRootMismatch(t *testing.T) {
	loop, _, _, _ := setupBuilderLoop(t)
	key := pendingPayloadKey{
		slot: 100, parentBlockHash: common.HexToHash("0xdead"),
		parentBlockRoot: common.HexToHash("0xbeef"), blockHash: common.HexToHash("0xb10c"),
	}
	pending := &pendingPayload{
		slot: 100, builderIndex: 42, bidValue: 1, parent: testParentInfo(),
		assembled: makeTestPayload(t, big.NewInt(1)),
		execReqs:  cltypes.NewExecutionRequestsWithVersion(loop.beaconCfg, clparams.GloasVersion),
	}
	record, err := encodeStoredPendingPayload(key, pending, loop.manager.Pubkey())
	require.NoError(t, err)
	altered := cltypes.NewExecutionRequestsWithVersion(loop.beaconCfg, clparams.GloasVersion)
	altered.BuilderDeposits.Append(&solid.BuilderDepositRequest{Amount: 123})
	record.ExecutionRequests, err = altered.EncodeSSZ(nil)
	require.NoError(t, err)

	_, _, err = decodeStoredPendingPayload(record, loop.beaconCfg)
	require.ErrorContains(t, err, "execution requests root")
}

func TestDecodeStoredPendingPayloadAcceptsZeroValueBid(t *testing.T) {
	loop, _, _, _ := setupBuilderLoop(t)
	key := pendingPayloadKey{
		slot: 100, parentBlockHash: common.HexToHash("0xdead"),
		parentBlockRoot: common.HexToHash("0xbeef"), blockHash: common.HexToHash("0xb10c"),
	}
	pending := &pendingPayload{
		slot: 100, builderIndex: 42, parent: testParentInfo(), assembled: makeTestPayload(t, big.NewInt(1)),
		execReqs: cltypes.NewExecutionRequestsWithVersion(loop.beaconCfg, clparams.GloasVersion),
	}
	record, err := encodeStoredPendingPayload(key, pending, loop.manager.Pubkey())
	require.NoError(t, err)

	_, restored, err := decodeStoredPendingPayload(record, loop.beaconCfg)
	require.NoError(t, err)
	require.Zero(t, restored.bidValue)
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
func (failingPendingPayloadStore) Load(context.Context, uint64) ([]storedPendingPayload, error) {
	return nil, nil
}

type rollbackFailingPendingPayloadStore struct {
	saves   int
	deletes int
}

func (s *rollbackFailingPendingPayloadStore) Save(context.Context, pendingPayloadKey, *pendingPayload, common.Bytes48) error {
	s.saves++
	return nil
}

func (s *rollbackFailingPendingPayloadStore) Delete(context.Context, pendingPayloadKey) error {
	s.deletes++
	return errors.New("rollback unavailable")
}

func (*rollbackFailingPendingPayloadStore) Load(context.Context, uint64) ([]storedPendingPayload, error) {
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

func TestBuilderLoopDoesNotRetryExpiredPendingDeletionForever(t *testing.T) {
	loop, _, _, _ := setupBuilderLoop(t)
	store := &rollbackFailingPendingPayloadStore{}
	loop.pendingStore = store
	const bidValue = uint64(10)
	require.True(t, loop.manager.ReserveBidWithStatus(BalanceStatus{Active: true, Balance: bidValue}, bidValue))
	key := pendingPayloadKey{slot: 1, blockHash: common.HexToHash("0xb10c")}
	loop.pendingPayloads[key] = &pendingPayload{slot: 1, bidValue: bidValue}

	loop.pruneBeforeSlot(2)
	loop.pruneBeforeSlot(3)
	require.Equal(t, 1, store.deletes)
	require.NotContains(t, loop.pendingPayloads, key)
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
	records, err := store.Load(t.Context(), 0)
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestBuilderLoopRestoreIgnoresExpiredFilesBeforeCapacityCheck(t *testing.T) {
	loop, _, _, _ := setupBuilderLoop(t)
	store := newFilePendingPayloadStore(t.TempDir(), loop.beaconCfg)
	store.syncDir = func(string) error { return nil }
	save := func(slot uint64) pendingPayloadKey {
		blockHash := common.BigToHash(new(big.Int).SetUint64(slot + 1))
		key := pendingPayloadKey{
			slot: slot, parentBlockHash: common.HexToHash("0xdead"),
			parentBlockRoot: common.HexToHash("0xbeef"), blockHash: blockHash,
		}
		payload := makeTestPayload(t, big.NewInt(1))
		payload.Eth1Block.SlotNumber = slot
		payload.Eth1Block.BlockHash = blockHash
		pending := &pendingPayload{
			slot: slot, builderIndex: 42, bidValue: 1, parent: testParentInfo(), assembled: payload,
			execReqs: cltypes.NewExecutionRequestsWithVersion(loop.beaconCfg, clparams.GloasVersion),
		}
		require.NoError(t, store.Save(t.Context(), key, pending, loop.manager.Pubkey()))
		return key
	}
	for slot := uint64(1); slot <= maxPendingPayloadFiles+1; slot++ {
		save(slot)
	}
	const currentSlot = uint64(1_000)
	currentKey := save(currentSlot)

	restarted, _, _, _ := setupBuilderLoop(t)
	restarted.pendingStore = store
	require.NoError(t, restarted.restorePendingPayloads(t.Context(), currentSlot))
	require.Contains(t, restarted.pendingPayloads, currentKey)
	require.Len(t, restarted.pendingPayloads, 1)
}
