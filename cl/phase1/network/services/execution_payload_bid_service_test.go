package services

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	synced_data_mock "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	forkchoice_mock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

func setupExecutionPayloadBidService(t *testing.T, ctrl *gomock.Controller) (
	*executionPayloadBidService,
	*synced_data_mock.MockSyncedData,
	*eth_clock.MockEthereumClock,
	*forkchoice_mock.ForkChoiceStorageMock,
	*pool.EpbsPool,
) {
	mockSyncedData := synced_data_mock.NewMockSyncedData(ctrl)
	ethClockMock := eth_clock.NewMockEthereumClock(ctrl)
	fcMock := forkchoice_mock.NewForkChoiceStorageMock(t)
	epbsPool := pool.NewEpbsPool()
	beaconCfg := &clparams.BeaconChainConfig{
		SlotsPerEpoch:       32,
		DomainBeaconBuilder: [4]byte{0x0B, 0x00, 0x00, 0x00},
	}

	seenCache, err := lru.New[seenBidKey, struct{}]("seen_bids_test", seenBidCacheSize)
	require.NoError(t, err)

	service := &executionPayloadBidService{
		syncedDataManager: mockSyncedData,
		forkchoiceStore:   fcMock,
		ethClock:          ethClockMock,
		beaconCfg:         beaconCfg,
		epbsPool:          epbsPool,
		seenCache:         seenCache,
		pendingCond:       sync.NewCond(&sync.Mutex{}),
	}

	return service, mockSyncedData, ethClockMock, fcMock, epbsPool
}

func newTestSignedExecutionPayloadBid(slot uint64, builderIndex uint64, value uint64) *cltypes.SignedExecutionPayloadBid {
	return &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			Slot:               slot,
			BuilderIndex:       builderIndex,
			Value:              value,
			ParentBlockHash:    common.HexToHash("0xaaaa"),
			ParentBlockRoot:    common.HexToHash("0xbbbb"),
			BlockHash:          common.HexToHash("0xcccc"),
			FeeRecipient:       common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"),
			GasLimit:           30_000_000,
			ExecutionPayment:   0,
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
		},
		Signature: common.Bytes96{},
	}
}

// addPreferencesToPool adds a SignedProposerPreferences to the pool for the given slot.
func addPreferencesToPool(epbsPool *pool.EpbsPool, slot uint64) {
	epbsPool.ProposerPreferences.Add(slot, &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   slot,
			ValidatorIndex: 99,
			FeeRecipient:   common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"),
			GasLimit:       30_000_000,
		},
	})
}

func TestExecutionPayloadBidServiceNames(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, _ := setupExecutionPayloadBidService(t, ctrl)

	names := service.Names()
	require.Len(t, names, 1)
	require.Equal(t, "execution_payload_bid", names[0])
}

func TestExecutionPayloadBidServiceNilMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, _ := setupExecutionPayloadBidService(t, ctrl)

	err := service.ProcessMessage(context.Background(), nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil execution payload bid message")

	err = service.ProcessMessage(context.Background(), nil, &cltypes.SignedExecutionPayloadBid{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil execution payload bid message")
}

func TestExecutionPayloadBidServiceWrongSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, _ := setupExecutionPayloadBidService(t, ctrl)

	// Bid for slot 100, but current slot is 50 → IGNORE
	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(50))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "not current")
}

func TestExecutionPayloadBidServiceCurrentSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)

	// Current slot == bid slot → valid
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	// ViewHeadState mock returns nil without executing the callback,
	// so state-dependent checks (IsActiveBuilder, BLS, etc.) are skipped.
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	})

	// Set up forkchoice mock
	fcMock.ExecutionPayloadStatusMap[common.HexToHash("0xaaaa")] = execution_client.PayloadStatusValidated
	fcMock.Headers[common.HexToHash("0xbbbb")] = &cltypes.BeaconBlockHeader{}

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)
}

func TestExecutionPayloadBidServiceNextSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(101, 1, 1000)
	addPreferencesToPool(epbsPool, 101)

	// Current slot is 100, bid for slot 101 → valid (next slot)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	})

	fcMock.ExecutionPayloadStatusMap[common.HexToHash("0xaaaa")] = execution_client.PayloadStatusValidated
	fcMock.Headers[common.HexToHash("0xbbbb")] = &cltypes.BeaconBlockHeader{}

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)
}

func TestExecutionPayloadBidServiceNonZeroExecutionPayment(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, _ := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	msg.Message.ExecutionPayment = 42 // Non-zero → REJECT

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrIgnore)) // REJECT, not IGNORE
	require.Contains(t, err.Error(), "execution_payment must be zero")
}

func TestExecutionPayloadBidServiceNoPreferences(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	// Do NOT add preferences for slot 100

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	// Should return nil (queued as pending) — no error propagated
	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Bid should NOT be in highest bids (pending, not validated)
	bidKey := pool.HighestBidKey{Slot: 100, ParentBlockHash: common.HexToHash("0xaaaa")}
	_, found := epbsPool.HighestBids.Get(bidKey)
	require.False(t, found)
}

func TestExecutionPayloadBidServiceFeeRecipientMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	msg.Message.FeeRecipient = common.HexToAddress("0xdead") // Different from preferences

	addPreferencesToPool(epbsPool, 100)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrIgnore)) // REJECT
	require.Contains(t, err.Error(), "fee_recipient")
}

func TestExecutionPayloadBidServiceGasLimitMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	msg.Message.GasLimit = 99_999 // Different from preferences (30_000_000)

	addPreferencesToPool(epbsPool, 100)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrIgnore)) // REJECT
	require.Contains(t, err.Error(), "gas_limit")
}

func TestExecutionPayloadBidServiceDuplicate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)

	fcMock.ExecutionPayloadStatusMap[common.HexToHash("0xaaaa")] = execution_client.PayloadStatusValidated
	fcMock.Headers[common.HexToHash("0xbbbb")] = &cltypes.BeaconBlockHeader{}

	// First call succeeds
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	})

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Second call → IGNORE (already seen from this builder for this slot)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err = service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "already seen bid")
}

func TestExecutionPayloadBidServiceViewHeadStateError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, _, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return fmt.Errorf("not synced")
	})

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bid validation failed")
	require.Contains(t, err.Error(), "not synced")

	// Should NOT be marked as seen
	seenKey := seenBidKey{builderIndex: 1, slot: 100}
	require.False(t, service.seenCache.Contains(seenKey))
}

func TestExecutionPayloadBidServiceParentBlockHashUnknown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	})

	// parent_block_hash NOT in forkchoice
	// (ExecutionPayloadStatusMap is empty for this hash)
	_ = fcMock

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "parent_block_hash")
}

func TestExecutionPayloadBidServiceParentBlockRootUnknown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	})

	// parent_block_hash known, but parent_block_root NOT known
	fcMock.ExecutionPayloadStatusMap[common.HexToHash("0xaaaa")] = execution_client.PayloadStatusValidated
	// Headers map is empty → parent_block_root not found

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "parent_block_root")
}

func TestExecutionPayloadBidServiceHighestBid(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	fcMock.ExecutionPayloadStatusMap[common.HexToHash("0xaaaa")] = execution_client.PayloadStatusValidated
	fcMock.Headers[common.HexToHash("0xbbbb")] = &cltypes.BeaconBlockHeader{}

	addPreferencesToPool(epbsPool, 100)

	// First bid: value 1000
	msg1 := newTestSignedExecutionPayloadBid(100, 1, 1000)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	})

	err := service.ProcessMessage(context.Background(), nil, msg1)
	require.NoError(t, err)

	// Check highest bid
	bidKey := pool.HighestBidKey{Slot: 100, ParentBlockHash: common.HexToHash("0xaaaa")}
	stored, found := epbsPool.HighestBids.Get(bidKey)
	require.True(t, found)
	require.Equal(t, uint64(1000), stored.Message.Value)

	// Second bid from different builder: value 2000 (higher → should replace)
	msg2 := newTestSignedExecutionPayloadBid(100, 2, 2000)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	})

	err = service.ProcessMessage(context.Background(), nil, msg2)
	require.NoError(t, err)

	stored, found = epbsPool.HighestBids.Get(bidKey)
	require.True(t, found)
	require.Equal(t, uint64(2000), stored.Message.Value)

	// Third bid from yet another builder: value 500 (lower → IGNORE)
	msg3 := newTestSignedExecutionPayloadBid(100, 3, 500)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	})

	err = service.ProcessMessage(context.Background(), nil, msg3)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "not higher than existing")

	// Highest bid should still be 2000
	stored, found = epbsPool.HighestBids.Get(bidKey)
	require.True(t, found)
	require.Equal(t, uint64(2000), stored.Message.Value)
}

func TestExecutionPayloadBidServiceDifferentParentHashes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	parentHash1 := common.HexToHash("0x1111")
	parentHash2 := common.HexToHash("0x2222")
	parentRoot := common.HexToHash("0xbbbb")

	fcMock.ExecutionPayloadStatusMap[parentHash1] = execution_client.PayloadStatusValidated
	fcMock.ExecutionPayloadStatusMap[parentHash2] = execution_client.PayloadStatusValidated
	fcMock.Headers[parentRoot] = &cltypes.BeaconBlockHeader{}

	addPreferencesToPool(epbsPool, 100)

	// Bid 1: parentBlockHash = 0x1111, value 1000
	msg1 := newTestSignedExecutionPayloadBid(100, 1, 1000)
	msg1.Message.ParentBlockHash = parentHash1
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	})

	err := service.ProcessMessage(context.Background(), nil, msg1)
	require.NoError(t, err)

	// Bid 2: parentBlockHash = 0x2222, value 500 (separate market → should succeed)
	msg2 := newTestSignedExecutionPayloadBid(100, 2, 500)
	msg2.Message.ParentBlockHash = parentHash2
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	})

	err = service.ProcessMessage(context.Background(), nil, msg2)
	require.NoError(t, err)

	// Both should have their own highest bid
	bidKey1 := pool.HighestBidKey{Slot: 100, ParentBlockHash: parentHash1}
	bidKey2 := pool.HighestBidKey{Slot: 100, ParentBlockHash: parentHash2}
	stored1, found1 := epbsPool.HighestBids.Get(bidKey1)
	stored2, found2 := epbsPool.HighestBids.Get(bidKey2)
	require.True(t, found1)
	require.True(t, found2)
	require.Equal(t, uint64(1000), stored1.Message.Value)
	require.Equal(t, uint64(500), stored2.Message.Value)
}

func TestExecutionPayloadBidServiceSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	})

	fcMock.ExecutionPayloadStatusMap[common.HexToHash("0xaaaa")] = execution_client.PayloadStatusValidated
	fcMock.Headers[common.HexToHash("0xbbbb")] = &cltypes.BeaconBlockHeader{}

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Verify stored in seen cache
	seenKey := seenBidKey{builderIndex: 1, slot: 100}
	require.True(t, service.seenCache.Contains(seenKey))

	// Verify stored in pool
	bidKey := pool.HighestBidKey{Slot: 100, ParentBlockHash: common.HexToHash("0xaaaa")}
	stored, found := epbsPool.HighestBids.Get(bidKey)
	require.True(t, found)
	require.Equal(t, msg, stored)
}

func TestExecutionPayloadBidServiceDecodeGossipMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, _ := setupExecutionPayloadBidService(t, ctrl)

	original := newTestSignedExecutionPayloadBid(100, 1, 1000)
	encoded, err := original.EncodeSSZ(nil)
	require.NoError(t, err)

	decoded, err := service.DecodeGossipMessage("peer123", encoded, clparams.GloasVersion)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	require.Equal(t, original.Message.Slot, decoded.Message.Slot)
	require.Equal(t, original.Message.BuilderIndex, decoded.Message.BuilderIndex)
	require.Equal(t, original.Message.Value, decoded.Message.Value)
	require.Equal(t, original.Message.FeeRecipient, decoded.Message.FeeRecipient)
	require.Equal(t, original.Message.GasLimit, decoded.Message.GasLimit)
	require.Equal(t, original.Message.ParentBlockHash, decoded.Message.ParentBlockHash)
	require.Equal(t, original.Message.ExecutionPayment, decoded.Message.ExecutionPayment)
}

func TestExecutionPayloadBidServiceDecodeGossipMessageInvalid(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, _ := setupExecutionPayloadBidService(t, ctrl)

	_, err := service.DecodeGossipMessage("peer123", []byte{0x00, 0x01, 0x02}, clparams.GloasVersion)
	require.Error(t, err)
}

func TestExecutionPayloadBidServiceFailedValidationNotStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, mockSyncedData, ethClockMock, _, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return fmt.Errorf("builder 1 is not active")
	})

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)

	// Should NOT be in seen cache
	seenKey := seenBidKey{builderIndex: 1, slot: 100}
	require.False(t, service.seenCache.Contains(seenKey))

	// Should NOT be in pool
	bidKey := pool.HighestBidKey{Slot: 100, ParentBlockHash: common.HexToHash("0xaaaa")}
	_, found := epbsPool.HighestBids.Get(bidKey)
	require.False(t, found)
}
