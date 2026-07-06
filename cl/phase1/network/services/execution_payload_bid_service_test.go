package services

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	synced_data_mock "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
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
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.SlotsPerEpoch = 32
	beaconCfg.SlotsPerHistoricalRoot = 8192
	beaconCfg.MinSeedLookahead = 1
	beaconCfg.DomainBeaconBuilder = [4]byte{0x0B, 0x00, 0x00, 0x00}
	fcMock.StateAtBlockRootVal[common.HexToHash("0xbbbb")] = newBidParentState(&beaconCfg, testDependentRoot)
	fcMock.Ancestors[63] = forkchoice.ForkChoiceNode{Root: testDependentRoot, PayloadStatus: cltypes.PayloadStatusPending}
	prevBlsVerify := blsVerify
	blsVerify = func(_ []byte, _ []byte, _ []byte) (bool, error) { return true, nil }
	t.Cleanup(func() { blsVerify = prevBlsVerify })

	seenCache, err := lru.New[seenBidKey, struct{}]("seen_bids_test", seenBidCacheSize)
	require.NoError(t, err)
	validationStateCache := lru.NewWithTTL[bidValidationStateKey, *bidValidationStateEntry](
		"bid_validation_states_test",
		bidValidationStateCacheSize,
		bidValidationStateCacheTTL(&beaconCfg),
	)

	service := &executionPayloadBidService{
		syncedDataManager:    mockSyncedData,
		forkchoiceStore:      fcMock,
		ethClock:             ethClockMock,
		beaconCfg:            &beaconCfg,
		epbsPool:             epbsPool,
		emitters:             beaconevents.NewEventEmitter(),
		seenCache:            seenCache,
		validationStateCache: validationStateCache,
		pendingCond:          sync.NewCond(&sync.Mutex{}),
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
			GasLimit:           30_000_000,
			FeeRecipient:       common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"),
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
		},
		Signature: common.Bytes96{},
	}
}

// addPreferencesToPool adds a SignedProposerPreferences to the pool for the given slot.
func addPreferencesToPool(epbsPool *pool.EpbsPool, slot uint64) {
	addPreferencesToPoolWithRoot(epbsPool, slot, testDependentRoot)
}

func addPreferencesToPoolWithRoot(epbsPool *pool.EpbsPool, slot uint64, dependentRoot common.Hash) {
	epbsPool.ProposerPreferences.Add(pool.ProposerPreferencesKey{
		Slot:          slot,
		DependentRoot: dependentRoot,
	}, &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   slot,
			ValidatorIndex: 99,
			FeeRecipient:   common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678"),
			TargetGasLimit: 30_000_000,
			DependentRoot:  dependentRoot,
		},
	})
}

func newBidParentState(cfg *clparams.BeaconChainConfig, dependentRoot common.Hash) *state2.CachingBeaconState {
	s := state2.New(cfg)
	s.SetVersion(clparams.GloasVersion)
	s.SetSlot(100)
	s.SetBlockRootAt(63, dependentRoot)
	s.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: 2})
	for i := 0; i < 8; i++ {
		var pk common.Bytes48
		pk[0] = byte(i)
		s.AddValidator(solid.NewValidatorFromParameters(pk, common.Hash{}, 0, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch), cfg.EffectiveBalanceIncrement)
	}
	s.SetPreviousEpochParticipationFlags(make(cltypes.ParticipationFlagsList, 8))
	s.SetCurrentEpochParticipationFlags(make(cltypes.ParticipationFlagsList, 8))
	s.SetProposerLookahead(solid.NewUint64VectorSSZ(int((cfg.MinSeedLookahead + 1) * cfg.SlotsPerEpoch)))
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, 73)
	for i := 0; i < 64; i++ {
		builders.Append(&cltypes.Builder{
			Version:           cfg.PayloadBuilderVersion,
			Balance:           cfg.MinDepositAmount + 1_000_000_000,
			DepositEpoch:      1,
			WithdrawableEpoch: cfg.FarFutureEpoch,
		})
	}
	s.SetBuilders(builders)
	return s
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

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)

	// Current slot == bid slot → valid
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	// Set up forkchoice mock
	fcMock.ExecutionPayloadStatusMap[common.HexToHash("0xaaaa")] = execution_client.PayloadStatusValidated
	fcMock.Headers[common.HexToHash("0xbbbb")] = &cltypes.BeaconBlockHeader{}

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)
}

func TestExecutionPayloadBidServiceRejectsNonPayloadBuilderVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)

	parentState := newBidParentState(service.beaconCfg, testDependentRoot)
	parentState.GetBuilders().Get(1).Version = service.beaconCfg.PayloadBuilderVersion + 1
	fcMock.StateAtBlockRootVal[msg.Message.ParentBlockRoot] = parentState

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{}

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported version")
}

func TestExecutionPayloadBidServiceNextSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(101, 1, 1000)
	parentState := fcMock.StateAtBlockRootVal[msg.Message.ParentBlockRoot]
	addPreferencesToPool(epbsPool, 101)

	// Current slot is 100, bid for slot 101 → valid (next slot)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	fcMock.ExecutionPayloadStatusMap[common.HexToHash("0xaaaa")] = execution_client.PayloadStatusValidated
	fcMock.Headers[common.HexToHash("0xbbbb")] = &cltypes.BeaconBlockHeader{}

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)
	require.Equal(t, uint64(100), parentState.Slot())
}

func TestExecutionPayloadBidServiceNoPreferences(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	// Do NOT add preferences for slot 100

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.True(t, errors.Is(err, ErrIgnore))

	// Bid should NOT be in highest bids (pending, not validated)
	bidKey := pool.HighestBidKey{Slot: 100, ParentBlockHash: common.HexToHash("0xaaaa"), ParentBlockRoot: common.HexToHash("0xbbbb")}
	_, found := epbsPool.HighestBids.Get(bidKey)
	require.False(t, found)
}

func TestExecutionPayloadBidServiceRejectsNonZeroExecutionPaymentBeforeQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, _ := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	msg.Message.ExecutionPayment = 1

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)

	require.Error(t, err)
	require.Contains(t, err.Error(), "execution_payment must be 0")
	require.Equal(t, int32(0), service.pendingCount.Load())
}

func TestExecutionPayloadBidServiceRejectsTooManyBlobCommitmentsBeforeQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, _ := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	maxBlobs := int(service.beaconCfg.GetBlobParameters(100 / service.beaconCfg.SlotsPerEpoch).MaxBlobsPerBlock)
	for i := 0; i <= maxBlobs; i++ {
		msg.Message.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	}

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)

	require.Error(t, err)
	require.Contains(t, err.Error(), "too many blob_kzg_commitments")
	require.Equal(t, int32(0), service.pendingCount.Load())
}

func TestExecutionPayloadBidServiceWaitsForMatchingDependentRootPreference(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	wrongRoot := common.Hash{0xee}
	epbsPool.ProposerPreferences.Add(pool.ProposerPreferencesKey{Slot: 100, DependentRoot: wrongRoot}, &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   100,
			ValidatorIndex: 99,
			FeeRecipient:   msg.Message.FeeRecipient,
			TargetGasLimit: 30_000_000,
			DependentRoot:  wrongRoot,
		},
	})

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	require.True(t, errors.Is(service.ProcessMessage(context.Background(), nil, msg), ErrIgnore))
	require.Equal(t, int32(1), service.pendingCount.Load())

	addPreferencesToPool(epbsPool, 100)
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{}
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	service.processPendingBids()
	require.Equal(t, int32(0), service.pendingCount.Load())
	_, found := epbsPool.HighestBids.Get(pool.HighestBidKey{Slot: 100, ParentBlockHash: msg.Message.ParentBlockHash, ParentBlockRoot: msg.Message.ParentBlockRoot})
	require.True(t, found)
}

func TestExecutionPayloadBidServiceWaitsForParentState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{}
	delete(fcMock.StateAtBlockRootVal, msg.Message.ParentBlockRoot)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	require.True(t, errors.Is(service.ProcessMessage(context.Background(), nil, msg), ErrIgnore))
	require.Equal(t, int32(1), service.pendingCount.Load())

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	service.processPendingBids()
	require.Equal(t, int32(1), service.pendingCount.Load())

	fcMock.StateAtBlockRootVal[msg.Message.ParentBlockRoot] = newBidParentState(service.beaconCfg, testDependentRoot)
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{}
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	service.processPendingBids()
	require.Equal(t, int32(0), service.pendingCount.Load())
	_, found := epbsPool.HighestBids.Get(pool.HighestBidKey{Slot: 100, ParentBlockHash: msg.Message.ParentBlockHash, ParentBlockRoot: msg.Message.ParentBlockRoot})
	require.True(t, found)
}

func TestExecutionPayloadBidServiceUsesDependentRootFromForkchoiceStore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	parentState := newBidParentState(service.beaconCfg, common.Hash{0xdd})
	fcMock.StateAtBlockRootVal[common.HexToHash("0xbbbb")] = parentState

	addPreferencesToPoolWithRoot(epbsPool, 100, testDependentRoot)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{}

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)
	require.Equal(t, uint64(100), parentState.Slot())
	_, found := epbsPool.HighestBids.Get(pool.HighestBidKey{Slot: 100, ParentBlockHash: msg.Message.ParentBlockHash, ParentBlockRoot: msg.Message.ParentBlockRoot})
	require.True(t, found)
}

func TestExecutionPayloadBidServiceRejectsEarlyEpochDependentRootLookup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, _ := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(1, 1, 1000)
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{}

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(1))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "cannot compute proposer dependent root")
}

func TestExecutionPayloadBidServiceGasLimitIncompatible(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	msg.Message.GasLimit = 99_999 // Incompatible with target 30_000_000 given parent 30_000_000

	addPreferencesToPool(epbsPool, 100)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	// Set up parent block hash as known with gas limit
	fcMock.ExecutionPayloadStatusMap[common.HexToHash("0xaaaa")] = execution_client.PayloadStatusValidated
	fcMock.ExecutionPayloadGasLimitMap[common.HexToHash("0xaaaa")] = 30_000_000
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{}

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore)) // Now IGNORE, not REJECT
	require.Contains(t, err.Error(), "gas_limit")
}

func TestExecutionPayloadBidServiceDuplicate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)

	fcMock.ExecutionPayloadStatusMap[common.HexToHash("0xaaaa")] = execution_client.PayloadStatusValidated
	fcMock.Headers[common.HexToHash("0xbbbb")] = &cltypes.BeaconBlockHeader{}

	// First call succeeds
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Second call → IGNORE (already seen from this builder for this slot)
	delete(fcMock.StateAtBlockRootVal, msg.Message.ParentBlockRoot)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err = service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "already seen bid")
}

func TestExecutionPayloadBidServiceBuilderInactiveError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	parentState := newBidParentState(service.beaconCfg, testDependentRoot)
	parentState.GetBuilders().Get(1).WithdrawableEpoch = 3
	fcMock.StateAtBlockRootVal[msg.Message.ParentBlockRoot] = parentState
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{}
	addPreferencesToPool(epbsPool, 100)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bid validation failed")
	require.Contains(t, err.Error(), "not active")

	// Should NOT be marked as seen
	seenKey := seenBidKey{builderIndex: 1, slot: 100}
	require.False(t, service.seenCache.Contains(seenKey))
}

func TestExecutionPayloadBidServiceParentBlockHashUnknown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	// parent_block_hash NOT in forkchoice
	// (ExecutionPayloadStatusMap is empty for this hash)
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{}

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "parent_block_hash")
}

func TestExecutionPayloadBidServiceParentBlockRootUnknown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

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

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	fcMock.ExecutionPayloadStatusMap[common.HexToHash("0xaaaa")] = execution_client.PayloadStatusValidated
	fcMock.Headers[common.HexToHash("0xbbbb")] = &cltypes.BeaconBlockHeader{}

	addPreferencesToPool(epbsPool, 100)

	// First bid: value 1000
	msg1 := newTestSignedExecutionPayloadBid(100, 1, 1000)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg1)
	require.NoError(t, err)

	// Check highest bid
	bidKey := pool.HighestBidKey{Slot: 100, ParentBlockHash: common.HexToHash("0xaaaa"), ParentBlockRoot: common.HexToHash("0xbbbb")}
	stored, found := epbsPool.HighestBids.Get(bidKey)
	require.True(t, found)
	require.Equal(t, uint64(1000), stored.Message.Value)

	// Second bid from different builder: value 2000 (higher → should replace)
	msg2 := newTestSignedExecutionPayloadBid(100, 2, 2000)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err = service.ProcessMessage(context.Background(), nil, msg2)
	require.NoError(t, err)

	stored, found = epbsPool.HighestBids.Get(bidKey)
	require.True(t, found)
	require.Equal(t, uint64(2000), stored.Message.Value)

	// Third bid from yet another builder: value 500 (lower → IGNORE)
	msg3 := newTestSignedExecutionPayloadBid(100, 3, 500)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err = service.ProcessMessage(context.Background(), nil, msg3)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "not higher than existing")

	// Highest bid should still be 2000
	stored, found = epbsPool.HighestBids.Get(bidKey)
	require.True(t, found)
	require.Equal(t, uint64(2000), stored.Message.Value)
}

func TestExecutionPayloadBidServiceRejectsLowerBidBeforeStateFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 3, 500)
	existing := newTestSignedExecutionPayloadBid(100, 1, 2000)
	bidKey := pool.HighestBidKey{Slot: msg.Message.Slot, ParentBlockHash: msg.Message.ParentBlockHash, ParentBlockRoot: msg.Message.ParentBlockRoot}
	epbsPool.HighestBids.Add(bidKey, existing)

	delete(fcMock.StateAtBlockRootVal, msg.Message.ParentBlockRoot)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "not higher than existing")
	require.Zero(t, service.validationStateCache.Len())
	require.Equal(t, int32(0), service.pendingCount.Load())
}

func TestExecutionPayloadBidServiceDifferentParentHashes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

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

	err := service.ProcessMessage(context.Background(), nil, msg1)
	require.NoError(t, err)

	// Bid 2: parentBlockHash = 0x2222, value 500 (separate market → should succeed)
	msg2 := newTestSignedExecutionPayloadBid(100, 2, 500)
	msg2.Message.ParentBlockHash = parentHash2
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err = service.ProcessMessage(context.Background(), nil, msg2)
	require.NoError(t, err)

	// Both should have their own highest bid
	bidKey1 := pool.HighestBidKey{Slot: 100, ParentBlockHash: parentHash1, ParentBlockRoot: parentRoot}
	bidKey2 := pool.HighestBidKey{Slot: 100, ParentBlockHash: parentHash2, ParentBlockRoot: parentRoot}
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

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	fcMock.ExecutionPayloadStatusMap[common.HexToHash("0xaaaa")] = execution_client.PayloadStatusValidated
	fcMock.Headers[common.HexToHash("0xbbbb")] = &cltypes.BeaconBlockHeader{}

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Verify stored in seen cache
	seenKey := seenBidKey{builderIndex: 1, slot: 100}
	require.True(t, service.seenCache.Contains(seenKey))

	// Verify stored in pool
	bidKey := pool.HighestBidKey{Slot: 100, ParentBlockHash: common.HexToHash("0xaaaa"), ParentBlockRoot: common.HexToHash("0xbbbb")}
	stored, found := epbsPool.HighestBids.Get(bidKey)
	require.True(t, found)
	require.Equal(t, msg, stored)
}

func TestExecutionPayloadBidServicePendingQueueCap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, _ := setupExecutionPayloadBidService(t, ctrl)

	// Fill the queue to the cap
	service.pendingCount.Store(maxPendingBids)

	msg := newTestSignedExecutionPayloadBid(100, 999, 1000)

	service.queuePendingBid(msg)

	// Should still be at cap — new item was rejected
	require.Equal(t, int32(maxPendingBids), service.pendingCount.Load())
	key := pendingBidKeyFor(msg)
	_, exists := service.pendingBids.Load(key)
	require.False(t, exists)
}

func TestExecutionPayloadBidServicePendingQueueDedupsSameBuilderSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, _ := setupExecutionPayloadBidService(t, ctrl)
	first := newTestSignedExecutionPayloadBid(100, 1, 1000)
	second := newTestSignedExecutionPayloadBid(100, 1, 1000)
	second.Signature[0] = 1

	service.queuePendingBid(first)
	service.queuePendingBid(first)
	service.queuePendingBid(second)

	require.Equal(t, int32(1), service.pendingCount.Load())
	_, firstExists := service.pendingBids.Load(pendingBidKeyFor(first))
	require.True(t, firstExists)
}

func TestExecutionPayloadBidServicePendingQueueCapConcurrent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, _ := setupExecutionPayloadBidService(t, ctrl)

	service.pendingCount.Store(maxPendingBids - 5)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			msg := newTestSignedExecutionPayloadBid(uint64(10000+idx), uint64(idx), 1000)
			service.queuePendingBid(msg)
		}(i)
	}
	wg.Wait()

	require.Equal(t, int32(maxPendingBids), service.pendingCount.Load())
	stored := 0
	service.pendingBids.Range(func(_, _ any) bool {
		stored++
		return true
	})
	require.Equal(t, 5, stored)
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
	require.Equal(t, original.Message.GasLimit, decoded.Message.GasLimit)
	require.Equal(t, original.Message.ParentBlockHash, decoded.Message.ParentBlockHash)
}

func TestExecutionPayloadBidServiceDecodeGossipMessageInvalid(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, _ := setupExecutionPayloadBidService(t, ctrl)

	_, err := service.DecodeGossipMessage("peer123", []byte{0x00, 0x01, 0x02}, clparams.GloasVersion)
	require.Error(t, err)
}

func TestExecutionPayloadBidServiceNonZeroExecutionPayment(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	msg.Message.ExecutionPayment = 500 // must be 0 at gossip time
	addPreferencesToPool(epbsPool, 100)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "execution_payment must be 0")
}

func TestExecutionPayloadBidServiceFeeRecipientMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	msg.Message.FeeRecipient = common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	addPreferencesToPool(epbsPool, 100)
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{}

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "fee_recipient")
	require.Contains(t, err.Error(), "does not match")
}

func TestExecutionPayloadBidServiceRejectsTooManyBlobCommitments(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, _, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	maxBlobs := int(service.beaconCfg.GetBlobParameters(100 / service.beaconCfg.SlotsPerEpoch).MaxBlobsPerBlock)
	for i := 0; i <= maxBlobs; i++ {
		msg.Message.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	}
	addPreferencesToPool(epbsPool, 100)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "too many blob_kzg_commitments")
}

func TestExecutionPayloadBidServiceRejectsPrevRandaoMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	parentState := newBidParentState(service.beaconCfg, testDependentRoot)
	parentState.SetRandaoMixAt(int(state2.Epoch(parentState)%service.beaconCfg.EpochsPerHistoricalVector), common.Hash{0x42})
	fcMock.StateAtBlockRootVal[msg.Message.ParentBlockRoot] = parentState
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{}
	addPreferencesToPool(epbsPool, 100)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "prev_randao")
}

func TestExecutionPayloadBidServiceRejectsBidAtParentSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{Slot: 100}

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not greater than parent block slot")
}

func TestExecutionPayloadBidServiceFailedValidationNotStored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	parentState := newBidParentState(service.beaconCfg, testDependentRoot)
	parentState.GetBuilders().Get(1).WithdrawableEpoch = 3
	fcMock.StateAtBlockRootVal[msg.Message.ParentBlockRoot] = parentState
	addPreferencesToPool(epbsPool, 100)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)

	// Should NOT be in seen cache
	seenKey := seenBidKey{builderIndex: 1, slot: 100}
	require.False(t, service.seenCache.Contains(seenKey))

	// Should NOT be in pool
	bidKey := pool.HighestBidKey{Slot: 100, ParentBlockHash: common.HexToHash("0xaaaa"), ParentBlockRoot: common.HexToHash("0xbbbb")}
	_, found := epbsPool.HighestBids.Get(bidKey)
	require.False(t, found)
}
