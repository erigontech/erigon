package services

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"time"

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
	ethClockMock.EXPECT().GenesisTime().Return(uint64(0)).AnyTimes()
	ethClockMock.EXPECT().GetSlotTime(gomock.Any()).DoAndReturn(func(slot uint64) time.Time {
		return time.Unix(int64(slot*12), 0)
	}).AnyTimes()
	fcMock := forkchoice_mock.NewForkChoiceStorageMock(t)
	epbsPool := pool.NewEpbsPool()
	beaconCfg := clparams.MainnetBeaconConfig
	beaconCfg.SlotsPerEpoch = 32
	beaconCfg.SlotsPerHistoricalRoot = 8192
	beaconCfg.MinSeedLookahead = 1
	beaconCfg.DomainBeaconBuilder = [4]byte{0x0B, 0x00, 0x00, 0x00}
	fcMock.StateAtBlockRootVal[common.HexToHash("0xbbbb")] = newBidParentState(&beaconCfg, testDependentRoot)
	fcMock.Headers[common.HexToHash("0xbbbb")] = &cltypes.BeaconBlockHeader{Slot: 99}
	fcMock.ExecutionPayloadGasLimitMap[common.HexToHash("0xaaaa")] = 30_000_000
	fcMock.Ancestors[63] = forkchoice.ForkChoiceNode{Root: testDependentRoot, PayloadStatus: cltypes.PayloadStatusPending}
	headRoot := common.HexToHash("0xeeee")
	headBlock := cltypes.NewBeaconBlock(&beaconCfg, clparams.GloasVersion)
	headBlock.ParentRoot = common.HexToHash("0xbbbb")
	headBlock.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		ParentBlockHash: common.HexToHash("0xaaaa"),
		BlockHash:       common.HexToHash("0xdddd"),
	}}
	fcMock.HeadVal = headRoot
	fcMock.Headers[headRoot] = &cltypes.BeaconBlockHeader{ParentRoot: headBlock.ParentRoot, Slot: 99}
	fcMock.Blocks[headRoot] = &cltypes.SignedBeaconBlock{Block: headBlock}
	prevBlsVerify := blsVerify
	blsVerify = func(_ []byte, _ []byte, _ []byte) (bool, error) { return true, nil }
	t.Cleanup(func() { blsVerify = prevBlsVerify })

	validationStateCache := lru.NewWithTTL[bidValidationStateKey, *bidValidationStateEntry](
		"bid_validation_states_test",
		bidValidationStateCacheSize,
		bidValidationStateCacheTTL(&beaconCfg),
	)

	service := &executionPayloadBidService{
		syncedDataManager: mockSyncedData,
		forkchoiceStore:   fcMock,
		ethClock:          ethClockMock,
		beaconCfg:         &beaconCfg,
		epbsPool:          epbsPool,
		emitters:          beaconevents.NewEventEmitter(),
		now: func() time.Time {
			return ethClockMock.GetSlotTime(ethClockMock.GetCurrentSlot())
		},
		seenCache:            newSeenBidStore(),
		validationStateCache: validationStateCache,
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
	if err := s.SetSlot(99); err != nil {
		panic(err)
	}
	if err := s.SetBlockRootAt(63, dependentRoot); err != nil {
		panic(err)
	}
	s.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: 2})
	for i := range 8 {
		var pk common.Bytes48
		pk[0] = byte(i)
		if err := s.AddValidator(solid.NewValidatorFromParameters(pk, common.Hash{}, 0, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch), cfg.EffectiveBalanceIncrement); err != nil {
			panic(err)
		}
	}
	s.SetPreviousEpochParticipationFlags(make(cltypes.ParticipationFlagsList, 8))
	s.SetCurrentEpochParticipationFlags(make(cltypes.ParticipationFlagsList, 8))
	s.SetProposerLookahead(solid.NewUint64VectorSSZ(int((cfg.MinSeedLookahead + 1) * cfg.SlotsPerEpoch)))
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, 73)
	for range 64 {
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

func TestExecutionPayloadBidServiceRejectsUnrepresentableSlotBeforeDependencies(t *testing.T) {
	for _, test := range []struct {
		name           string
		slot           uint64
		secondsPerSlot uint64
	}{
		{name: "slot addition overflow", slot: math.MaxUint64, secondsPerSlot: 12},
		{name: "unix second overflow", slot: math.MaxInt64/12 + 1, secondsPerSlot: 12},
		{name: "zero seconds per slot", slot: 100, secondsPerSlot: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			clock := eth_clock.NewMockEthereumClock(ctrl)
			clock.EXPECT().GenesisTime().Return(uint64(0)).AnyTimes()
			cfg := clparams.MainnetBeaconConfig
			cfg.SecondsPerSlot = test.secondsPerSlot
			stateCalls := 0
			fc := forkchoice_mock.NewForkChoiceStorageMock(t)
			fc.GetStateAtBlockRootFn = func(common.Hash, bool) (*state2.CachingBeaconState, error) {
				stateCalls++
				return nil, nil
			}
			service := &executionPayloadBidService{
				forkchoiceStore: fc,
				ethClock:        clock,
				beaconCfg:       &cfg,
				epbsPool:        pool.NewEpbsPool(),
				now:             func() time.Time { return time.Unix(0, 0) },
				seenCache:       newSeenBidStore(),
			}

			err := service.ProcessMessage(context.Background(), nil, newTestSignedExecutionPayloadBid(test.slot, 1, 1))
			require.ErrorIs(t, err, ErrIgnore)
			require.Zero(t, stateCalls)
		})
	}
}

func TestSafeSlotTimeRepresentabilityBoundaries(t *testing.T) {
	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GenesisTime().Return(uint64(7)).AnyTimes()
	cfg := clparams.MainnetBeaconConfig
	cfg.SecondsPerSlot = 12
	lastSlot := uint64((math.MaxInt64 - 7) / 12)
	wantUnixSeconds := uint64(7) + lastSlot*cfg.SecondsPerSlot

	got, ok := safeSlotTime(clock, &cfg, lastSlot)
	require.True(t, ok)
	require.Equal(t, time.Unix(int64(wantUnixSeconds), 0), got)
	_, ok = safeSlotTime(clock, &cfg, lastSlot+1)
	require.False(t, ok)
	_, ok = safeSlotTime(clock, &cfg, math.MaxUint64)
	require.False(t, ok)

	cfg.SecondsPerSlot = 0
	_, ok = safeSlotTime(clock, &cfg, 1)
	require.False(t, ok)
}

func TestExecutionPayloadBidServiceOrdersHighestBeforeStatelessChecks(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, ethClockMock, _, epbsPool := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1)
	msg.Message.ExecutionPayment = 10
	msg.Message.ExecutionPayment = 1
	epbsPool.HighestBids.Add(pool.HighestBidKey{
		Slot: msg.Message.Slot, ParentBlockHash: msg.Message.ParentBlockHash, ParentBlockRoot: msg.Message.ParentBlockRoot,
	}, newTestSignedExecutionPayloadBid(100, 2, 2))
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.ErrorIs(t, err, ErrIgnore)
	require.Contains(t, err.Error(), "not higher")
}

func TestExecutionPayloadBidServiceAuthenticatesAcceptedBidOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1)
	addPreferencesToPool(epbsPool, 100)
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	var calls int
	blsVerify = func(_, _, _ []byte) (bool, error) {
		calls++
		return true, nil
	}

	require.NoError(t, service.ProcessMessage(context.Background(), nil, msg))
	require.Equal(t, 1, calls)
}

func TestValidateDirectBidDoesNotApplyGossipHighestFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1)
	addPreferencesToPool(epbsPool, 100)
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	epbsPool.HighestBids.Add(pool.HighestBidKey{
		Slot: msg.Message.Slot, ParentBlockHash: msg.Message.ParentBlockHash, ParentBlockRoot: msg.Message.ParentBlockRoot,
	}, newTestSignedExecutionPayloadBid(100, 2, 2))
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100)).Times(2)

	require.NoError(t, service.ValidateBid(context.Background(), msg))
	require.Error(t, service.ProcessMessage(context.Background(), nil, msg))
}

func TestValidateDirectBidDoesNotRequireGossipProposerPreferences(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1)
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	require.Empty(t, epbsPool.ProposerPreferences.Keys())
	require.NoError(t, service.ValidateBid(context.Background(), msg))
}

func TestValidateDirectBidAllowsExecutionPayment(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, ethClockMock, fcMock, _ := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1)
	msg.Message.ExecutionPayment = 1
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	require.NoError(t, service.ValidateBid(context.Background(), msg))
}

func TestValidateDirectBidUsesFrozenParentWhenHeadFlipsToSibling(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, ethClockMock, fcMock, _ := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1)
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	siblingRoot := common.Hash{0xfa}
	fcMock.HeadVal = siblingRoot
	fcMock.Headers[siblingRoot] = &cltypes.BeaconBlockHeader{ParentRoot: common.Hash{0xfb}, Slot: 99}
	siblingBlock := cltypes.NewBeaconBlock(service.beaconCfg, clparams.GloasVersion)
	siblingBlock.Body.SignedExecutionPayloadBid.Message.ParentBlockHash = common.Hash{0xfc}
	siblingBlock.Body.SignedExecutionPayloadBid.Message.BlockHash = common.Hash{0xfd}
	fcMock.Blocks[siblingRoot] = &cltypes.SignedBeaconBlock{Block: siblingBlock}
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	require.NoError(t, service.ValidateBid(context.Background(), msg))
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

func TestIsCurrentOrNextSlotClockDisparityBoundaries(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, _, _, _ := setupExecutionPayloadBidService(t, ctrl)
	lower := service.ethClock.GetSlotTime(99).Add(-gloasMaximumClockDisparity)
	upper := service.ethClock.GetSlotTime(101).Add(gloasMaximumClockDisparity)

	tests := []struct {
		name string
		now  time.Time
		want bool
	}{
		{name: "before lower", now: lower.Add(-time.Millisecond), want: false},
		{name: "at lower", now: lower, want: true},
		{name: "after lower", now: lower.Add(time.Millisecond), want: true},
		{name: "before upper", now: upper.Add(-time.Millisecond), want: true},
		{name: "at upper", now: upper, want: true},
		{name: "after upper", now: upper.Add(time.Millisecond), want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, isCurrentOrNextSlot(service.ethClock, service.beaconCfg, test.now, 100, gloasMaximumClockDisparity))
		})
	}
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
	require.Equal(t, uint64(99), parentState.Slot())
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
	require.Zero(t, service.validationStateCache.Len())
}

func TestExecutionPayloadBidServiceRejectsNonZeroExecutionPaymentWithMissingStateBeforeQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, _ := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	msg.Message.ExecutionPayment = 1
	delete(fcMock.StateAtBlockRootVal, msg.Message.ParentBlockRoot)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)

	require.Error(t, err)
	require.Contains(t, err.Error(), "execution_payment must be 0")
}

func TestExecutionPayloadBidServiceRejectsTooManyBlobCommitmentsWithMissingStateBeforeQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, _ := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	delete(fcMock.StateAtBlockRootVal, msg.Message.ParentBlockRoot)
	maxBlobs := int(service.beaconCfg.GetBlobParameters(100 / service.beaconCfg.SlotsPerEpoch).MaxBlobsPerBlock)
	for i := 0; i <= maxBlobs; i++ {
		msg.Message.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	}

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)

	require.Error(t, err)
	require.Contains(t, err.Error(), "too many blob_kzg_commitments")
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
	err := service.ProcessMessage(context.Background(), nil, msg)
	require.ErrorIs(t, err, ErrIgnore)

	addPreferencesToPool(epbsPool, 100)
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{}
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	require.NoError(t, service.ProcessMessage(context.Background(), nil, msg))
	_, found := epbsPool.HighestBids.Get(pool.HighestBidKey{Slot: 100, ParentBlockHash: msg.Message.ParentBlockHash, ParentBlockRoot: msg.Message.ParentBlockRoot})
	require.True(t, found)
}

func TestExecutionPayloadBidServiceMissingParentStateIsNotQueued(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{}
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	delete(fcMock.StateAtBlockRootVal, msg.Message.ParentBlockRoot)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	err := service.ProcessMessage(context.Background(), nil, msg)
	require.ErrorIs(t, err, ErrIgnore)
	_, found := epbsPool.HighestBids.Get(pool.HighestBidKey{Slot: 100, ParentBlockHash: msg.Message.ParentBlockHash, ParentBlockRoot: msg.Message.ParentBlockRoot})
	require.False(t, found)
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
	require.Equal(t, uint64(99), parentState.Slot())
	_, found := epbsPool.HighestBids.Get(pool.HighestBidKey{Slot: 100, ParentBlockHash: msg.Message.ParentBlockHash, ParentBlockRoot: msg.Message.ParentBlockRoot})
	require.True(t, found)
}

func TestExecutionPayloadBidServiceUsesGenesisDependentRootInEarlyEpoch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(1, 1, 1000)
	genesisRoot := common.HexToHash("0x1234")
	fcMock.Ancestors[0] = forkchoice.ForkChoiceNode{Root: genesisRoot}
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{}
	fcMock.StateAtBlockRootVal[msg.Message.ParentBlockRoot] = newBidParentState(service.beaconCfg, genesisRoot)
	require.NoError(t, fcMock.StateAtBlockRootVal[msg.Message.ParentBlockRoot].SetSlot(0))
	fcMock.StateAtBlockRootVal[msg.Message.ParentBlockRoot].GetBuilders().Get(1).DepositEpoch = 0
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	addPreferencesToPoolWithRoot(epbsPool, 1, genesisRoot)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(1))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)
}

func TestBidCompatibleWithHead(t *testing.T) {
	headRoot := common.HexToHash("0x10")
	parentRoot := common.HexToHash("0x20")
	parentPayload := common.HexToHash("0x30")
	headPayload := common.HexToHash("0x40")
	headHeader := &cltypes.BeaconBlockHeader{ParentRoot: parentRoot, Slot: 99}
	headBid := &cltypes.ExecutionPayloadBid{ParentBlockHash: parentPayload, BlockHash: headPayload}

	buildsOnParent := &cltypes.ExecutionPayloadBid{Slot: 100, ParentBlockRoot: parentRoot, ParentBlockHash: parentPayload}
	require.True(t, forkchoice.BidCompatibleWithHead(buildsOnParent, headRoot, headHeader, headBid, true))
	buildsOnHead := &cltypes.ExecutionPayloadBid{Slot: 100, ParentBlockRoot: headRoot, ParentBlockHash: headPayload}
	require.True(t, forkchoice.BidCompatibleWithHead(buildsOnHead, headRoot, headHeader, headBid, true))
	require.False(t, forkchoice.BidCompatibleWithHead(buildsOnHead, headRoot, headHeader, headBid, false))
	stale := &cltypes.ExecutionPayloadBid{Slot: 100, ParentBlockRoot: common.HexToHash("0x50"), ParentBlockHash: parentPayload}
	require.False(t, forkchoice.BidCompatibleWithHead(stale, headRoot, headHeader, headBid, false))
}

func TestExecutionPayloadBidServiceFirstGloasSlotBuildsOnFuluHead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	service, _, _, fc, _ := setupExecutionPayloadBidService(t, ctrl)
	headRoot := common.HexToHash("0xf001")
	payloadHash := common.HexToHash("0xf002")
	headBlock := cltypes.NewBeaconBlock(service.beaconCfg, clparams.FuluVersion)
	headBlock.Body.ExecutionPayload.BlockHash = payloadHash
	fc.HeadVal = headRoot
	fc.Headers[headRoot] = &cltypes.BeaconBlockHeader{Slot: 99}
	fc.Blocks[headRoot] = &cltypes.SignedBeaconBlock{Block: headBlock}

	compatible, err := service.isBidCompatibleWithHead(&cltypes.ExecutionPayloadBid{
		Slot: 100, ParentBlockRoot: headRoot, ParentBlockHash: payloadHash,
	})
	require.NoError(t, err)
	require.True(t, compatible)
}

func TestExecutionPayloadBidServiceUsesCoherentHeadNodeSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	service, _, _, fc, _ := setupExecutionPayloadBidService(t, ctrl)
	headRoot := fc.HeadVal
	fc.GetHeadNodeFn = func() (forkchoice.ForkChoiceNode, error) {
		fc.HeadVal = common.HexToHash("0xdead")
		return forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusFull}, nil
	}
	compatible, err := service.isBidCompatibleWithHead(&cltypes.ExecutionPayloadBid{
		Slot: 100, ParentBlockRoot: headRoot, ParentBlockHash: common.HexToHash("0xdddd"),
	})
	require.NoError(t, err)
	require.True(t, compatible)
}

func TestSeenBidKeyIncludesParentTuple(t *testing.T) {
	bid1 := newTestSignedExecutionPayloadBid(100, 1, 1000).Message
	bid2 := newTestSignedExecutionPayloadBid(100, 1, 1001).Message
	bid2.ParentBlockRoot = common.HexToHash("0xdddd")
	require.NotEqual(t, newSeenBidKey(bid1), newSeenBidKey(bid2))

	bid2.ParentBlockRoot = bid1.ParentBlockRoot
	bid2.ParentBlockHash = common.HexToHash("0xdddd")
	require.NotEqual(t, newSeenBidKey(bid1), newSeenBidKey(bid2))

	bid2.ParentBlockHash = bid1.ParentBlockHash
	bid2.BuilderIndex++
	require.NotEqual(t, newSeenBidKey(bid1), newSeenBidKey(bid2))
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

func TestExecutionPayloadBidServiceKnownPayloadWithoutGasLimitIsIgnored(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	delete(fcMock.ExecutionPayloadGasLimitMap, msg.Message.ParentBlockHash)
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))
	var blsCalls int
	blsVerify = func(_, _, _ []byte) (bool, error) {
		blsCalls++
		return true, nil
	}

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.ErrorIs(t, err, ErrIgnore)
	require.Contains(t, err.Error(), "gas limit")
	require.Zero(t, blsCalls)
	require.Zero(t, service.validationStateCache.Len())
}

func TestExecutionPayloadBidServiceHeadUnavailableDoesNotFetchValidationState(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	addPreferencesToPool(epbsPool, 100)
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	fcMock.GetHeadNodeFn = func() (forkchoice.ForkChoiceNode, error) {
		return forkchoice.ForkChoiceNode{}, errors.New("head unavailable")
	}
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.ErrorIs(t, err, ErrIgnore)
	require.Contains(t, err.Error(), "head unavailable")
	require.Zero(t, service.validationStateCache.Len())
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
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	addPreferencesToPool(epbsPool, 100)

	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bid validation failed")
	require.Contains(t, err.Error(), "not active")

	// Should NOT be marked as seen
	seenKey := newSeenBidKey(msg.Message)
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
	delete(fcMock.Headers, msg.Message.ParentBlockRoot)
	delete(fcMock.StateAtBlockRootVal, msg.Message.ParentBlockRoot)

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

func TestExecutionPayloadBidServiceStoreValidBidDoesNotOverwriteHigherBid(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, epbsPool := setupExecutionPayloadBidService(t, ctrl)
	service.now = func() time.Time { return time.Unix(100*12, 0) }
	high := newTestSignedExecutionPayloadBid(100, 1, 2000)
	low := newTestSignedExecutionPayloadBid(100, 2, 500)

	require.NoError(t, service.storeValidBidAt(high, time.Time{}))
	err := service.storeValidBidAt(low, time.Time{})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))

	bidKey := pool.HighestBidKey{Slot: 100, ParentBlockHash: high.Message.ParentBlockHash, ParentBlockRoot: high.Message.ParentBlockRoot}
	stored, found := epbsPool.HighestBids.Get(bidKey)
	require.True(t, found)
	require.Equal(t, high, stored)
	require.False(t, service.seenCache.Contains(newSeenBidKey(low.Message)))
}

func TestExecutionPayloadBidServiceSeenBidsRetainsEveryBidInGossipWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, _ := setupExecutionPayloadBidService(t, ctrl)
	service.now = func() time.Time { return time.Unix(100*12, 0) }

	const builders = 576
	for builderIndex := range uint64(builders) {
		bid := newTestSignedExecutionPayloadBid(100, builderIndex, 1000)
		bid.Message.ParentBlockHash[0] = byte(builderIndex)
		bid.Message.ParentBlockHash[1] = byte(builderIndex >> 8)
		bid.Message.ParentBlockRoot[0] = byte(builderIndex)
		bid.Message.ParentBlockRoot[1] = byte(builderIndex >> 8)
		require.NoError(t, service.storeValidBidAt(bid, service.now()))
	}

	first := newTestSignedExecutionPayloadBid(100, 0, 1000)
	require.True(t, service.seenCache.Contains(newSeenBidKey(first.Message)))
	firstKey := pool.HighestBidKey{Slot: 100, ParentBlockHash: first.Message.ParentBlockHash, ParentBlockRoot: first.Message.ParentBlockRoot}
	_, found := service.epbsPool.HighestBids.Get(firstKey)
	require.True(t, found)
	lower := newTestSignedExecutionPayloadBid(100, builders+1, 999)
	err := service.storeValidBidAt(lower, service.now())
	require.ErrorIs(t, err, ErrIgnore)
	stored, found := service.epbsPool.HighestBids.Get(firstKey)
	require.True(t, found)
	require.Equal(t, uint64(1000), stored.Message.Value)
}

func TestExecutionPayloadBidServiceSeenBidsPrunesAfterGossipWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _, _, _ := setupExecutionPayloadBidService(t, ctrl)
	boundary := time.Unix(101*12, 0).Add(gloasMaximumClockDisparity)
	service.now = func() time.Time { return boundary }
	stale := newTestSignedExecutionPayloadBid(100, 1, 1000)
	require.NoError(t, service.storeValidBidAt(stale, service.now()))
	require.True(t, service.seenCache.Contains(newSeenBidKey(stale.Message)))

	service.now = func() time.Time { return boundary.Add(time.Nanosecond) }
	trigger := newTestSignedExecutionPayloadBid(101, 2, 1000)
	trigger.Message.ParentBlockHash = common.Hash{2}
	trigger.Message.ParentBlockRoot = common.Hash{3}
	require.NoError(t, service.storeValidBidAt(trigger, service.now()))
	require.False(t, service.seenCache.Contains(newSeenBidKey(stale.Message)))
	_, found := service.epbsPool.HighestBids.Get(pool.HighestBidKey{
		Slot:            stale.Message.Slot,
		ParentBlockHash: stale.Message.ParentBlockHash,
		ParentBlockRoot: stale.Message.ParentBlockRoot,
	})
	require.False(t, found)
}

func TestExecutionPayloadBidServiceRetainsPreferencesThroughBidWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	service, _, _, fc, epbsPool := setupExecutionPayloadBidService(t, ctrl)
	bid := newTestSignedExecutionPayloadBid(100, 1, 1000)
	dependentRoot := fc.Ancestors[63].Root
	preferences := &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot:  bid.Message.Slot,
		DependentRoot: dependentRoot,
	}}
	epbsPool.ProposerPreferences.Add(pool.ProposerPreferencesKey{Slot: bid.Message.Slot, DependentRoot: dependentRoot}, preferences)
	now := time.Unix(100*12, 0).Add(gloasMaximumClockDisparity + time.Nanosecond)
	epbsPool.ProposerPreferences.PruneSlots(func(entrySlot uint64) bool {
		return isPastBidWindow(service.ethClock, service.beaconCfg, now, entrySlot)
	})

	matched, ok, err := service.matchingProposerPreferences(bid)
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, preferences, matched)
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
}

func TestExecutionPayloadBidServiceAcceptsSameBuilderAtSameSlotForDifferentParent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock, fcMock, epbsPool := setupExecutionPayloadBidService(t, ctrl)

	parentHash1 := common.HexToHash("0xaaaa")
	parentHash2 := common.HexToHash("0xdddd")
	parentRoot1 := common.HexToHash("0xbbbb")
	parentRoot2 := common.HexToHash("0xeeee")

	fcMock.ExecutionPayloadStatusMap[parentHash1] = execution_client.PayloadStatusValidated
	fcMock.ExecutionPayloadStatusMap[parentHash2] = execution_client.PayloadStatusValidated
	fcMock.ExecutionPayloadGasLimitMap[parentHash2] = 30_000_000
	fcMock.Headers[parentRoot1] = &cltypes.BeaconBlockHeader{Slot: 99}
	fcMock.StateAtBlockRootVal[parentRoot2] = newBidParentState(service.beaconCfg, testDependentRoot)

	addPreferencesToPool(epbsPool, 100)

	msg1 := newTestSignedExecutionPayloadBid(100, 1, 1000)
	msg1.Message.ParentBlockHash = parentHash1
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg1)
	require.NoError(t, err)

	msg2 := newTestSignedExecutionPayloadBid(100, 1, 500)
	msg2.Message.ParentBlockHash = parentHash2
	msg2.Message.ParentBlockRoot = parentRoot2
	ethClockMock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err = service.ProcessMessage(context.Background(), nil, msg2)
	require.NoError(t, err)

	bidKey1 := pool.HighestBidKey{Slot: 100, ParentBlockHash: parentHash1, ParentBlockRoot: parentRoot1}
	bidKey2 := pool.HighestBidKey{Slot: 100, ParentBlockHash: parentHash2, ParentBlockRoot: parentRoot2}
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
	seenKey := newSeenBidKey(msg.Message)
	require.True(t, service.seenCache.Contains(seenKey))

	// Verify stored in pool
	bidKey := pool.HighestBidKey{Slot: 100, ParentBlockHash: common.HexToHash("0xaaaa"), ParentBlockRoot: common.HexToHash("0xbbbb")}
	stored, found := epbsPool.HighestBids.Get(bidKey)
	require.True(t, found)
	require.Equal(t, msg, stored)
}

func TestExecutionPayloadBidServiceRejectsWhenPreferencesAreMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	service, _, ethClock, _, _ := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.ErrorIs(t, err, ErrIgnore)
	require.Contains(t, err.Error(), "proposer preferences not available")
}

func TestExecutionPayloadBidServiceRejectsInvalidSignatureVariantsBeforeValidBid(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClock, fc, epbsPool := setupExecutionPayloadBidService(t, ctrl)
	valid := newTestSignedExecutionPayloadBid(100, 1, 1000)
	blsVerify = func(signature, _, _ []byte) (bool, error) { return signature[0] == 0, nil }
	fc.Headers[valid.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{Slot: 99}
	fc.ExecutionPayloadStatusMap[valid.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
	addPreferencesToPool(epbsPool, 100)
	const invalidVariants = 5
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(100)).Times(invalidVariants + 1)

	for i := range invalidVariants {
		invalid := newTestSignedExecutionPayloadBid(100, 1, 1000)
		invalid.Signature[0] = byte(i + 1)
		err := service.ProcessMessage(context.Background(), nil, invalid)
		require.ErrorContains(t, err, "invalid builder signature")
	}
	require.NoError(t, service.ProcessMessage(context.Background(), nil, valid))

	stored, ok := epbsPool.HighestBids.Get(pool.HighestBidKey{
		Slot:            valid.Message.Slot,
		ParentBlockHash: valid.Message.ParentBlockHash,
		ParentBlockRoot: valid.Message.ParentBlockRoot,
	})
	require.True(t, ok)
	require.Equal(t, valid.Signature, stored.Signature)
}

func TestExecutionPayloadBidServiceRejectsNonAdvancingKnownParentBeforeQueue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	service, _, ethClock, fc, _ := setupExecutionPayloadBidService(t, ctrl)
	msg := newTestSignedExecutionPayloadBid(100, 1, 1000)
	fc.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{Slot: 100}
	require.NoError(t, fc.StateAtBlockRootVal[msg.Message.ParentBlockRoot].SetSlot(100))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(100))

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
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

func TestExecutionPayloadBidServiceDecodeGossipMessageRejectsNonCanonicalOffsets(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, _, _, _ := setupExecutionPayloadBidService(t, ctrl)
	encoded, err := newTestSignedExecutionPayloadBid(100, 1, 1000).EncodeSSZ(nil)
	require.NoError(t, err)
	const signedFixedSize = 100
	const bidFixedSize = 224
	const commitmentsOffsetPosition = 188
	nonCanonical := append([]byte(nil), encoded[:signedFixedSize+bidFixedSize]...)
	nonCanonical = append(nonCanonical, make([]byte, 4)...)
	nonCanonical = append(nonCanonical, encoded[signedFixedSize+bidFixedSize:]...)
	offset := signedFixedSize + commitmentsOffsetPosition
	binary.LittleEndian.PutUint32(nonCanonical[offset:], binary.LittleEndian.Uint32(encoded[offset:])+4)

	_, err = service.DecodeGossipMessage("peer123", nonCanonical, clparams.GloasVersion)
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
	require.ErrorIs(t, err, ErrIgnore)
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
	require.NoError(t, parentState.SetRandaoMixAt(int(state2.Epoch(parentState)%service.beaconCfg.EpochsPerHistoricalVector), common.Hash{0x42}))
	fcMock.StateAtBlockRootVal[msg.Message.ParentBlockRoot] = parentState
	fcMock.Headers[msg.Message.ParentBlockRoot] = &cltypes.BeaconBlockHeader{}
	fcMock.ExecutionPayloadStatusMap[msg.Message.ParentBlockHash] = execution_client.PayloadStatusValidated
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
	seenKey := newSeenBidKey(msg.Message)
	require.False(t, service.seenCache.Contains(seenKey))

	// Should NOT be in pool
	bidKey := pool.HighestBidKey{Slot: 100, ParentBlockHash: common.HexToHash("0xaaaa"), ParentBlockRoot: common.HexToHash("0xbbbb")}
	_, found := epbsPool.HighestBids.Get(bidKey)
	require.False(t, found)
}
