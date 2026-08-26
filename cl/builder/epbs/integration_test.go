package epbs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/builder/epbs/epbscfg"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/devgenesis"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

type testImportedBlockReader struct {
	block *cltypes.SignedBeaconBlock
}

type slotContextForkChoiceStub struct {
	state         *state.CachingBeaconState
	requestedRoot common.Hash
	dependentRoot common.Hash
}

func (s *slotContextForkChoiceStub) GetStateAtBlockRoot(root common.Hash, alwaysCopy bool) (*state.CachingBeaconState, error) {
	s.requestedRoot = root
	if !alwaysCopy {
		return s.state, nil
	}
	return s.state.Copy()
}

func (s *slotContextForkChoiceStub) Ancestor(common.Hash, uint64) forkchoice.ForkChoiceNode {
	return forkchoice.ForkChoiceNode{Root: s.dependentRoot}
}

func (*slotContextForkChoiceStub) ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	return nil, nil
}

func TestBuildSlotContextUsesAndAdvancesParentState(t *testing.T) {
	cfg := testBeaconCfg()
	cfg.SlotsPerEpoch = 8
	cfg.MinSeedLookahead = 1
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()
	parentState := state.New(cfg)
	parentState.SetVersion(clparams.GloasVersion)
	require.NoError(t, parentState.SetSlot(16))
	wantRandao := common.HexToHash("0x1234")
	require.NoError(t, parentState.SetRandaoMixAt(2, wantRandao))
	parentState.SetPayloadExpectedWithdrawals(solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44))
	parentRoot := common.HexToHash("0xaaaa")
	dependentRoot := common.HexToHash("0xbbbb")
	fc := &slotContextForkChoiceStub{state: parentState, dependentRoot: dependentRoot}

	sc, err := buildSlotContext(fc, cfg, 17, 123, forkchoice.ParentCandidate{
		Slot: 16, BlockRoot: parentRoot, ExecutionHash: common.HexToHash("0xcccc"),
	}, common.Bytes48{})
	require.NoError(t, err)
	require.Equal(t, parentRoot, fc.requestedRoot)
	require.Equal(t, uint64(16), parentState.Slot())
	require.Equal(t, dependentRoot, sc.DependentRoot)
	require.Equal(t, wantRandao, sc.PrevRandao)
}

func TestBuildSlotContextAppliesEpochRandaoReset(t *testing.T) {
	cfg := testBeaconCfg()
	cfg.SlotsPerEpoch = 8
	cfg.MinSeedLookahead = 1
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()
	parentState, _, err := devgenesis.BuildGenesisState("slot-context", 64, cfg, 0, common.Hash{})
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(parentState, 23))
	wantRandao := common.HexToHash("0x1234")
	require.NoError(t, parentState.SetRandaoMixAt(2, wantRandao))
	require.NoError(t, parentState.SetRandaoMixAt(3, common.HexToHash("0x9999")))
	parentState.SetPayloadExpectedWithdrawals(solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44))
	fc := &slotContextForkChoiceStub{state: parentState, dependentRoot: common.HexToHash("0xbbbb")}

	sc, err := buildSlotContext(fc, cfg, 24, 123, forkchoice.ParentCandidate{
		Slot: 23, BlockRoot: common.HexToHash("0xaaaa"),
	}, common.Bytes48{})
	require.NoError(t, err)
	require.Equal(t, wantRandao, sc.PrevRandao)
}

func TestBuildSlotContextUsesTargetParentBuilderBalance(t *testing.T) {
	cfg := testBeaconCfg()
	cfg.SlotsPerEpoch = 8
	cfg.MinSeedLookahead = 1
	cfg.GloasForkEpoch = 3
	cfg.InitializeForkSchedule()
	parentState, _, err := devgenesis.BuildGenesisState("slot-context-balance", 64, cfg, 0, common.Hash{})
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(parentState, 24))
	pubkey := common.Bytes48{1}
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, new(cltypes.Builder).EncodingSizeSSZ())
	builders.Append(&cltypes.Builder{
		Pubkey: pubkey, Balance: cfg.MinDepositAmount + 100,
		WithdrawableEpoch: cfg.FarFutureEpoch,
	})
	parentState.SetBuilders(builders)
	parentState.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: 1})
	withdrawals := solid.NewStaticListSSZ[*cltypes.BuilderPendingWithdrawal](
		int(cfg.BuilderPendingWithdrawalsLimit), new(cltypes.BuilderPendingWithdrawal).EncodingSizeSSZ(),
	)
	withdrawals.Append(&cltypes.BuilderPendingWithdrawal{BuilderIndex: 0, Amount: 60})
	parentState.SetBuilderPendingWithdrawals(withdrawals)
	parentState.SetLatestBlockHash(common.HexToHash("0x01"))
	_, _, found := builderStatusForPubkey(parentState, pubkey)
	require.True(t, found)
	fc := &slotContextForkChoiceStub{state: parentState, dependentRoot: common.HexToHash("0xbbbb")}

	sc, err := buildSlotContext(fc, cfg, 25, 123, forkchoice.ParentCandidate{
		Slot: 24, BlockRoot: common.HexToHash("0xaaaa"),
	}, pubkey)
	require.NoError(t, err)
	require.True(t, sc.BuilderFound)
	require.Equal(t, uint64(0), sc.BuilderIndex)
	require.Equal(t, uint64(40), sc.BuilderStatus.Balance)
}

func (r testImportedBlockReader) GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return r.block, r.block != nil
}

func TestInitBuilderService_Disabled(t *testing.T) {
	cfg := epbscfg.Config{Enabled: false}
	svc, err := InitBuilderService(cfg, BuilderDeps{})
	require.NoError(t, err)
	require.Nil(t, svc)
}

func TestInitBuilderService_RejectsInvalidConfig(t *testing.T) {
	_, err := InitBuilderService(epbscfg.Config{Enabled: true, KeyPath: "key", BidMargin: -1}, BuilderDeps{})
	require.ErrorContains(t, err, "bid margin")

	_, err = InitBuilderService(epbscfg.Config{Enabled: true, BidMargin: 0.5}, BuilderDeps{})
	require.ErrorContains(t, err, "key path")
}

func TestInitBuilderService_RejectsMissingDependencies(t *testing.T) {
	_, err := InitBuilderService(epbscfg.Config{Enabled: true, KeyPath: "key", BidMargin: 0.5}, BuilderDeps{})
	require.ErrorContains(t, err, "context is required")
}

func TestBuilderService_Shutdown_Nil(t *testing.T) {
	// Shutdown on nil should not panic.
	var svc *BuilderService
	svc.Shutdown()
}

func TestBuilderServiceShutdownDiscardsTrackedBuilds(t *testing.T) {
	loop, _, _, _ := setupBuilderLoop(t)
	require.NoError(t, loop.OnNewHead(t.Context(), testSlotContext()))
	svc := &BuilderService{Loop: loop, Manager: loop.manager}

	svc.Shutdown()
	svc.Shutdown()
	require.Empty(t, loop.speculativePayloads)
	require.Empty(t, loop.specBuild.builds)
}

func TestBuilderServiceShutdownWaitsBeforeReleasingReservations(t *testing.T) {
	loop, _, _, _ := setupBuilderLoop(t)
	require.True(t, loop.manager.ReserveBid(10))
	loop.pendingPayloads[pendingPayloadKey{slot: 1}] = &pendingPayload{slot: 1, bidValue: 10}
	svc := &BuilderService{Loop: loop, Manager: loop.manager}
	release := make(chan struct{})
	svc.run(func() { <-release })
	done := make(chan struct{})
	go func() {
		svc.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("shutdown returned before its worker stopped")
	case <-time.After(20 * time.Millisecond):
	}
	require.Equal(t, uint64(10), loop.manager.reservedBidValue)
	close(release)
	<-done
	require.Zero(t, loop.manager.reservedBidValue)
}

func TestBalanceStatus_Zero(t *testing.T) {
	status := BalanceStatus{}
	require.False(t, status.Active)
	require.Equal(t, uint64(0), status.Balance)
}

func TestHandleImportedBlock_RevealsExactWinningBid(t *testing.T) {
	cfg := testBeaconCfg()
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = 100
	block.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		Slot:            100,
		BuilderIndex:    42,
		ParentBlockHash: common.HexToHash("0x1111"),
		ParentBlockRoot: common.HexToHash("0x2222"),
		BlockHash:       common.HexToHash("0x3333"),
	}}
	blockRoot := common.HexToHash("0x4444")
	var called bool
	err := handleImportedBlock(context.Background(), &beaconevents.BlockData{Slot: 100, Block: blockRoot}, testImportedBlockReader{block: block}, func(_ context.Context, slot, builderIndex uint64, parentHash, parentRoot, blockHash, beaconRoot common.Hash) error {
		called = true
		require.Equal(t, uint64(100), slot)
		require.Equal(t, uint64(42), builderIndex)
		require.Equal(t, common.HexToHash("0x1111"), parentHash)
		require.Equal(t, common.HexToHash("0x2222"), parentRoot)
		require.Equal(t, common.HexToHash("0x3333"), blockHash)
		require.Equal(t, blockRoot, beaconRoot)
		return nil
	})
	require.NoError(t, err)
	require.True(t, called)
}

func TestRevealWinningBidUntilRecoveryOrDeadline(t *testing.T) {
	attempts := 0
	err := revealWinningBidUntil(t.Context(), time.Now().Add(time.Second), func(context.Context) error {
		attempts++
		if attempts <= 3 {
			return errors.New("temporarily unavailable")
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 4, attempts)

	attempts = 0
	err = revealWinningBidUntil(t.Context(), time.Now().Add(20*time.Millisecond), func(context.Context) error {
		attempts++
		return errors.New("permanent failure")
	})
	require.ErrorContains(t, err, "permanent failure")
	require.Positive(t, attempts)
}

func TestPayloadRevealDeadlineUsesConfiguredBasisPoints(t *testing.T) {
	cfg := testBeaconCfg()
	cfg.PayloadDueBps = 7500
	clock := eth_clock.NewEthereumClock(1000, common.Hash{}, cfg)
	require.Equal(t, time.Unix(1069, 0), payloadRevealDeadline(clock, cfg, 5))
}
