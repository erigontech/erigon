package epbs

import (
	"context"
	"errors"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/builder/epbs/epbscfg"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/devgenesis"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

type testImportedBlockReader struct {
	block *cltypes.SignedBeaconBlock
}

type slotContextForkChoiceStub struct {
	state          *state.CachingBeaconState
	requestedRoot  common.Hash
	dependentRoot  common.Hash
	parentGasLimit uint64
	envelope       *cltypes.SignedExecutionPayloadEnvelope
}

func (s *slotContextForkChoiceStub) GetExecutionPayloadGasLimit(common.Hash) (uint64, bool) {
	if s.parentGasLimit == 0 {
		return 30_000_000, true
	}
	return s.parentGasLimit, true
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

func (s *slotContextForkChoiceStub) ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	return s.envelope, nil
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

func TestBuildSlotContextUsesGenesisDependentRootInEarlyEpoch(t *testing.T) {
	cfg := testBeaconCfg()
	cfg.SlotsPerEpoch = 8
	cfg.MinSeedLookahead = 1
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()
	parentState := state.New(cfg)
	parentState.SetVersion(clparams.GloasVersion)
	require.NoError(t, parentState.SetSlot(0))
	parentState.SetPayloadExpectedWithdrawals(solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44))
	parentRoot := common.HexToHash("0xaaaa")
	dependentRoot := common.HexToHash("0xbbbb")
	fc := &slotContextForkChoiceStub{state: parentState, dependentRoot: dependentRoot}

	sc, err := buildSlotContext(fc, cfg, 1, 123, forkchoice.ParentCandidate{
		Slot: 0, BlockRoot: parentRoot, ExecutionHash: common.HexToHash("0xcccc"),
	}, common.Bytes48{})
	require.NoError(t, err)
	require.Equal(t, dependentRoot, sc.DependentRoot)
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

func TestBuildSlotContextUsesGossipValidationBuilderBalance(t *testing.T) {
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

func TestBuildSlotContextDoesNotApplyFullParentRequestsToGossipEligibility(t *testing.T) {
	cfg := testBeaconCfg()
	cfg.SlotsPerEpoch = 8
	cfg.MinSeedLookahead = 1
	cfg.GloasForkEpoch = 3
	cfg.InitializeForkSchedule()
	parentState, _, err := devgenesis.BuildGenesisState("slot-context-full-parent", 64, cfg, 0, common.Hash{})
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(parentState, 24))
	pubkey := common.Bytes48{1}
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, new(cltypes.Builder).EncodingSizeSSZ())
	builders.Append(&cltypes.Builder{
		Pubkey: pubkey, Balance: cfg.MinDepositAmount + 100, Version: cfg.PayloadBuilderVersion,
		WithdrawableEpoch: cfg.FarFutureEpoch,
	})
	parentState.SetBuilders(builders)
	parentState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{Slot: 24})
	requests := cltypes.NewExecutionRequestsWithVersion(cfg, clparams.GloasVersion)
	requests.BuilderDeposits.Append(&solid.BuilderDepositRequest{
		PubKey: pubkey, WithdrawalCredentials: common.Hash{byte(cfg.BuilderWithdrawalPrefix)}, Amount: 25,
	})
	fc := &slotContextForkChoiceStub{
		state: parentState, dependentRoot: common.HexToHash("0xbbbb"),
		envelope: &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
			ExecutionRequests: requests,
		}},
	}

	sc, err := buildSlotContext(fc, cfg, 25, 123, forkchoice.ParentCandidate{
		Slot: 24, BlockRoot: common.HexToHash("0xaaaa"), ShouldExtend: true,
	}, pubkey)
	require.NoError(t, err)
	require.Equal(t, uint64(100), sc.BuilderStatus.Balance)
}

func TestBuildSlotContextDoesNotUseNonPayloadBuilderVersion(t *testing.T) {
	cfg := testBeaconCfg()
	cfg.SlotsPerEpoch = 8
	cfg.GloasForkEpoch = 3
	cfg.InitializeForkSchedule()
	parentState, _, err := devgenesis.BuildGenesisState("slot-context-builder-version", 64, cfg, 0, common.Hash{})
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(parentState, 24))
	pubkey := common.Bytes48{1}
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, new(cltypes.Builder).EncodingSizeSSZ())
	builders.Append(&cltypes.Builder{
		Pubkey: pubkey, Balance: cfg.MinDepositAmount + 100,
		WithdrawableEpoch: cfg.FarFutureEpoch,
		Version:           cfg.PayloadBuilderVersion + 1,
	})
	parentState.SetBuilders(builders)
	parentState.SetLatestBlockHash(common.HexToHash("0x01"))
	fc := &slotContextForkChoiceStub{state: parentState, dependentRoot: common.HexToHash("0xbbbb")}

	sc, err := buildSlotContext(fc, cfg, 25, 123, forkchoice.ParentCandidate{
		Slot: 24, BlockRoot: common.HexToHash("0xaaaa"),
	}, pubkey)
	require.NoError(t, err)
	require.False(t, sc.BuilderFound)
}

func TestEmptyCanonicalHeadRetainsWinnerUntilImportedReveal(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	sc := testSlotContext()
	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	require.NoError(t, loop.OnNewHead(t.Context(), sc))
	prefsWatch.OnPreferencesReceived(sc.Slot, &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: sc.Slot, TargetGasLimit: 30_000_000,
	}})
	require.NoError(t, loop.OnSlot(t.Context(), sc))

	cfg := loop.beaconCfg
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()
	parentState, _, err := devgenesis.BuildGenesisState("empty-canonical-head", 64, cfg, 0, common.Hash{})
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(parentState, sc.Slot))
	parentState.SetVersion(clparams.GloasVersion)
	parentState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		Slot: sc.Slot, ParentBlockHash: sc.Parent.ExecutionHash, BlockHash: common.HexToHash("0xb10c"),
	})
	canonicalRoot := common.HexToHash("0xa1")
	fc := &slotContextForkChoiceStub{state: parentState, dependentRoot: common.HexToHash("0xbbbb")}
	next, err := buildSlotContext(fc, cfg, sc.Slot+1, sc.Timestamp+cfg.SecondsPerSlot, forkchoice.ParentCandidate{
		Slot: sc.Slot, BlockRoot: canonicalRoot, ExecutionHash: sc.Parent.ExecutionHash, ShouldExtend: false,
	}, loop.manager.Pubkey())
	require.NoError(t, err)
	require.Equal(t, common.HexToHash("0xb10c"), next.Parent.PayloadBlockHash)
	require.NoError(t, loop.OnNewHead(t.Context(), next))
	require.Len(t, loop.pendingPayloads, 1)
	require.NoError(t, loop.OnBidWon(
		t.Context(), sc.Slot, 42, sc.Parent.ExecutionHash, sc.Parent.BlockRoot,
		common.HexToHash("0xb10c"), canonicalRoot,
	))
	require.Len(t, submitter.broadcasts, 1)

	parentState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
		Slot: sc.Slot, ParentBlockHash: sc.Parent.ExecutionHash, BlockHash: common.HexToHash("0x9999"),
	})
	replacement, err := buildSlotContext(fc, cfg, sc.Slot+1, sc.Timestamp+cfg.SecondsPerSlot, forkchoice.ParentCandidate{
		Slot: sc.Slot, BlockRoot: common.HexToHash("0xb2"), ExecutionHash: sc.Parent.ExecutionHash, ShouldExtend: false,
	}, loop.manager.Pubkey())
	require.NoError(t, err)
	require.NoError(t, loop.OnNewHead(t.Context(), replacement))
	require.Len(t, loop.pendingPayloads, 1)
	replacement.FinalizedSlot = sc.Slot + 1
	require.NoError(t, loop.OnNewHead(t.Context(), replacement))
	require.Empty(t, loop.pendingPayloads)
}

func (r testImportedBlockReader) GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return r.block, r.block != nil
}

type importedBlockWatcherReaderStub struct {
	mu        sync.RWMutex
	head      common.Hash
	finalized solid.Checkpoint
	headers   map[common.Hash]*cltypes.BeaconBlockHeader
	blocks    map[common.Hash]*cltypes.SignedBeaconBlock
	lookups   chan struct{}
}

func (r *importedBlockWatcherReaderStub) GetHeadNode() (forkchoice.ForkChoiceNode, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return forkchoice.ForkChoiceNode{Root: r.head}, nil
}

func (r *importedBlockWatcherReaderStub) GetHeader(root common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	header, ok := r.headers[root]
	return header, ok
}

func (r *importedBlockWatcherReaderStub) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.lookups != nil {
		select {
		case r.lookups <- struct{}{}:
		default:
		}
	}
	block, ok := r.blocks[root]
	return block, ok
}

func (r *importedBlockWatcherReaderStub) Ancestor(root common.Hash, slot uint64) forkchoice.ForkChoiceNode {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for root != (common.Hash{}) {
		header, ok := r.headers[root]
		if !ok || header == nil {
			return forkchoice.ForkChoiceNode{}
		}
		if header.Slot <= slot {
			return forkchoice.ForkChoiceNode{Root: root}
		}
		root = header.ParentRoot
	}
	return forkchoice.ForkChoiceNode{}
}

func (r *importedBlockWatcherReaderStub) FinalizedCheckpoint() solid.Checkpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.finalized
}

func (r *importedBlockWatcherReaderStub) setBlock(root common.Hash, block *cltypes.SignedBeaconBlock) {
	r.mu.Lock()
	r.blocks[root] = block
	r.mu.Unlock()
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

func TestInitBuilderServiceRejectsZeroSlotsPerEpoch(t *testing.T) {
	beaconCfg := testBeaconCfg()
	beaconCfg.SlotsPerEpoch = 0
	_, err := InitBuilderService(epbscfg.Config{Enabled: true, KeyPath: "key", BidMargin: 0.5}, BuilderDeps{
		Ctx: t.Context(), BeaconCfg: beaconCfg,
	})
	require.ErrorContains(t, err, "slots per epoch")
}

func TestFinalizedPayloadPruneSlotFailsSafe(t *testing.T) {
	maxEpoch := uint64(math.MaxUint64) / 32
	require.Equal(t, uint64(96), finalizedPayloadPruneSlot(3, 32))
	require.Zero(t, finalizedPayloadPruneSlot(3, 0))
	require.Zero(t, finalizedPayloadPruneSlot(maxEpoch+1, 32))
	require.Equal(t, maxEpoch*32, finalizedPayloadPruneSlot(maxEpoch, 32))
}

func TestInitBuilderServiceRetainsUnfinalizedCandidatesBehindWallClock(t *testing.T) {
	beaconCfg := testBeaconCfg()
	beaconCfg.GloasForkEpoch = 0
	beaconCfg.PayloadDueBps = 5000
	beaconCfg.InitializeForkSchedule()
	const (
		headSlot    = uint64(111)
		currentSlot = uint64(129)
	)
	slotDuration := time.Duration(beaconCfg.SecondsPerSlot) * time.Second
	genesisTime := uint64(time.Now().Add(-time.Duration(currentSlot)*slotDuration - 3*slotDuration/4).Unix())
	clock := eth_clock.NewEthereumClock(genesisTime, common.Hash{}, beaconCfg)
	require.Equal(t, currentSlot, clock.GetCurrentSlot())
	require.False(t, time.Now().Before(payloadRevealDeadline(clock, beaconCfg, currentSlot)))

	keyBytes := make([]byte, 32)
	for i := range keyBytes {
		keyBytes[i] = byte(i + 1)
	}
	keyPath := filepath.Join(t.TempDir(), "builder.key")
	require.NoError(t, os.WriteFile(keyPath, keyBytes, 0600))
	signer, err := NewLocalSignerFromBytes(keyBytes)
	require.NoError(t, err)
	store := newFilePendingPayloadStore(t.TempDir(), beaconCfg)
	savePending := func(slot uint64, root, blockHash common.Hash) pendingPayloadKey {
		key := pendingPayloadKey{
			slot: slot, parentBlockHash: common.HexToHash("0xdead"),
			parentBlockRoot: root, blockHash: blockHash,
		}
		assembled := makeTestPayload(t, big.NewInt(1))
		assembled.Eth1Block.SlotNumber = slot
		assembled.Eth1Block.BlockHash = blockHash
		parent := testParentInfo()
		parent.BlockRoot = root
		pending := &pendingPayload{
			slot: slot, builderIndex: 42, bidValue: 1, parent: parent, assembled: assembled,
			execReqs: cltypes.NewExecutionRequestsWithVersion(beaconCfg, clparams.GloasVersion),
		}
		require.NoError(t, store.Save(t.Context(), key, pending, signer.Pubkey()))
		return key
	}
	finalizedEpochStart := uint64(96)
	boundarySlots := []uint64{
		finalizedEpochStart - 1,
		finalizedEpochStart,
		finalizedEpochStart + 1,
		headSlot,
		finalizedEpochStart + beaconCfg.SlotsPerEpoch - 1,
		finalizedEpochStart + beaconCfg.SlotsPerEpoch,
	}
	keys := make(map[uint64]pendingPayloadKey, len(boundarySlots))
	for _, slot := range boundarySlots {
		root := common.BigToHash(new(big.Int).SetUint64(slot + 1_000))
		blockHash := common.BigToHash(new(big.Int).SetUint64(slot + 2_000))
		if slot == headSlot {
			root = common.HexToHash("0xbeef")
			blockHash = common.HexToHash("0xb10c")
		}
		keys[slot] = savePending(slot, root, blockHash)
	}
	key := keys[headSlot]

	anchor := state.New(beaconCfg)
	anchor.SetVersion(clparams.GloasVersion)
	require.NoError(t, anchor.SetSlot(headSlot))
	anchor.SetGenesisValidatorsRoot(common.Hash{})
	anchor.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: headSlot})
	graph, err := fork_graph.NewForkGraphDisk(anchor, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	emitters := beaconevents.NewEventEmitter()
	syncedData := synced_data.NewSyncedDataManager(beaconCfg, true)
	forkChoice, err := forkchoice.NewForkChoiceStore(
		clock, anchor, nil, pool.NewOperationsPool(beaconCfg), graph, emitters, syncedData, nil,
		public_keys_registry.NewInMemoryPublicKeysRegistry(), validator_params.NewValidatorParams(), false, nil,
	)
	require.NoError(t, err)
	require.Equal(t, finalizedEpochStart+beaconCfg.SlotsPerEpoch-1, forkChoice.FinalizedSlot())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	svc, err := InitBuilderService(epbscfg.Config{Enabled: true, KeyPath: keyPath, BidMargin: 0.5}, BuilderDeps{
		Ctx: ctx, BeaconCfg: beaconCfg, EthClock: clock, SyncedData: syncedData, ForkChoice: forkChoice,
		Exec: newMockPayloadAssembler(), EpbsPool: pool.NewEpbsPool(), Gossip: &testGossipPublisher{},
		Columns: &testColumnStorage{writes: make(map[uint64]int), errors: make(map[uint64]error)},
		Pending: store, Emitters: emitters,
	})
	require.NoError(t, err)
	defer svc.Shutdown()
	require.NotContains(t, svc.Loop.pendingPayloads, keys[finalizedEpochStart-1])
	for _, slot := range boundarySlots[1:] {
		require.Contains(t, svc.Loop.pendingPayloads, keys[slot])
	}
	records, err := store.Load(t.Context(), 0)
	require.NoError(t, err)
	require.Len(t, records, len(boundarySlots)-1)
	for _, record := range records {
		require.GreaterOrEqual(t, record.Slot, finalizedEpochStart)
	}
	submitter := &mockBidSubmitter{}
	svc.Loop.submitter = submitter
	require.NoError(t, svc.Loop.OnBidWon(
		t.Context(), headSlot, 42, key.parentBlockHash, key.parentBlockRoot, key.blockHash, common.HexToHash("0xa1"),
	))
	require.Len(t, submitter.broadcasts, 1)
}

func TestInitBuilderServiceSeedsPooledProposerPreferences(t *testing.T) {
	beaconCfg := testBeaconCfg()
	beaconCfg.GloasForkEpoch = 0
	beaconCfg.InitializeForkSchedule()
	const currentSlot = uint64(100)
	genesisTime := uint64(time.Now().Unix()) - currentSlot*beaconCfg.SecondsPerSlot
	clock := eth_clock.NewEthereumClock(genesisTime, common.Hash{}, beaconCfg)

	keyBytes := make([]byte, 32)
	for i := range keyBytes {
		keyBytes[i] = byte(i + 1)
	}
	keyPath := filepath.Join(t.TempDir(), "builder.key")
	require.NoError(t, os.WriteFile(keyPath, keyBytes, 0600))

	anchor := state.New(beaconCfg)
	anchor.SetVersion(clparams.GloasVersion)
	require.NoError(t, anchor.SetSlot(currentSlot))
	anchor.SetGenesisValidatorsRoot(common.Hash{})
	anchor.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: currentSlot})
	graph, err := fork_graph.NewForkGraphDisk(anchor, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	emitters := beaconevents.NewEventEmitter()
	syncedData := synced_data.NewSyncedDataManager(beaconCfg, true)
	forkChoice, err := forkchoice.NewForkChoiceStore(
		clock, anchor, nil, pool.NewOperationsPool(beaconCfg), graph, emitters, syncedData, nil,
		public_keys_registry.NewInMemoryPublicKeysRegistry(), validator_params.NewValidatorParams(), false, nil,
	)
	require.NoError(t, err)

	dependentRoot := common.HexToHash("0x1111")
	want := &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: currentSlot, DependentRoot: dependentRoot, TargetGasLimit: 30_000_000,
	}}
	epbsPool := pool.NewEpbsPool()
	epbsPool.ProposerPreferences.Add(pool.ProposerPreferencesKey{
		Slot: currentSlot, DependentRoot: common.HexToHash("0x2222"),
	}, &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: currentSlot, DependentRoot: common.HexToHash("0x2222"),
	}})
	epbsPool.ProposerPreferences.Add(pool.ProposerPreferencesKey{
		Slot: currentSlot + 1, DependentRoot: dependentRoot,
	}, &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: currentSlot + 1, DependentRoot: dependentRoot,
	}})
	epbsPool.ProposerPreferences.Add(pool.ProposerPreferencesKey{
		Slot: currentSlot, DependentRoot: dependentRoot,
	}, want)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	svc, err := InitBuilderService(epbscfg.Config{Enabled: true, KeyPath: keyPath, BidMargin: 0.5}, BuilderDeps{
		Ctx: ctx, BeaconCfg: beaconCfg, EthClock: clock, SyncedData: syncedData, ForkChoice: forkChoice,
		Exec: newMockPayloadAssembler(), EpbsPool: epbsPool, Gossip: &testGossipPublisher{},
		Columns: &testColumnStorage{writes: make(map[uint64]int), errors: make(map[uint64]error)},
		Pending: newFilePendingPayloadStore(t.TempDir(), beaconCfg), Emitters: emitters,
	})
	require.NoError(t, err)
	defer svc.Shutdown()

	got, err := svc.Loop.prefsWatch.WaitForPreferences(t.Context(), currentSlot, dependentRoot, 20*time.Millisecond)
	require.NoError(t, err)
	require.Same(t, want, got)
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

func TestImportedBlockWatcherRevealsAlreadyImportedRootOnHeadChange(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	sc := testSlotContext()
	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	require.NoError(t, loop.OnNewHead(t.Context(), sc))
	prefsWatch.OnPreferencesReceived(sc.Slot, &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: sc.Slot, TargetGasLimit: 30_000_000,
	}})
	require.NoError(t, loop.OnSlot(t.Context(), sc))
	require.Len(t, submitter.submittedBids, 1)

	rootA := common.HexToHash("0xa1")
	rootB := common.HexToHash("0xb2")
	makeBlock := func() *cltypes.SignedBeaconBlock {
		block := cltypes.NewSignedBeaconBlock(loop.beaconCfg, clparams.GloasVersion)
		block.Block.Slot = sc.Slot
		block.Block.Body.SignedExecutionPayloadBid = submitter.submittedBids[0]
		return block
	}
	reader := &importedBlockWatcherReaderStub{
		head: rootA,
		headers: map[common.Hash]*cltypes.BeaconBlockHeader{
			rootA: {Slot: sc.Slot}, rootB: {Slot: sc.Slot},
		},
		blocks: map[common.Hash]*cltypes.SignedBeaconBlock{rootA: makeBlock(), rootB: makeBlock()},
	}
	emitters := beaconevents.NewEventEmitter()
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix())-sc.Slot*loop.beaconCfg.SecondsPerSlot, common.Hash{}, loop.beaconCfg)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		runImportedBlockWatcher(ctx, emitters, reader, clock, loop.beaconCfg, loop)
		close(done)
	}()
	require.Eventually(t, func() bool {
		submitter.mu.Lock()
		defer submitter.mu.Unlock()
		return len(submitter.broadcasts) == 1
	}, time.Second, time.Millisecond)
	emitters.State().SendHead(&beaconevents.HeadData{Slot: sc.Slot, Block: rootB})
	require.Eventually(t, func() bool {
		submitter.mu.Lock()
		defer submitter.mu.Unlock()
		return len(submitter.broadcasts) == 2
	}, time.Second, time.Millisecond)
	cancel()
	<-done
}

func TestImportedBlockWatcherRecoversCanonicalAncestorAndIgnoresSiblingAfterRestart(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	store := newFilePendingPayloadStore(t.TempDir(), loop.beaconCfg)
	loop.pendingStore = store
	sc := testSlotContext()
	sc.Slot = 96
	sc.BuilderStatus.Slot = sc.Slot
	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	require.NoError(t, loop.OnNewHead(t.Context(), sc))
	prefsWatch.OnPreferencesReceived(sc.Slot, &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: sc.Slot, TargetGasLimit: 30_000_000,
	}})
	require.NoError(t, loop.OnSlot(t.Context(), sc))

	restarted, _, restartedSubmitter, _ := setupBuilderLoop(t)
	restarted.pendingStore = store
	require.NoError(t, restarted.restorePendingPayloads(t.Context(), sc.Slot+2, sc.Slot))
	winnerRoot := common.HexToHash("0xb1")
	headRoot := common.HexToHash("0xa1")
	siblingRoot := common.HexToHash("0xc1")
	winner := cltypes.NewSignedBeaconBlock(loop.beaconCfg, clparams.GloasVersion)
	winner.Block.Slot = sc.Slot
	winner.Block.Body.SignedExecutionPayloadBid = submitter.submittedBids[0]
	sibling := cltypes.NewSignedBeaconBlock(loop.beaconCfg, clparams.GloasVersion)
	sibling.Block.Slot = sc.Slot
	sibling.Block.Body.SignedExecutionPayloadBid = submitter.submittedBids[0]
	head := cltypes.NewSignedBeaconBlock(loop.beaconCfg, clparams.GloasVersion)
	head.Block.Slot = sc.Slot + 2
	head.Block.ParentRoot = winnerRoot
	reader := &importedBlockWatcherReaderStub{
		head: headRoot, finalized: solid.Checkpoint{Epoch: sc.Slot / loop.beaconCfg.SlotsPerEpoch},
		headers: map[common.Hash]*cltypes.BeaconBlockHeader{
			headRoot:    {Slot: head.Block.Slot, ParentRoot: winnerRoot},
			winnerRoot:  {Slot: winner.Block.Slot},
			siblingRoot: {Slot: sibling.Block.Slot},
		},
		blocks: map[common.Hash]*cltypes.SignedBeaconBlock{headRoot: head, winnerRoot: winner, siblingRoot: sibling},
	}
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix())-(sc.Slot+2)*loop.beaconCfg.SecondsPerSlot, common.Hash{}, loop.beaconCfg)
	emitters := beaconevents.NewEventEmitter()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		runImportedBlockWatcher(ctx, emitters, reader, clock, loop.beaconCfg, restarted)
		close(done)
	}()
	require.Eventually(t, func() bool {
		restartedSubmitter.mu.Lock()
		defer restartedSubmitter.mu.Unlock()
		return len(restartedSubmitter.broadcasts) == 1
	}, time.Second, time.Millisecond)
	cancel()
	<-done
	require.Len(t, restartedSubmitter.broadcasts, 1)
	require.Equal(t, winnerRoot, restartedSubmitter.broadcasts[0].Message.BeaconBlockRoot)
}

func TestImportedBlockWatcherRetriesWinningPayloadAfterLocalDeadline(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	sc := testSlotContext()
	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	require.NoError(t, loop.OnNewHead(t.Context(), sc))
	prefsWatch.OnPreferencesReceived(sc.Slot, &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: sc.Slot, TargetGasLimit: 30_000_000,
	}})
	require.NoError(t, loop.OnSlot(t.Context(), sc))

	root := common.HexToHash("0xa1")
	block := cltypes.NewSignedBeaconBlock(loop.beaconCfg, clparams.GloasVersion)
	block.Block.Slot = sc.Slot
	block.Block.Body.SignedExecutionPayloadBid = submitter.submittedBids[0]
	reader := &importedBlockWatcherReaderStub{
		head: root,
		headers: map[common.Hash]*cltypes.BeaconBlockHeader{
			root: {Slot: sc.Slot},
		},
		blocks: map[common.Hash]*cltypes.SignedBeaconBlock{root: block},
	}
	submitter.broadcastErr = errors.New("transient gossip failure")
	submitter.broadcastFailures = 1
	emitters := beaconevents.NewEventEmitter()
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Add(-time.Hour).Unix()), common.Hash{}, loop.beaconCfg)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		runImportedBlockWatcher(ctx, emitters, reader, clock, loop.beaconCfg, loop)
		close(done)
	}()
	require.Eventually(t, func() bool {
		submitter.mu.Lock()
		defer submitter.mu.Unlock()
		return len(submitter.broadcasts) == 1
	}, time.Second, time.Millisecond)
	cancel()
	<-done
}

func TestBuilderLoopPruneDiscardsExpiredRevealProgress(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	store := newFilePendingPayloadStore(t.TempDir(), loop.beaconCfg)
	loop.pendingStore = store
	sc := testSlotContext()
	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	require.NoError(t, loop.OnNewHead(t.Context(), sc))
	prefsWatch.OnPreferencesReceived(sc.Slot, &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: sc.Slot, TargetGasLimit: 30_000_000,
	}})
	require.NoError(t, loop.OnSlot(t.Context(), sc))

	root := common.HexToHash("0xa1")
	block := cltypes.NewSignedBeaconBlock(loop.beaconCfg, clparams.GloasVersion)
	block.Block.Slot = sc.Slot
	block.Block.Body.SignedExecutionPayloadBid = submitter.submittedBids[0]
	caplinSubmitter := NewCaplinBidSubmitter(nil, &testGossipPublisher{err: errors.New("unavailable")}, testEnvelopeProcessor{}, nil)
	loop.submitter = caplinSubmitter
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Add(-time.Hour).Unix()), common.Hash{}, loop.beaconCfg)
	scheduler := newRevealScheduler(t.Context(), 1, 1)
	require.NoError(t, scheduleImportedBlockReveal(
		&beaconevents.BlockData{Slot: sc.Slot, Block: root}, testImportedBlockReader{block: block},
		clock, loop.beaconCfg, loop, scheduler,
	))
	scheduler.Wait()
	require.Contains(t, caplinSubmitter.progress, root)

	next := sc
	next.Slot += 2
	next.FinalizedSlot = sc.Slot + 1
	require.NoError(t, loop.OnNewHead(t.Context(), next))
	require.Empty(t, loop.pendingPayloads)
	require.Empty(t, caplinSubmitter.progress)
	require.Zero(t, loop.manager.reservedBidValue)
	records, err := store.Load(t.Context(), 0)
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestRevealQueueAdmissionFailureRemainsRetryable(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	sc := testSlotContext()
	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	require.NoError(t, loop.OnNewHead(t.Context(), sc))
	prefsWatch.OnPreferencesReceived(sc.Slot, &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: sc.Slot, TargetGasLimit: 30_000_000,
	}})
	require.NoError(t, loop.OnSlot(t.Context(), sc))
	root := common.HexToHash("0xcafe")
	block := cltypes.NewSignedBeaconBlock(loop.beaconCfg, clparams.GloasVersion)
	block.Block.Slot = sc.Slot
	block.Block.Body.SignedExecutionPayloadBid = submitter.submittedBids[0]
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix())-sc.Slot*loop.beaconCfg.SecondsPerSlot, common.Hash{}, loop.beaconCfg)
	scheduler := newRevealScheduler(t.Context(), 1, 0)
	err := scheduleImportedBlockReveal(&beaconevents.BlockData{Slot: sc.Slot, Block: root}, testImportedBlockReader{block: block}, clock, loop.beaconCfg, loop, scheduler)
	require.ErrorContains(t, err, "queue full")
	_, queued := loop.queuePendingBidReveal(sc.Slot, 42, sc.Parent.ExecutionHash, sc.Parent.BlockRoot, common.HexToHash("0xb10c"), root)
	require.True(t, queued)
}

func TestImportedBlockWatcherRetriesCurrentHeadReconciliation(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	sc := testSlotContext()
	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	require.NoError(t, loop.OnNewHead(t.Context(), sc))
	prefsWatch.OnPreferencesReceived(sc.Slot, &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
		ProposalSlot: sc.Slot, TargetGasLimit: 30_000_000,
	}})
	require.NoError(t, loop.OnSlot(t.Context(), sc))
	root := common.HexToHash("0xa1")
	block := cltypes.NewSignedBeaconBlock(loop.beaconCfg, clparams.GloasVersion)
	block.Block.Slot = sc.Slot
	block.Block.Body.SignedExecutionPayloadBid = submitter.submittedBids[0]
	reader := &importedBlockWatcherReaderStub{
		head: root, headers: map[common.Hash]*cltypes.BeaconBlockHeader{root: {Slot: sc.Slot}},
		blocks: make(map[common.Hash]*cltypes.SignedBeaconBlock), lookups: make(chan struct{}, 1),
	}
	emitters := beaconevents.NewEventEmitter()
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix())-sc.Slot*loop.beaconCfg.SecondsPerSlot, common.Hash{}, loop.beaconCfg)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		runImportedBlockWatcher(ctx, emitters, reader, clock, loop.beaconCfg, loop)
		close(done)
	}()
	<-reader.lookups
	reader.setBlock(root, block)
	require.Eventually(t, func() bool {
		submitter.mu.Lock()
		defer submitter.mu.Unlock()
		return len(submitter.broadcasts) == 1
	}, time.Second, time.Millisecond)
	cancel()
	<-done
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
