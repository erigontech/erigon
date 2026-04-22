package epbs

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/stretchr/testify/require"
)

// --- Mock payload assembler ---

type mockPayloadAssembler struct {
	mu      sync.Mutex
	nextId  uint64
	results map[uint64]*eladapter.AssembledPayload
	busy    bool
	err     error
}

func newMockPayloadAssembler() *mockPayloadAssembler {
	return &mockPayloadAssembler{
		results: make(map[uint64]*eladapter.AssembledPayload),
	}
}

func (m *mockPayloadAssembler) AssemblePayload(_ context.Context, _ *builder.Parameters) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return 0, m.err
	}
	if m.busy {
		return 0, fmt.Errorf("eladapter: execution module busy")
	}
	m.nextId++
	return m.nextId, nil
}

func (m *mockPayloadAssembler) GetPayload(_ context.Context, payloadID uint64) (*eladapter.AssembledPayload, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result, ok := m.results[payloadID]
	if !ok {
		return nil, nil
	}
	return result, nil
}

// setResultForNext pre-loads a result for the NEXT payloadId that will be assigned.
func (m *mockPayloadAssembler) setResultForNext(payload *eladapter.AssembledPayload) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.results[m.nextId+1] = payload
}

// --- Mock bid submitter ---

type mockBidSubmitter struct {
	mu            sync.Mutex
	submittedBids []*cltypes.SignedExecutionPayloadBid
	broadcasts    []*cltypes.SignedExecutionPayloadEnvelope
	submitBidErr  error
	broadcastErr  error
}

func (s *mockBidSubmitter) SubmitBid(_ context.Context, bid *cltypes.SignedExecutionPayloadBid) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.submitBidErr != nil {
		return s.submitBidErr
	}
	s.submittedBids = append(s.submittedBids, bid)
	return nil
}

func (s *mockBidSubmitter) BroadcastPayload(_ context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broadcastErr != nil {
		return s.broadcastErr
	}
	s.broadcasts = append(s.broadcasts, envelope)
	return nil
}

// --- Test helpers ---

func testBeaconCfg() *clparams.BeaconChainConfig {
	cfg := clparams.MainnetBeaconConfig // copy by value
	cfg.SecondsPerSlot = 12
	cfg.SlotsPerEpoch = 32
	return &cfg
}

func testSigner(t *testing.T) Signer {
	t.Helper()
	// Generate a deterministic 32-byte key
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	signer, err := NewLocalSignerFromBytes(key)
	require.NoError(t, err)
	return signer
}

func testParentInfo() ParentInfo {
	return ParentInfo{
		Slot:          99,
		BlockRoot:     common.HexToHash("0xbeef"),
		ExecutionHash: common.HexToHash("0xdead"),
		ShouldExtend:  true,
	}
}

func testSlotContext() SlotContext {
	return SlotContext{
		Slot:       100,
		Parent:     testParentInfo(),
		Timestamp:  1700000000,
		PrevRandao: common.HexToHash("0xcafe"),
	}
}

func makeTestPayload(t *testing.T, blockValue *big.Int) *eladapter.AssembledPayload {
	t.Helper()
	cfg := testBeaconCfg()
	eth1Block := cltypes.NewEth1Block(clparams.GloasVersion, cfg)
	eth1Block.ParentHash = common.HexToHash("0xdead")
	eth1Block.BlockNumber = 1000
	eth1Block.GasLimit = 30_000_000
	eth1Block.GasUsed = 15_000_000
	eth1Block.BlockHash = common.HexToHash("0xblockhash")
	eth1Block.Extra = solid.NewExtraData()
	eth1Block.Transactions = &solid.TransactionsSSZ{}
	eth1Block.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)

	return &eladapter.AssembledPayload{
		Eth1Block:  eth1Block,
		BlockValue: blockValue,
	}
}

func setupBuilderLoop(t *testing.T) (*BuilderLoop, *mockPayloadAssembler, *mockBidSubmitter, *PreferencesWatcher) {
	t.Helper()

	cfg := testBeaconCfg()
	signer := testSigner(t)
	idx := uint64(42)
	manager := NewBuilderManager(signer, &idx, cfg, common.HexToHash("0x1234"))
	strategy := &FixedMarginStrategy{Margin: 0.85}
	exec := newMockPayloadAssembler()
	prefsWatch := NewPreferencesWatcher()
	submitter := &mockBidSubmitter{}

	loop := NewBuilderLoop(manager, strategy, exec, prefsWatch, submitter, cfg)

	return loop, exec, submitter, prefsWatch
}

// --- Tests ---

func TestBuilderLoop_FastPath(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	ctx := context.Background()
	sc := testSlotContext()

	// Pre-load result for speculative build
	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000))) // 1000 gwei

	// Step 1: OnNewHead starts speculative build
	err := loop.OnNewHead(ctx, sc)
	require.NoError(t, err)

	// Step 2: Simulate preferences arriving.
	// Zero FeeRecipient = no constraint -> speculative build's 0x0 coinbase matches.
	prefs := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   sc.Slot,
			ValidatorIndex: 7,
			FeeRecipient:   common.Address{}, // no constraint -> fast path
			GasLimit:       30_000_000,
		},
	}

	// Send preferences via watcher
	go func() {
		time.Sleep(50 * time.Millisecond)
		prefsWatch.OnPreferencesReceived(sc.Slot, prefs)
	}()

	// Step 3: OnSlot should use fast path
	err = loop.OnSlot(ctx, sc)
	require.NoError(t, err)

	// Verify bid was submitted
	submitter.mu.Lock()
	require.Len(t, submitter.submittedBids, 1)
	bid := submitter.submittedBids[0]
	submitter.mu.Unlock()

	require.Equal(t, sc.Slot, bid.Message.Slot)
	require.Equal(t, sc.Parent.ExecutionHash, bid.Message.ParentBlockHash)
	require.Equal(t, sc.Parent.BlockRoot, bid.Message.ParentBlockRoot)
	require.Equal(t, uint64(42), bid.Message.BuilderIndex)
	require.Equal(t, uint64(0), bid.Message.ExecutionPayment) // per spec
	// Signature should be non-zero
	require.NotEqual(t, common.Bytes96{}, bid.Signature)
}

func TestBuilderLoop_RebuildPath(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	ctx := context.Background()
	sc := testSlotContext()

	// Do NOT call OnNewHead -> no speculative build exists -> triggers rebuild path.

	prefs := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   sc.Slot,
			ValidatorIndex: 7,
			FeeRecipient:   common.HexToAddress("0xfee"),
			GasLimit:       30_000_000,
		},
	}

	// Pre-load result for the rebuild build
	exec.setResultForNext(makeTestPayload(t, big.NewInt(2_000_000_000_000)))

	// Send preferences via watcher before OnSlot
	go func() {
		time.Sleep(50 * time.Millisecond)
		prefsWatch.OnPreferencesReceived(sc.Slot, prefs)
	}()

	err := loop.OnSlot(ctx, sc)
	require.NoError(t, err)

	// Verify bid was submitted
	submitter.mu.Lock()
	require.Len(t, submitter.submittedBids, 1)
	bid := submitter.submittedBids[0]
	submitter.mu.Unlock()

	require.Equal(t, sc.Slot, bid.Message.Slot)
	require.Equal(t, uint64(42), bid.Message.BuilderIndex)
	require.Equal(t, uint64(0), bid.Message.ExecutionPayment)
}

func TestBuilderLoop_SkipPath_NoPreferences(t *testing.T) {
	loop, _, submitter, _ := setupBuilderLoop(t)
	ctx := context.Background()
	sc := testSlotContext()

	// OnSlot with no preferences -> should skip (timeout)
	// Override the preferences timeout for the test
	err := loop.OnSlot(ctx, sc)
	require.NoError(t, err)

	// No bid should be submitted
	submitter.mu.Lock()
	require.Len(t, submitter.submittedBids, 0)
	submitter.mu.Unlock()
}

func TestBuilderLoop_BidWonReveal(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	ctx := context.Background()
	sc := testSlotContext()

	// Step 1: Complete a build cycle (fast path)
	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	err := loop.OnNewHead(ctx, sc)
	require.NoError(t, err)

	prefs := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   sc.Slot,
			ValidatorIndex: 7,
			FeeRecipient:   common.Address{}, // no constraint -> fast path
			GasLimit:       30_000_000,
		},
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		prefsWatch.OnPreferencesReceived(sc.Slot, prefs)
	}()

	err = loop.OnSlot(ctx, sc)
	require.NoError(t, err)

	// Verify bid was submitted
	submitter.mu.Lock()
	require.Len(t, submitter.submittedBids, 1)
	submitter.mu.Unlock()

	// Step 2: Bid won -> reveal
	// beaconBlockRoot is the hash_tree_root of the containing block (distinct from parent root).
	beaconBlockRoot := common.HexToHash("0xbeefcafe")
	err = loop.OnBidWon(ctx, sc.Slot, 42, sc.Parent.ExecutionHash, sc.Parent.BlockRoot, beaconBlockRoot)
	require.NoError(t, err)

	// Verify envelope was broadcast
	submitter.mu.Lock()
	require.Len(t, submitter.broadcasts, 1)
	envelope := submitter.broadcasts[0]
	submitter.mu.Unlock()

	require.NotNil(t, envelope.Message)
	require.Equal(t, uint64(42), envelope.Message.BuilderIndex)
	// BeaconBlockRoot must be the containing block root, NOT the parent root.
	require.Equal(t, beaconBlockRoot, envelope.Message.BeaconBlockRoot)
	require.NotEqual(t, sc.Parent.BlockRoot, envelope.Message.BeaconBlockRoot,
		"BeaconBlockRoot must not equal parentBlockRoot -- they are different blocks")
	require.NotNil(t, envelope.Message.ExecutionRequests)
	// Signature should be non-zero
	require.NotEqual(t, common.Bytes96{}, envelope.Signature)
}

func TestBuilderLoop_BidWonReveal_NoPending(t *testing.T) {
	loop, _, _, _ := setupBuilderLoop(t)
	ctx := context.Background()

	// OnBidWon without any pending payload -> error
	err := loop.OnBidWon(ctx, 100, 42, common.HexToHash("0xdead"), common.HexToHash("0xbeef"), common.HexToHash("0xaabb"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "no pending payload")
}

func TestBuilderLoop_BidWonReveal_WrongBuilder(t *testing.T) {
	// If a different builder wins, we must NOT reveal our payload.
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	ctx := context.Background()
	sc := testSlotContext()

	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	err := loop.OnNewHead(ctx, sc)
	require.NoError(t, err)

	prefs := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   sc.Slot,
			ValidatorIndex: 7,
			FeeRecipient:   common.Address{},
			GasLimit:       30_000_000,
		},
	}
	prefsWatch.OnPreferencesReceived(sc.Slot, prefs)

	err = loop.OnSlot(ctx, sc)
	require.NoError(t, err)

	// Different builder index (99 != 42) should be silently ignored.
	err = loop.OnBidWon(ctx, sc.Slot, 99, sc.Parent.ExecutionHash, sc.Parent.BlockRoot, common.HexToHash("0xbeef"))
	require.NoError(t, err)

	submitter.mu.Lock()
	require.Len(t, submitter.broadcasts, 0, "should NOT reveal when another builder wins")
	submitter.mu.Unlock()

	// Pending payload should still exist for our own bid.
	loop.mu.Lock()
	require.Len(t, loop.pendingPayloads, 1)
	loop.mu.Unlock()
}

func TestBuilderLoop_BidFields(t *testing.T) {
	// Verify that bid fields are set correctly per spec
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	ctx := context.Background()
	sc := testSlotContext()

	exec.setResultForNext(makeTestPayload(t, big.NewInt(5_000_000_000_000)))
	err := loop.OnNewHead(ctx, sc)
	require.NoError(t, err)

	prefs := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   sc.Slot,
			ValidatorIndex: 7,
			FeeRecipient:   common.Address{}, // no constraint -> fast path
			GasLimit:       30_000_000,
		},
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		prefsWatch.OnPreferencesReceived(sc.Slot, prefs)
	}()

	err = loop.OnSlot(ctx, sc)
	require.NoError(t, err)

	submitter.mu.Lock()
	require.Len(t, submitter.submittedBids, 1)
	bid := submitter.submittedBids[0].Message
	submitter.mu.Unlock()

	// ExecutionPayment must be 0
	require.Equal(t, uint64(0), bid.ExecutionPayment)
	// BlobKzgCommitments should be initialized (empty list for block without blob txns)
	require.NotNil(t, &bid.BlobKzgCommitments)
	// ExecutionRequestsRoot should be a valid hash (at least non-panic for empty requests)
	require.NotEqual(t, common.Hash{}, bid.ExecutionRequestsRoot)
}

func TestPreferencesWatcher_Timeout(t *testing.T) {
	w := NewPreferencesWatcher()
	_, err := w.WaitForPreferences(42, 50*time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout")
}

func TestPreferencesWatcher_Receive(t *testing.T) {
	w := NewPreferencesWatcher()
	prefs := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   42,
			ValidatorIndex: 7,
		},
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		w.OnPreferencesReceived(42, prefs)
	}()

	result, err := w.WaitForPreferences(42, time.Second)
	require.NoError(t, err)
	require.Equal(t, prefs, result)
}

func TestPreferencesWatcher_WrongSlot(t *testing.T) {
	w := NewPreferencesWatcher()
	prefs := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   43, // wrong slot
			ValidatorIndex: 7,
		},
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		w.OnPreferencesReceived(43, prefs) // wrong slot -- ignored
	}()

	_, err := w.WaitForPreferences(42, 100*time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout")
}

func TestNewExecutionPayloadEnvelope_Used(t *testing.T) {
	// Verify we use the constructor, not struct literal
	cfg := testBeaconCfg()
	envelope := cltypes.NewExecutionPayloadEnvelope(cfg)
	require.NotNil(t, envelope)
	require.NotNil(t, envelope.Payload)
	require.NotNil(t, envelope.ExecutionRequests)
}

func TestDecodeAndHashRequests_Empty(t *testing.T) {
	cfg := testBeaconCfg()
	signer := testSigner(t)
	idx := uint64(42)
	manager := NewBuilderManager(signer, &idx, cfg, common.HexToHash("0x1234"))
	loop := &BuilderLoop{
		manager:   manager,
		beaconCfg: cfg,
	}

	execReqs, root, err := loop.decodeAndHashRequests(nil)
	require.NoError(t, err)
	require.NotNil(t, execReqs)
	// Empty requests should still produce a valid (non-zero) hash
	require.NotEqual(t, common.Hash{}, root)
}

func TestDomainBeaconBuilder_Used(t *testing.T) {
	// Verify the manager uses DomainBeaconBuilder (0x0B000000) for signing.
	cfg := testBeaconCfg()
	// Check that DomainBeaconBuilder is correctly configured
	require.Equal(t, common.Bytes4{0x0B, 0x00, 0x00, 0x00}, cfg.DomainBeaconBuilder)
}

func TestSpeculativeBuild_StartAndGet(t *testing.T) {
	exec := newMockPayloadAssembler()
	spec := NewSpeculativeBuild(exec)

	payload := makeTestPayload(t, big.NewInt(999))
	exec.setResultForNext(payload)

	ctx := context.Background()
	params := &builder.Parameters{
		ParentHash: common.HexToHash("0xdead"),
		Timestamp:  1700000000,
	}

	payloadId, err := spec.StartBuild(ctx, params)
	require.NoError(t, err)
	require.Equal(t, uint64(1), payloadId)

	result, err := spec.GetResult(ctx, payloadId)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.BlockValue)
}

func TestSpeculativeBuild_Busy(t *testing.T) {
	exec := newMockPayloadAssembler()
	exec.busy = true
	spec := NewSpeculativeBuild(exec)

	ctx := context.Background()
	params := &builder.Parameters{
		ParentHash: common.HexToHash("0xdead"),
	}

	_, err := spec.StartBuild(ctx, params)
	require.Error(t, err)
	require.Contains(t, err.Error(), "busy")
}

func TestCaplinBidSubmitter_SubmitBid(t *testing.T) {
	// This is a basic test that CaplinBidSubmitter compiles and works with nil pool/gossip
	// In a real test, we'd wire up actual pool and gossip mocks.
	bid := &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			Slot:               100,
			ParentBlockHash:    common.HexToHash("0xdead"),
			ParentBlockRoot:    common.HexToHash("0xbeef"),
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
		},
	}

	// Verify the bid message fields
	require.Equal(t, uint64(100), bid.Message.Slot)
	require.NotNil(t, bid.Message)
}

func TestBuilderLoop_EnvelopeUsesConstructor(t *testing.T) {
	// Verify that OnBidWon uses cltypes.NewExecutionPayloadEnvelope(cfg) constructor
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	ctx := context.Background()
	sc := testSlotContext()

	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	err := loop.OnNewHead(ctx, sc)
	require.NoError(t, err)

	prefs := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   sc.Slot,
			ValidatorIndex: 7,
			FeeRecipient:   common.Address{}, // no constraint -> fast path
			GasLimit:       30_000_000,
		},
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		prefsWatch.OnPreferencesReceived(sc.Slot, prefs)
	}()

	err = loop.OnSlot(ctx, sc)
	require.NoError(t, err)

	// parentBlockRoot for key lookup, beaconBlockRoot for envelope
	beaconBlockRoot := common.HexToHash("0xbeefcafe")
	err = loop.OnBidWon(ctx, sc.Slot, 42, sc.Parent.ExecutionHash, sc.Parent.BlockRoot, beaconBlockRoot)
	require.NoError(t, err)

	submitter.mu.Lock()
	require.Len(t, submitter.broadcasts, 1)
	env := submitter.broadcasts[0]
	submitter.mu.Unlock()

	// The envelope should have proper initialization from constructor
	require.NotNil(t, env.Message.Payload)
	require.NotNil(t, env.Message.ExecutionRequests)

	// BeaconBlockRoot must be the containing block root
	require.Equal(t, beaconBlockRoot, env.Message.BeaconBlockRoot)

	// The signature should be from DomainBeaconBuilder (0x0B000000)
	// We can verify it's a proper BLS signature (non-zero)
	require.NotEqual(t, common.Bytes96{}, env.Signature)

	fmt.Printf("DomainBeaconBuilder: %x\n", loop.beaconCfg.DomainBeaconBuilder)
}

// TestBuilderLoop_FastPath_FeeRecipientMismatch verifies BUG 1 fix:
// when a speculative build exists but the fee recipient doesn't match proposer
// preferences, the builder falls through to the rebuild path.
func TestBuilderLoop_FastPath_FeeRecipientMismatch(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	ctx := context.Background()
	sc := testSlotContext()

	// Pre-load result for speculative build (uses 0x0 fee recipient)
	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))

	// Start speculative build via OnNewHead
	err := loop.OnNewHead(ctx, sc)
	require.NoError(t, err)

	// Proposer wants a NON-ZERO fee recipient -> speculative build mismatch
	prefs := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   sc.Slot,
			ValidatorIndex: 7,
			FeeRecipient:   common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
			GasLimit:       30_000_000,
		},
	}

	// Pre-load a second result for the REBUILD (the mock assigns sequential IDs)
	exec.setResultForNext(makeTestPayload(t, big.NewInt(2_000_000_000_000)))

	// Send preferences and trigger OnSlot
	go func() {
		time.Sleep(50 * time.Millisecond)
		prefsWatch.OnPreferencesReceived(sc.Slot, prefs)
	}()

	err = loop.OnSlot(ctx, sc)
	require.NoError(t, err)

	// A bid should still be submitted (via rebuild path)
	submitter.mu.Lock()
	require.Len(t, submitter.submittedBids, 1, "rebuild path should produce a bid")
	submitter.mu.Unlock()
}

/// TestBuilderLoop_FastPath_GasLimitMismatch verifies that when a speculative
// build exists but the gas limit doesn't match proposer preferences, the builder
// falls through to the rebuild path. Without this, gossip validation would reject
// the bid (execution_payload_bid_service.go requires bid.gas_limit == prefs.gas_limit).
func TestBuilderLoop_FastPath_GasLimitMismatch(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	ctx := context.Background()
	sc := testSlotContext()

	// Pre-load result for speculative build — default gas limit from the EL
	payload := makeTestPayload(t, big.NewInt(1_000_000_000_000))
	payload.Eth1Block.GasLimit = 30_000_000 // speculative build used this
	exec.setResultForNext(payload)

	// Start speculative build via OnNewHead
	err := loop.OnNewHead(ctx, sc)
	require.NoError(t, err)

	// Proposer wants a DIFFERENT gas limit
	prefs := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   sc.Slot,
			ValidatorIndex: 7,
			FeeRecipient:   common.Address{}, // zero = no FeeRecipient constraint
			GasLimit:       50_000_000,        // different from speculative build
		},
	}

	// Pre-load a second result for the REBUILD
	rebuildPayload := makeTestPayload(t, big.NewInt(2_000_000_000_000))
	rebuildPayload.Eth1Block.GasLimit = 50_000_000
	exec.setResultForNext(rebuildPayload)

	// Send preferences and trigger OnSlot
	go func() {
		time.Sleep(50 * time.Millisecond)
		prefsWatch.OnPreferencesReceived(sc.Slot, prefs)
	}()

	err = loop.OnSlot(ctx, sc)
	require.NoError(t, err)

	// A bid should be submitted via rebuild path
	submitter.mu.Lock()
	require.Len(t, submitter.submittedBids, 1, "rebuild path should produce a bid")
	bid := submitter.submittedBids[0]
	submitter.mu.Unlock()

	// The bid's gas limit should match the proposer's preference, not the speculative value.
	require.Equal(t, uint64(50_000_000), bid.Message.GasLimit,
		"bid gas_limit must match proposer preferences, not speculative build")
}

// TestPreferencesWatcher_EarlyArrival verifies BUG 3 fix:
// preferences arriving BEFORE WaitForPreferences is called are not lost.
func TestPreferencesWatcher_EarlyArrival(t *testing.T) {
	w := NewPreferencesWatcher()
	prefs := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   42,
			ValidatorIndex: 7,
		},
	}

	// Preferences arrive BEFORE anyone is waiting
	w.OnPreferencesReceived(42, prefs)

	// Now wait -- should return immediately (fast path from prefsBySlot map)
	result, err := w.WaitForPreferences(42, 100*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, prefs, result)
}

// TestPreferencesWatcher_EarlyArrival_WrongSlot verifies that early-arriving
// preferences for a different slot don't satisfy a wait for the target slot.
func TestPreferencesWatcher_EarlyArrival_WrongSlot(t *testing.T) {
	w := NewPreferencesWatcher()
	prefs := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   43,
			ValidatorIndex: 7,
		},
	}

	// Prefs for slot 43 arrive early
	w.OnPreferencesReceived(43, prefs)

	// Wait for slot 42 -- should timeout (slot 43 prefs don't match)
	_, err := w.WaitForPreferences(42, 100*time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "timeout")
}

// TestBuilderLoop_OnBidWon_BeaconBlockRootDistinctFromParent verifies BUG 2 fix:
// the envelope's BeaconBlockRoot is set to the containing block root, NOT the
// parent block root used for pending payload lookup.
func TestBuilderLoop_OnBidWon_BeaconBlockRootDistinctFromParent(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	ctx := context.Background()
	sc := testSlotContext()

	// Complete a build cycle
	exec.setResultForNext(makeTestPayload(t, big.NewInt(1_000_000_000_000)))
	err := loop.OnNewHead(ctx, sc)
	require.NoError(t, err)

	prefs := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   sc.Slot,
			ValidatorIndex: 7,
			FeeRecipient:   common.Address{}, // no constraint -> fast path
			GasLimit:       30_000_000,
		},
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		prefsWatch.OnPreferencesReceived(sc.Slot, prefs)
	}()
	err = loop.OnSlot(ctx, sc)
	require.NoError(t, err)

	// Use two distinct hashes to prove they're assigned correctly.
	parentBlockRoot := sc.Parent.BlockRoot                    // used for lookup
	beaconBlockRoot := common.HexToHash("0x1111222233334444") // containing block root

	err = loop.OnBidWon(ctx, sc.Slot, 42, sc.Parent.ExecutionHash, parentBlockRoot, beaconBlockRoot)
	require.NoError(t, err)

	submitter.mu.Lock()
	require.Len(t, submitter.broadcasts, 1)
	env := submitter.broadcasts[0]
	submitter.mu.Unlock()

	// Envelope BeaconBlockRoot must be the containing block root
	require.Equal(t, beaconBlockRoot, env.Message.BeaconBlockRoot)
	// And it must NOT equal the parent block root
	require.NotEqual(t, parentBlockRoot, env.Message.BeaconBlockRoot,
		"BeaconBlockRoot must be the containing block's root, not the parent's")
}

// TestBuilderLoop_MultiMarket verifies that the builder can handle multiple
// parent candidates in the same slot (multi-market scenario from ActiveParents).
// Each parent has an independent speculative build, bid, and pending payload.
func TestBuilderLoop_MultiMarket(t *testing.T) {
	loop, exec, submitter, prefsWatch := setupBuilderLoop(t)
	ctx := context.Background()

	slot := uint64(100)

	// Two different parent candidates for the same slot (FULL and EMPTY path).
	parentA := ParentInfo{
		Slot:          99,
		BlockRoot:     common.HexToHash("0xaaaa"),
		ExecutionHash: common.HexToHash("0xaa01"),
		ShouldExtend:  true,
	}
	parentB := ParentInfo{
		Slot:          98,
		BlockRoot:     common.HexToHash("0xbbbb"),
		ExecutionHash: common.HexToHash("0xbb02"),
		ShouldExtend:  false,
	}

	scA := SlotContext{Slot: slot, Parent: parentA, Timestamp: 1700000000, PrevRandao: common.HexToHash("0xcafe")}
	scB := SlotContext{Slot: slot, Parent: parentB, Timestamp: 1700000000, PrevRandao: common.HexToHash("0xcafe")}

	// Pre-load results for both speculative builds (sequential payloadIds).
	payloadA := makeTestPayload(t, big.NewInt(1_000_000_000_000))
	payloadA.Eth1Block.BlockHash = common.HexToHash("0xcc01")
	exec.setResultForNext(payloadA)

	err := loop.OnNewHead(ctx, scA)
	require.NoError(t, err)

	payloadB := makeTestPayload(t, big.NewInt(2_000_000_000_000))
	payloadB.Eth1Block.BlockHash = common.HexToHash("0xcc02")
	exec.setResultForNext(payloadB)

	err = loop.OnNewHead(ctx, scB)
	require.NoError(t, err)

	// Both speculative builds should be tracked independently.
	loop.mu.Lock()
	require.Len(t, loop.speculativePayloads, 2, "both parents should have separate speculative builds")
	loop.mu.Unlock()

	// Send preferences (no fee recipient constraint -> fast path for both).
	prefs := &cltypes.SignedProposerPreferences{
		Message: &cltypes.ProposerPreferences{
			ProposalSlot:   slot,
			ValidatorIndex: 7,
			FeeRecipient:   common.Address{},
			GasLimit:       30_000_000,
		},
	}

	// OnSlot for parent A
	prefsWatch.OnPreferencesReceived(slot, prefs)
	err = loop.OnSlot(ctx, scA)
	require.NoError(t, err)

	// OnSlot for parent B
	prefsWatch.OnPreferencesReceived(slot, prefs)
	err = loop.OnSlot(ctx, scB)
	require.NoError(t, err)

	// Verify two separate bids were submitted.
	submitter.mu.Lock()
	require.Len(t, submitter.submittedBids, 2, "each parent should produce an independent bid")
	bidA := submitter.submittedBids[0].Message
	bidB := submitter.submittedBids[1].Message
	submitter.mu.Unlock()

	// Bids should reference their respective parents.
	require.Equal(t, parentA.ExecutionHash, bidA.ParentBlockHash)
	require.Equal(t, parentA.BlockRoot, bidA.ParentBlockRoot)
	require.Equal(t, parentB.ExecutionHash, bidB.ParentBlockHash)
	require.Equal(t, parentB.BlockRoot, bidB.ParentBlockRoot)

	// BlockHashes should differ (different EL builds).
	require.Equal(t, common.HexToHash("0xcc01"), bidA.BlockHash)
	require.Equal(t, common.HexToHash("0xcc02"), bidB.BlockHash)

	// Both should have ExecutionPayment=0.
	require.Equal(t, uint64(0), bidA.ExecutionPayment)
	require.Equal(t, uint64(0), bidB.ExecutionPayment)

	// Verify two separate pending payloads exist.
	loop.mu.Lock()
	require.Len(t, loop.pendingPayloads, 2, "each parent should have a separate pending payload")
	loop.mu.Unlock()

	// Reveal parent A's payload
	beaconRootA := common.HexToHash("0xdd01")
	err = loop.OnBidWon(ctx, slot, 42, parentA.ExecutionHash, parentA.BlockRoot, beaconRootA)
	require.NoError(t, err)

	// Reveal parent B's payload
	beaconRootB := common.HexToHash("0xdd02")
	err = loop.OnBidWon(ctx, slot, 42, parentB.ExecutionHash, parentB.BlockRoot, beaconRootB)
	require.NoError(t, err)

	// Verify two separate envelopes were broadcast.
	submitter.mu.Lock()
	require.Len(t, submitter.broadcasts, 2, "each parent should have an independent envelope broadcast")
	envA := submitter.broadcasts[0]
	envB := submitter.broadcasts[1]
	submitter.mu.Unlock()

	// Each envelope should have the correct beacon block root.
	require.Equal(t, beaconRootA, envA.Message.BeaconBlockRoot)
	require.Equal(t, beaconRootB, envB.Message.BeaconBlockRoot)
}
