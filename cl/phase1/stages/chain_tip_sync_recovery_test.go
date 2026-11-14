package stages

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	das_mock_services "github.com/erigontech/erigon/cl/das/mock_services"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type orderedPayloadValidator struct {
	slow  common.Hash
	calls []common.Hash
}

func (v *orderedPayloadValidator) NewPayloadWithAdmission(ctx context.Context, payload *cltypes.Eth1Block, _ *common.Hash, _ []common.Hash, _ []hexutil.Bytes) (execution_client.PayloadStatus, error) {
	v.calls = append(v.calls, payload.BlockHash)
	if payload.BlockHash == v.slow {
		<-ctx.Done()
		return admitted(ctx)(execution_client.PayloadStatusNone, ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return admitted(ctx)(execution_client.PayloadStatusNone, err)
	}
	return execution_client.PayloadStatusNotValidated, nil
}

type storedParentPayloadTestStore struct {
	has          bool
	status       execution_client.PayloadStatus
	statusFound  bool
	block        *cltypes.SignedBeaconBlock
	markRetained bool
	marked       execution_client.PayloadStatus
	markedGas    uint64
	requeued     []forkchoice.PendingELPayload
}

type chainTipBatchForkGraph struct {
	fork_graph.ForkGraph
	parents     map[common.Hash]*cltypes.SignedBeaconBlock
	parentState *state2.CachingBeaconState
	envelopesMu sync.RWMutex
	envelopes   map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	rejected    map[common.Hash]bool
	added       map[common.Hash]int
	insertOnAdd bool
	envReads    map[common.Hash]int
	anchorSlot  *uint64
}

type chainTipBatchEnvelopeSentinel struct {
	sentinelproto.SentinelClient
	response []byte
	calls    int
}

func (s *chainTipBatchEnvelopeSentinel) SendRequest(context.Context, *sentinelproto.RequestData, ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	s.calls++
	return &sentinelproto.ResponseData{Data: s.response, Peer: &sentinelproto.Peer{Pid: "envelope-peer"}}, nil
}

func (*chainTipBatchEnvelopeSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
}

func (*chainTipBatchEnvelopeSentinel) BanPeer(context.Context, *sentinelproto.Peer, ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	return &sentinelproto.EmptyMessage{}, nil
}

func (g *chainTipBatchForkGraph) AddChainSegment(block *cltypes.SignedBeaconBlock, _ bool) (*state2.CachingBeaconState, fork_graph.ChainSegmentInsertionResult, error) {
	root, err := block.Block.HashSSZ()
	if err != nil {
		return nil, fork_graph.InvalidBlock, err
	}
	g.added[common.Hash(root)]++
	if g.insertOnAdd {
		g.parents[common.Hash(root)] = block
		return g.parentState, fork_graph.Success, nil
	}
	return nil, fork_graph.PreValidated, nil
}

func (g *chainTipBatchForkGraph) AnchorSlot() uint64 {
	if g.anchorSlot != nil {
		return *g.anchorSlot
	}
	return g.ForkGraph.AnchorSlot()
}

func (g *chainTipBatchForkGraph) GetHeader(root common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	if parent, ok := g.parents[root]; ok {
		return parent.SignedBeaconBlockHeader().Header, true
	}
	return g.ForkGraph.GetHeader(root)
}

func (g *chainTipBatchForkGraph) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	if parent, ok := g.parents[root]; ok {
		return parent, true
	}
	return g.ForkGraph.GetBlock(root)
}

func (g *chainTipBatchForkGraph) ReplayState(root common.Hash) (*state2.CachingBeaconState, error) {
	if _, ok := g.parents[root]; ok {
		return g.parentState.Copy()
	}
	return g.ForkGraph.ReplayState(root)
}

func (g *chainTipBatchForkGraph) GetCurrentJustifiedCheckpoint(root common.Hash) (solid.Checkpoint, bool) {
	if _, ok := g.parents[root]; ok {
		return g.parentState.CurrentJustifiedCheckpoint(), true
	}
	return g.ForkGraph.GetCurrentJustifiedCheckpoint(root)
}

func (g *chainTipBatchForkGraph) HasEnvelope(root common.Hash) bool {
	if g.IsEnvelopeRejected(root) {
		return false
	}
	g.envelopesMu.RLock()
	defer g.envelopesMu.RUnlock()
	_, ok := g.envelopes[root]
	return ok || g.ForkGraph.HasEnvelope(root)
}

func (g *chainTipBatchForkGraph) RejectEnvelope(root common.Hash) bool {
	if !g.IsBlockRetained(root) {
		return false
	}
	g.envelopesMu.Lock()
	defer g.envelopesMu.Unlock()
	if g.rejected == nil {
		g.rejected = make(map[common.Hash]bool)
	}
	g.rejected[root] = true
	g.ClearPayloadAccepted(root)
	g.MarkPayloadUnavailable(root)
	return true
}

func (g *chainTipBatchForkGraph) IsEnvelopeRejected(root common.Hash) bool {
	g.envelopesMu.RLock()
	defer g.envelopesMu.RUnlock()
	return g.rejected[root] || g.ForkGraph.IsEnvelopeRejected(root)
}

func (g *chainTipBatchForkGraph) ReadEnvelopeFromDisk(root common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	g.envelopesMu.Lock()
	defer g.envelopesMu.Unlock()
	if g.envReads == nil {
		g.envReads = map[common.Hash]int{}
	}
	g.envReads[root]++
	if envelope, ok := g.envelopes[root]; ok {
		return envelope, nil
	}
	return g.ForkGraph.ReadEnvelopeFromDisk(root)
}

func (g *chainTipBatchForkGraph) DumpEnvelopeOnDisk(root common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
	g.envelopesMu.Lock()
	defer g.envelopesMu.Unlock()
	g.envelopes[root] = envelope
	return nil
}

func (g *chainTipBatchForkGraph) IsBlockRetained(root common.Hash) bool {
	if _, ok := g.parents[root]; ok {
		return true
	}
	guard, ok := g.ForkGraph.(interface{ IsBlockRetained(common.Hash) bool })
	return ok && guard.IsBlockRetained(root)
}

func (g *chainTipBatchForkGraph) WithRetainedBlock(root common.Hash, fn func(func(common.Hash) bool)) bool {
	if !g.IsBlockRetained(root) {
		return false
	}
	fn(g.IsBlockRetained)
	return true
}

func (s *storedParentPayloadTestStore) HasEnvelope(common.Hash) bool { return s.has }

func (s *storedParentPayloadTestStore) RejectEnvelope(common.Hash) { s.has = false }

func (s *storedParentPayloadTestStore) GetRecentExecutionPayloadStatusByRoot(common.Hash) (execution_client.PayloadStatus, bool) {
	return s.status, s.statusFound
}

func (s *storedParentPayloadTestStore) GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return s.block, s.block != nil
}

func (s *storedParentPayloadTestStore) MarkPayloadStatusAndGasLimitIfRetained(_ common.Hash, _ common.Hash, status execution_client.PayloadStatus, gasLimit uint64) (execution_client.PayloadStatus, bool) {
	s.marked = status
	s.markedGas = gasLimit
	return status, s.markRetained
}

func (s *storedParentPayloadTestStore) RequeuePendingELPayload(payload forkchoice.PendingELPayload) {
	s.requeued = append(s.requeued, payload)
}

func newChainTipBatchFixture(t *testing.T, replayStatus execution_client.PayloadStatus) (*Cfg, *chainTipBatchForkGraph, common.Hash, *cltypes.SignedBeaconBlock, *cltypes.SignedBeaconBlock, *testExecutionEngine) {
	return newChainTipBatchFixtureWithRecordedStatus(t, replayStatus, true)
}

func newChainTipBatchFixtureWithRecordedStatus(t *testing.T, replayStatus execution_client.PayloadStatus, recordStatus bool) (*Cfg, *chainTipBatchForkGraph, common.Hash, *cltypes.SignedBeaconBlock, *cltypes.SignedBeaconBlock, *testExecutionEngine) {
	return newChainTipBatchFixtureWithBlob(t, replayStatus, recordStatus, false)
}

func newChainTipBatchFixtureWithBlob(t *testing.T, replayStatus execution_client.PayloadStatus, recordStatus, withBlob bool) (*Cfg, *chainTipBatchForkGraph, common.Hash, *cltypes.SignedBeaconBlock, *cltypes.SignedBeaconBlock, *testExecutionEngine) {
	t.Helper()

	beaconCfg, anchorState, parentBid, envelope, _ := validAnchorEnvelopeFixture(t, 0)
	beaconCfg.AltairForkEpoch = 0
	beaconCfg.BellatrixForkEpoch = 0
	beaconCfg.CapellaForkEpoch = 0
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.ElectraForkEpoch = 0
	beaconCfg.FuluForkEpoch = 0
	beaconCfg.InitializeForkSchedule()
	require.NoError(t, anchorState.SetSlot(63))
	anchorBid := anchorState.GetLatestExecutionPayloadBid().Copy()
	anchorBid.ParentBlockHash = parentBid.ParentBlockHash
	anchorState.SetLatestExecutionPayloadBid(anchorBid)
	anchorState.SetLatestBlockHash(parentBid.ParentBlockHash)
	anchorRoot, err := anchorState.BlockRoot()
	require.NoError(t, err)

	parentState, err := anchorState.Copy()
	require.NoError(t, err)
	require.NoError(t, parentState.SetSlot(envelope.Message.Payload.SlotNumber))
	if !recordStatus {
		parentState.SetCurrentEpochParticipation(solid.NewParticipationBitList(parentState.ValidatorLength(), int(beaconCfg.ValidatorRegistryLimit)))
		parentState.SetPreviousEpochParticipation(solid.NewParticipationBitList(parentState.ValidatorLength(), int(beaconCfg.ValidatorRegistryLimit)))
		anchorCheckpoint := solid.Checkpoint{Epoch: anchorState.Slot() / beaconCfg.SlotsPerEpoch, Root: anchorRoot}
		parentState.SetPreviousJustifiedCheckpoint(anchorCheckpoint)
		parentState.SetCurrentJustifiedCheckpoint(anchorCheckpoint)
		parentState.SetFinalizedCheckpoint(anchorCheckpoint)
	}
	parentBid.ParentBlockRoot = anchorRoot
	if withBlob {
		commitment := new(cltypes.KZGCommitment)
		versionedHash, err := utils.KzgCommitmentToVersionedHash(common.Bytes48(*commitment))
		require.NoError(t, err)
		txn := &types.BlobTx{
			DynamicFeeTransaction: types.DynamicFeeTransaction{CommonTx: types.CommonTx{GasLimit: 21_000, To: &common.Address{1}}},
			BlobVersionedHashes:   []common.Hash{versionedHash},
		}
		raw, err := types.MarshalTransactionsBinary(types.Transactions{txn})
		require.NoError(t, err)
		envelope.Message.Payload.Transactions = solid.NewProgressiveTransactionsSSZFromTransactions(raw)
		envelope.Message.Payload.BlobGasUsed = misc.GetBlobGasUsed(1)
		parentBid.BlobKzgCommitments.EnsureStaticProgressive(cltypes.MaxBlobsCommittmentsPerBlock, 48)
		parentBid.BlobKzgCommitments.Append(commitment)
	}
	envelope.Message.ParentBeaconBlockRoot = anchorRoot
	envelope.Message.Payload.Time = state2.ComputeTimestampAtSlot(parentState, parentState.Slot())
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(beaconCfg, envelope.Message.ExecutionRequests))
	envelope.Message.Payload.BlockHash, err = envelope.Message.Payload.ComputeBlockHash(&envelope.Message.ParentBeaconBlockRoot, requestsHash, nil)
	require.NoError(t, err)
	parentBid.BlockHash = envelope.Message.Payload.BlockHash
	parentState.SetLatestExecutionPayloadBid(parentBid)
	parentState.SetLatestBlockHash(envelope.Message.Payload.ParentHash)
	parentState.SetPayloadExpectedWithdrawals(envelope.Message.Payload.Withdrawals)
	parent := cltypes.NewSignedBeaconBlock(beaconCfg, clparams.GloasVersion)
	parent.Block.Slot = envelope.Message.Payload.SlotNumber
	parent.Block.ParentRoot = anchorRoot
	parent.Block.Body.GetSignedExecutionPayloadBid().Message = parentBid
	parentState.SetLatestBlockHeader(parent.SignedBeaconBlockHeader().Header)
	parent.Block.StateRoot, err = parentState.HashSSZ()
	require.NoError(t, err)
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	stateBlockRoot, err := parentState.BlockRoot()
	require.NoError(t, err)
	require.Equal(t, parentRoot, stateBlockRoot)
	envelope.Message.BeaconBlockRoot = parentRoot
	privKey, err := bls.NewPrivateKeyFromIKM([]byte("01234567890123456789012345678901"))
	require.NoError(t, err)
	signAnchorEnvelope(t, parentState, privKey, envelope, parent.Block.Slot)

	baseGraph, err := fork_graph.NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	graph := &chainTipBatchForkGraph{
		ForkGraph: baseGraph,
		parents: map[common.Hash]*cltypes.SignedBeaconBlock{
			parentRoot: parent,
		},
		parentState: parentState,
		envelopes: map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{
			parentRoot: envelope,
		},
		added: make(map[common.Hash]int),
	}
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, beaconCfg)
	engine := &testExecutionEngine{payloadStatus: replayStatus}
	store, err := forkchoice.NewForkChoiceStore(
		clock,
		anchorState,
		engine,
		pool.NewOperationsPool(beaconCfg),
		graph,
		beaconevents.NewEventEmitter(),
		synced_data.NewSyncedDataManager(beaconCfg, true),
		nil,
		public_keys_registry.NewInMemoryPublicKeysRegistry(),
		validator_params.NewValidatorParams(),
		false,
		nil,
	)
	require.NoError(t, err)
	store.OnTick((parent.Block.Slot + 1) * beaconCfg.SecondsPerSlot * 1000)
	if recordStatus {
		store.MarkPayloadStatus(parentRoot, envelope.Message.Payload.BlockHash, execution_client.PayloadStatusNone)
		status, found := store.GetRecentExecutionPayloadStatusByRoot(parentRoot)
		require.True(t, found)
		require.EqualValues(t, execution_client.PayloadStatusNone, status)
	}

	fullChild := cltypes.NewSignedBeaconBlock(beaconCfg, clparams.GloasVersion)
	fullChild.Block.Slot = parent.Block.Slot + 1
	fullChild.Block.ParentRoot = parentRoot
	fullChild.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = parentBid.BlockHash
	emptyChild := cltypes.NewSignedBeaconBlock(beaconCfg, clparams.GloasVersion)
	emptyChild.Block.Slot = parent.Block.Slot + 1
	emptyChild.Block.ParentRoot = parentRoot
	emptyChild.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = parentBid.ParentBlockHash

	stageCfg := &Cfg{
		beaconCfg:             beaconCfg,
		forkChoice:            store,
		indiciesDB:            mdbxtest.NewTestDB(t, dbcfg.ChainDB),
		executionClient:       engine,
		gloasPayloadValidator: engine,
	}
	return stageCfg, graph, parentRoot, fullChild, emptyChild, engine
}

func TestChainTipSyncVerifiesStoredGloasPayloadWhileBehind(t *testing.T) {
	cfg, graph, headRoot, _, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	headBlock := graph.parents[headRoot]
	delete(graph.parents, headRoot)
	graph.insertOnAdd = true
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), headBlock, false, false, false))

	selectedRoot, _, err := cfg.forkChoice.GetHead(nil)
	require.NoError(t, err)
	require.Equal(t, headRoot, selectedRoot)
	require.True(t, cfg.forkChoice.HasEnvelope(headRoot))
	_, found := cfg.forkChoice.GetRecentExecutionPayloadStatusByRoot(headRoot)
	require.False(t, found)

	targetSlot := headBlock.Block.Slot + 1
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	err = chainTipSync(ctx, log.Root(), cfg, Args{seenSlot: targetSlot - 1, targetSlot: targetSlot})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, engine.newPayloadCalls)
	require.True(t, cfg.forkChoice.IsPayloadVerified(headRoot))
}

func TestForwardSyncReplayMakesGloasPayloadAvailableForHeadSelection(t *testing.T) {
	cfg, graph, parentRoot, fullChild, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	engine.supportInsertion = true
	parent := graph.parents[parentRoot]
	envelope := graph.envelopes[parentRoot]
	delete(graph.parents, parentRoot)
	delete(graph.envelopes, parentRoot)
	graph.insertOnAdd = true
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), parent, false, false, false))
	require.NoError(t, processDownloadedGloasEnvelope(
		t.Context(),
		log.Root(),
		cfg.forkChoice,
		&gloasCollectorTest{},
		parent.Block,
		parentRoot,
		envelope,
		true,
		shouldValidateForwardSyncPayload(cfg, true),
		false,
	))
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), fullChild, false, false, false))
	fullChildRoot, err := fullChild.Block.HashSSZ()
	require.NoError(t, err)

	selectedRoot, _, err := cfg.forkChoice.GetHead(nil)
	require.NoError(t, err)
	require.Equal(t, common.Hash(fullChildRoot), selectedRoot)
	require.Zero(t, engine.newPayloadCalls)
	require.False(t, cfg.forkChoice.IsPayloadVerified(parentRoot))
}

func TestChainTipSyncDoesNotVerifyStoredGloasPayloadWhileExecutionEngineIsBusy(t *testing.T) {
	cfg, graph, headRoot, _, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	headBlock := graph.parents[headRoot]
	delete(graph.parents, headRoot)
	graph.insertOnAdd = true
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), headBlock, false, false, false))
	engine.notReady = true

	targetSlot := headBlock.Block.Slot + 1
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := chainTipSync(ctx, log.Root(), cfg, Args{seenSlot: targetSlot - 1, targetSlot: targetSlot})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, engine.newPayloadCalls)
}

func TestVerifyUnverifiedGloasPayloadsDoesNotRecordInterruptedCallAsVerdict(t *testing.T) {
	cfg, graph, headRoot, _, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	headBlock := graph.parents[headRoot]
	delete(graph.parents, headRoot)
	graph.insertOnAdd = true
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), headBlock, false, false, false))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	engine.newPayloadFn = func(ctx context.Context, _ *cltypes.Eth1Block) (execution_client.PayloadStatus, error) {
		cancel()
		return execution_client.PayloadStatusNone, ctx.Err()
	}
	cfg.gloasVerificationCursor = headRoot
	// Through the store's own admission, which classifies the interrupted call.
	cfg.gloasPayloadValidator = cfg.forkChoice

	verifyUnverifiedGloasPayloads(ctx, cfg)

	require.Equal(t, 1, engine.newPayloadCalls)
	_, found := cfg.forkChoice.GetRecentExecutionPayloadStatusByRoot(headRoot)
	require.False(t, found)
	require.False(t, graph.IsPayloadUnavailable(headRoot))
	require.Empty(t, cfg.forkChoice.DrainPendingELPayloads())
	require.Equal(t, headRoot, cfg.gloasVerificationCursor)

	engine.newPayloadFn = nil
	verifyUnverifiedGloasPayloads(t.Context(), cfg)

	require.Equal(t, 2, engine.newPayloadCalls)
	require.True(t, cfg.forkChoice.IsPayloadVerified(headRoot))
	require.Zero(t, cfg.gloasVerificationCursor)
}

func TestVerifyUnverifiedGloasPayloadsRejectsBlobMismatch(t *testing.T) {
	cfg, graph, headRoot, child, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	headBlock := graph.parents[headRoot]
	delete(graph.parents, headRoot)
	graph.insertOnAdd = true
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), headBlock, false, false, false))
	engine.newPayloadFn = func(context.Context, *cltypes.Eth1Block) (execution_client.PayloadStatus, error) {
		return execution_client.PayloadStatusInvalidated, misc.ErrMismatchBlobHashes
	}
	pending := forkchoice.PendingELPayload{Block: headBlock, Envelope: graph.envelopes[headRoot]}
	cfg.forkChoice.RequeuePendingELPayload(pending)

	verifyUnverifiedGloasPayloads(t.Context(), cfg)
	verifyUnverifiedGloasPayloads(t.Context(), cfg)
	drainPendingGloasPayloads(t.Context(), cfg)

	require.Equal(t, 1, engine.newPayloadCalls)
	require.False(t, cfg.forkChoice.HasEnvelope(headRoot))
	require.False(t, graph.IsBlockInvalid(headRoot))
	require.True(t, graph.IsPayloadUnavailable(headRoot))
	_, found := cfg.forkChoice.GetRecentExecutionPayloadStatus(pending.Envelope.Message.Payload.BlockHash)
	require.False(t, found)
	cfg.forkChoice.RequeuePendingELPayload(pending)
	require.Empty(t, cfg.forkChoice.DrainPendingELPayloads())
	roots, _ := determineParentEnvelopeRoots(cfg, []*cltypes.SignedBeaconBlock{child})
	require.Empty(t, roots)
}

// Below the EL's validation depth the sweep still sends the payload to the EL, which inserts it, and
// then stops at that root while it stays below the depth.
func TestVerifyUnverifiedGloasPayloadsAsksTheELOnceBelowItsValidationDepth(t *testing.T) {
	cfg, graph, headRoot, _, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	engine.payloadStatus = execution_client.PayloadStatusNotValidated
	headBlock := graph.parents[headRoot]
	const number = 10
	graph.envelopes[headRoot].Message.Payload.BlockNumber = number
	delete(graph.parents, headRoot)
	graph.insertOnAdd = true
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), headBlock, false, false, false))
	cfg.gloasVerificationCursor = headRoot
	cfg.executionClient = validationDepthEngine{testExecutionEngine: engine, below: number + 1}

	verifyUnverifiedGloasPayloads(t.Context(), cfg)

	require.Equal(t, 1, engine.newPayloadCalls)
	status, found := cfg.forkChoice.GetRecentExecutionPayloadStatusByRoot(headRoot)
	require.True(t, found)
	require.EqualValues(t, execution_client.PayloadStatusNotValidated, status)
	require.Empty(t, cfg.forkChoice.DrainPendingELPayloads())
	require.Contains(t, cfg.gloasSkippedValidation, headRoot)

	// A later pass stops at the root without reading it again, also when the EL head cannot be read.
	reads := graph.envReads[headRoot]
	verifyUnverifiedGloasPayloads(t.Context(), cfg)
	cfg.executionClient = validationDepthEngine{testExecutionEngine: engine, unknown: true}
	verifyUnverifiedGloasPayloads(t.Context(), cfg)
	require.Equal(t, reads, graph.envReads[headRoot])
	require.Equal(t, 1, engine.newPayloadCalls)
	require.Contains(t, cfg.gloasSkippedValidation, headRoot)

	// The depth follows the EL head; once the payload is back above it, the root is checked again.
	cfg.executionClient = validationDepthEngine{testExecutionEngine: engine, below: number}
	verifyUnverifiedGloasPayloads(t.Context(), cfg)
	require.Equal(t, 2, engine.newPayloadCalls)
	require.NotContains(t, cfg.gloasSkippedValidation, headRoot)
}

// Only the EL's own NotValidated answer is final below the depth. An error answer for a root already
// recorded NotValidated keeps that status, but the payload is requeued and checked again.
func TestVerifyUnverifiedGloasPayloadsRetriesAnErrorBelowTheELValidationDepth(t *testing.T) {
	cfg, graph, headRoot, _, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	engine.newPayloadFn = func(context.Context, *cltypes.Eth1Block) (execution_client.PayloadStatus, error) {
		return execution_client.PayloadStatusNone, errors.New("insert failed")
	}
	headBlock := graph.parents[headRoot]
	payload := graph.envelopes[headRoot].Message.Payload
	const number = 10
	payload.BlockNumber = number
	delete(graph.parents, headRoot)
	graph.insertOnAdd = true
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), headBlock, false, false, false))
	cfg.forkChoice.MarkPayloadStatusAndGasLimitIfRetained(headRoot, payload.BlockHash, execution_client.PayloadStatusNotValidated, payload.GasLimit)
	cfg.gloasVerificationCursor = headRoot
	cfg.executionClient = validationDepthEngine{testExecutionEngine: engine, below: number + 1}

	verifyUnverifiedGloasPayloads(t.Context(), cfg)
	verifyUnverifiedGloasPayloads(t.Context(), cfg)

	require.Equal(t, 2, engine.newPayloadCalls)
	require.NotContains(t, cfg.gloasSkippedValidation, headRoot)
	require.NotEmpty(t, cfg.forkChoice.DrainPendingELPayloads())
}

// An EL head at the first blocks validates every block: a known zero depth checks a remembered root
// again, where an unreadable head keeps it.
func TestVerifyUnverifiedGloasPayloadsChecksARememberedRootAtAKnownZeroDepth(t *testing.T) {
	cfg, graph, headRoot, _, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	engine.payloadStatus = execution_client.PayloadStatusNotValidated
	headBlock := graph.parents[headRoot]
	payload := graph.envelopes[headRoot].Message.Payload
	delete(graph.parents, headRoot)
	graph.insertOnAdd = true
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), headBlock, false, false, false))
	cfg.gloasSkippedValidation = map[common.Hash]skippedValidation{headRoot: {slot: headBlock.Block.Slot, blockNumber: payload.BlockNumber}}
	cfg.executionClient = validationDepthEngine{testExecutionEngine: engine}

	verifyUnverifiedGloasPayloads(t.Context(), cfg)

	require.Equal(t, 1, engine.newPayloadCalls)
	require.NotContains(t, cfg.gloasSkippedValidation, headRoot)
}

// A selected head remembered below the depth is not sent again while the cursor walks its ancestors.
func TestVerifyUnverifiedGloasPayloadsSkipsARememberedSelectedHead(t *testing.T) {
	cfg, graph, parentRoot, child, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusNotValidated, false)
	parent := graph.parents[parentRoot]
	parentEnvelope := graph.envelopes[parentRoot]
	delete(graph.parents, parentRoot)
	graph.insertOnAdd = true
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), parent, false, false, false))
	cfg.forkChoice.MarkPayloadStatus(parentRoot, parentEnvelope.Message.Payload.BlockHash, execution_client.PayloadStatusValidated)
	childPayloadHash := common.Hash{2}
	child.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = childPayloadHash
	childRoot, err := child.Block.HashSSZ()
	require.NoError(t, err)
	encoded, err := parentEnvelope.EncodeSSZ(nil)
	require.NoError(t, err)
	childEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg.beaconCfg)}
	require.NoError(t, childEnvelope.DecodeSSZ(encoded, int(clparams.GloasVersion)))
	childEnvelope.Message.BeaconBlockRoot = childRoot
	childEnvelope.Message.Payload.BlockHash = childPayloadHash
	graph.envelopes[childRoot] = childEnvelope
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), child, false, false, false))
	cfg.gloasVerificationHead = childRoot
	cfg.executionClient = validationDepthEngine{testExecutionEngine: engine, below: 1 << 30}
	cfg.gloasSkippedValidation = map[common.Hash]skippedValidation{childRoot: {slot: child.Block.Slot, blockNumber: childEnvelope.Message.Payload.BlockNumber}}

	for range 3 {
		cfg.gloasVerificationCursor = parentRoot
		verifyUnverifiedGloasPayloads(t.Context(), cfg)
	}

	require.Zero(t, engine.newPayloadCalls)
	require.Zero(t, graph.envReads[childRoot])
}

// A remembered root does not prove that its ancestors reached the EL, for example after a batch was
// interrupted on an ancestor. The sweep skips only the remembered root and walks on.
func TestVerifyUnverifiedGloasPayloadsWalksPastARememberedRoot(t *testing.T) {
	cfg, graph, parentRoot, child, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusNotValidated, false)
	parent := graph.parents[parentRoot]
	parentEnvelope := graph.envelopes[parentRoot]
	delete(graph.parents, parentRoot)
	graph.insertOnAdd = true
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), parent, false, false, false))
	cfg.forkChoice.MarkPayloadStatus(parentRoot, parentEnvelope.Message.Payload.BlockHash, execution_client.PayloadStatusNotValidated)
	childPayloadHash := common.Hash{2}
	child.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = childPayloadHash
	childRoot, err := child.Block.HashSSZ()
	require.NoError(t, err)
	encoded, err := parentEnvelope.EncodeSSZ(nil)
	require.NoError(t, err)
	childEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg.beaconCfg)}
	require.NoError(t, childEnvelope.DecodeSSZ(encoded, int(clparams.GloasVersion)))
	childEnvelope.Message.BeaconBlockRoot = childRoot
	childEnvelope.Message.Payload.BlockHash = childPayloadHash
	graph.envelopes[childRoot] = childEnvelope
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), child, false, false, false))
	selectedRoot, _, err := cfg.forkChoice.GetHead(nil)
	require.NoError(t, err)
	require.Equal(t, common.Hash(childRoot), selectedRoot)
	cfg.gloasVerificationHead = childRoot
	cfg.executionClient = validationDepthEngine{testExecutionEngine: engine, below: 1 << 30}
	cfg.gloasSkippedValidation = map[common.Hash]skippedValidation{childRoot: {slot: child.Block.Slot, blockNumber: childEnvelope.Message.Payload.BlockNumber}}

	verifyUnverifiedGloasPayloads(t.Context(), cfg)

	require.Equal(t, 1, engine.newPayloadCalls, "the parent reached the EL")
	require.Equal(t, 1, graph.envReads[parentRoot])
	require.Zero(t, graph.envReads[childRoot], "the remembered root is not read")
}

// The roots recorded below the EL's validation depth are forgotten once finalized.
func TestVerifyUnverifiedGloasPayloadsPrunesSkippedRootsAtFinalization(t *testing.T) {
	cfg, _, _, _, _, _ := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	finalizedSlot := cfg.forkChoice.FinalizedSlot()
	finalized, live := common.Hash{0xf1}, common.Hash{0xf2}
	cfg.gloasSkippedValidation = map[common.Hash]skippedValidation{
		finalized: {slot: finalizedSlot},
		live:      {slot: finalizedSlot + 1},
	}

	verifyUnverifiedGloasPayloads(t.Context(), cfg)

	require.NotContains(t, cfg.gloasSkippedValidation, finalized)
	require.Contains(t, cfg.gloasSkippedValidation, live)
}

func TestVerifyUnverifiedGloasPayloadsChecksDirectExtensionWithEmptyCursor(t *testing.T) {
	cfg, graph, oldHeadRoot, newHead, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	oldHead := graph.parents[oldHeadRoot]
	oldEnvelope := graph.envelopes[oldHeadRoot]
	delete(graph.parents, oldHeadRoot)
	graph.insertOnAdd = true
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), oldHead, false, false, false))
	cfg.forkChoice.MarkPayloadStatus(oldHeadRoot, oldEnvelope.Message.Payload.BlockHash, execution_client.PayloadStatusValidated)

	newPayloadHash := common.Hash{2}
	newHead.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = newPayloadHash
	newHeadRoot, err := newHead.Block.HashSSZ()
	require.NoError(t, err)
	encodedEnvelope, err := oldEnvelope.EncodeSSZ(nil)
	require.NoError(t, err)
	newEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg.beaconCfg)}
	require.NoError(t, newEnvelope.DecodeSSZ(encodedEnvelope, int(clparams.GloasVersion)))
	newEnvelope.Message.BeaconBlockRoot = newHeadRoot
	newEnvelope.Message.Payload.BlockHash = newPayloadHash
	graph.envelopes[newHeadRoot] = newEnvelope
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), newHead, false, false, false))
	selectedRoot, _, err := cfg.forkChoice.GetHead(nil)
	require.NoError(t, err)
	require.Equal(t, common.Hash(newHeadRoot), selectedRoot)

	cfg.gloasVerificationHead = oldHeadRoot
	verifyUnverifiedGloasPayloads(t.Context(), cfg)

	require.Equal(t, 1, engine.newPayloadCalls)
	require.True(t, cfg.forkChoice.IsPayloadVerified(newHeadRoot))
}

func TestChainTipSyncReverifiesHeadEnvelopeReceivedAtTarget(t *testing.T) {
	cfg, graph, headRoot, _, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	headBlock := graph.parents[headRoot]
	envelope := graph.envelopes[headRoot]
	delete(graph.parents, headRoot)
	graph.insertOnAdd = true
	require.NoError(t, cfg.forkChoice.OnBlock(t.Context(), headBlock, false, false, false))
	delete(graph.envelopes, headRoot)
	peerRPC, sentinel := newChainTipBatchEnvelopeRPC(t, cfg.beaconCfg, envelope)
	cfg.rpc = peerRPC
	engine.newPayloadFn = func(context.Context, *cltypes.Eth1Block) (execution_client.PayloadStatus, error) {
		if engine.newPayloadCalls == 1 {
			return execution_client.PayloadStatusNone, errors.New("engine unavailable")
		}
		return execution_client.PayloadStatusValidated, nil
	}

	selectedRoot, _, err := cfg.forkChoice.GetHead(nil)
	require.NoError(t, err)
	require.Equal(t, headRoot, selectedRoot)
	require.False(t, cfg.forkChoice.HasEnvelope(headRoot))

	targetSlot := headBlock.Block.Slot
	err = chainTipSync(t.Context(), log.Root(), cfg, Args{seenSlot: targetSlot, targetSlot: targetSlot})

	require.NoError(t, err)
	require.Equal(t, 1, sentinel.calls)
	require.Equal(t, 2, engine.newPayloadCalls)
	require.True(t, cfg.forkChoice.IsPayloadVerified(headRoot))
}

func newChainTipBatchEnvelopeRPC(t *testing.T, cfg *clparams.BeaconChainConfig, envelope *cltypes.SignedExecutionPayloadEnvelope) (*rpc.BeaconRpcP2P, *chainTipBatchEnvelopeSentinel) {
	t.Helper()

	clock := eth_clock.NewEthereumClock(0, common.Hash{}, cfg)
	sentinel := &chainTipBatchEnvelopeSentinel{response: encodeEnvelopeResponse(t, cfg, envelope)}
	return rpc.NewBeaconRpcP2P(t.Context(), sentinel, cfg, clock), sentinel
}

func encodeEnvelopeResponse(t *testing.T, cfg *clparams.BeaconChainConfig, envelope *cltypes.SignedExecutionPayloadEnvelope) []byte {
	t.Helper()

	clock := eth_clock.NewEthereumClock(0, common.Hash{}, cfg)
	digest, err := clock.ComputeForkDigest(cfg.GloasForkEpoch)
	require.NoError(t, err)
	var response bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, envelope, digest[:]...))
	return response.Bytes()
}

func TestChainTipBatchReplayBudgetSkipsParentsWithUsableVerdict(t *testing.T) {
	for _, test := range []struct {
		name    string
		verdict execution_client.PayloadStatus
	}{
		{name: "not validated", verdict: execution_client.PayloadStatusNotValidated},
		{name: "validated", verdict: execution_client.PayloadStatusValidated},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, graph, parentARoot, childA, _, engine := newChainTipBatchFixture(t, execution_client.PayloadStatusValidated)
			parentA := graph.parents[parentARoot]
			parentB := cltypes.NewSignedBeaconBlock(cfg.beaconCfg, clparams.GloasVersion)
			parentB.Block.Slot = parentA.Block.Slot
			parentB.Block.ParentRoot = parentA.Block.ParentRoot
			parentB.Block.Body.GetSignedExecutionPayloadBid().Message = parentA.Block.Body.GetSignedExecutionPayloadBid().Message.Copy()
			parentB.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = common.Hash{2}
			parentBRoot, err := parentB.Block.HashSSZ()
			require.NoError(t, err)

			encodedEnvelope, err := graph.envelopes[parentARoot].EncodeSSZ(nil)
			require.NoError(t, err)
			envelopeB := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg.beaconCfg)}
			require.NoError(t, envelopeB.DecodeSSZ(encodedEnvelope, int(clparams.GloasVersion)))
			envelopeB.Message.BeaconBlockRoot = parentBRoot
			envelopeB.Message.Payload.BlockHash = parentB.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash
			graph.parents[parentBRoot] = parentB
			graph.envelopes[parentBRoot] = envelopeB
			cfg.forkChoice.MarkPayloadStatus(parentBRoot, envelopeB.Message.Payload.BlockHash, test.verdict)
			_, found := cfg.forkChoice.GetExecutionPayloadGasLimit(envelopeB.Message.Payload.BlockHash)
			require.False(t, found)

			childB := cltypes.NewSignedBeaconBlock(cfg.beaconCfg, clparams.GloasVersion)
			childB.Block.Slot = parentB.Block.Slot + 1
			childB.Block.ParentRoot = parentBRoot
			childB.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = parentB.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash
			childARoot, err := childA.Block.HashSSZ()
			require.NoError(t, err)
			childBRoot, err := childB.Block.HashSSZ()
			require.NoError(t, err)

			var replayedPayload common.Hash
			var replayBudget time.Duration
			engine.newPayloadFn = func(ctx context.Context, payload *cltypes.Eth1Block) (execution_client.PayloadStatus, error) {
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				replayedPayload = payload.BlockHash
				replayBudget = time.Until(deadline)
				return execution_client.PayloadStatusValidated, nil
			}

			seen := make(map[common.Hash]struct{})
			processChainTipBatch(t.Context(), cfg, Args{targetSlot: childB.Block.Slot + 1}, []*cltypes.SignedBeaconBlock{childA, childB}, seen, make(map[common.Hash]error))

			require.Equal(t, 1, engine.newPayloadCalls)
			require.Equal(t, graph.envelopes[parentARoot].Message.Payload.BlockHash, replayedPayload)
			require.Greater(t, replayBudget, gloasPayloadRetryBudget*3/4)
			require.Equal(t, 1, graph.added[common.Hash(childARoot)])
			require.Equal(t, 1, graph.added[common.Hash(childBRoot)])
		})
	}
}

func TestChainTipBatchReplayBudgetStopsAtTarget(t *testing.T) {
	cfg, graph, parentARoot, childA, _, engine := newChainTipBatchFixture(t, execution_client.PayloadStatusValidated)
	parentA := graph.parents[parentARoot]
	parentB := cltypes.NewSignedBeaconBlock(cfg.beaconCfg, clparams.GloasVersion)
	parentB.Block.Slot = parentA.Block.Slot
	parentB.Block.ParentRoot = parentA.Block.ParentRoot
	parentB.Block.Body.GetSignedExecutionPayloadBid().Message = parentA.Block.Body.GetSignedExecutionPayloadBid().Message.Copy()
	parentB.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = common.Hash{2}
	parentBRoot, err := parentB.Block.HashSSZ()
	require.NoError(t, err)

	encodedEnvelope, err := graph.envelopes[parentARoot].EncodeSSZ(nil)
	require.NoError(t, err)
	envelopeB := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg.beaconCfg)}
	require.NoError(t, envelopeB.DecodeSSZ(encodedEnvelope, int(clparams.GloasVersion)))
	envelopeB.Message.BeaconBlockRoot = parentBRoot
	envelopeB.Message.Payload.BlockHash = parentB.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash
	graph.parents[parentBRoot] = parentB
	graph.envelopes[parentBRoot] = envelopeB
	cfg.forkChoice.MarkPayloadStatus(parentBRoot, envelopeB.Message.Payload.BlockHash, execution_client.PayloadStatusNone)

	childB := cltypes.NewSignedBeaconBlock(cfg.beaconCfg, clparams.GloasVersion)
	childB.Block.Slot = childA.Block.Slot + 1
	childB.Block.ParentRoot = parentBRoot
	childB.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = parentB.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash
	childARoot, err := childA.Block.HashSSZ()
	require.NoError(t, err)
	childBRoot, err := childB.Block.HashSSZ()
	require.NoError(t, err)

	var replayBudget time.Duration
	engine.newPayloadFn = func(ctx context.Context, _ *cltypes.Eth1Block) (execution_client.PayloadStatus, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		replayBudget = time.Until(deadline)
		return execution_client.PayloadStatusValidated, nil
	}

	reachedTarget := processChainTipBatch(
		t.Context(),
		cfg,
		Args{targetSlot: childA.Block.Slot},
		[]*cltypes.SignedBeaconBlock{childA, childB},
		make(map[common.Hash]struct{}),
		make(map[common.Hash]error),
	)

	require.True(t, reachedTarget)
	require.Equal(t, 1, engine.newPayloadCalls)
	require.Greater(t, replayBudget, gloasPayloadRetryBudget*3/4)
	require.Equal(t, 1, graph.added[common.Hash(childARoot)])
	require.Zero(t, graph.added[common.Hash(childBRoot)])
}

func TestChainTipBatchRejectsFullChildOfRejectedAnchor(t *testing.T) {
	for _, test := range []struct {
		name            string
		alreadyRejected bool
		wantRequests    int
	}{
		{name: "already rejected", alreadyRejected: true},
		{name: "rejected during recovery", wantRequests: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, graph, parentRoot, child, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
			anchor := graph.parentState
			bid := anchor.GetLatestExecutionPayloadBid().Copy()
			bid.BlobKzgCommitments.Append(&cltypes.KZGCommitment{1})
			anchor.SetLatestExecutionPayloadBid(bid)
			store := newAnchorForkChoiceStore(t, cfg.beaconCfg, anchor, engine)
			anchorRoot := store.AnchorRoot()
			child.Block.ParentRoot = anchorRoot
			store.OnTick((child.Block.Slot + 1) * cfg.beaconCfg.SecondsPerSlot * 1000)
			if test.alreadyRejected {
				store.RejectEnvelope(anchorRoot)
			}
			envelope := graph.envelopes[parentRoot]
			envelope.Message.BeaconBlockRoot = anchorRoot
			key, err := bls.NewPrivateKeyFromIKM([]byte("01234567890123456789012345678901"))
			require.NoError(t, err)
			signAnchorEnvelope(t, anchor, key, envelope, bid.Slot)
			peerRPC, sentinel := newChainTipBatchEnvelopeRPC(t, cfg.beaconCfg, envelope)
			cfg.rpc = peerRPC
			cfg.caplinConfig.DisabledCheckpointSync = true
			cfg.forkChoice = store
			cfg.chainTipRejections = newChainTipRejections(time.Hour, nil)
			cfg.chainTipRejections.lastLog = time.Now()
			root, err := child.Block.HashSSZ()
			require.NoError(t, err)
			seen := make(map[common.Hash]struct{})
			rejected := make(map[common.Hash]error)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			for range 2 {
				reachedTarget := processChainTipBatch(ctx, cfg, Args{targetSlot: child.Block.Slot}, []*cltypes.SignedBeaconBlock{child}, seen, rejected)

				require.False(t, reachedTarget)
				require.True(t, store.IsEnvelopeRejected(anchorRoot))
				require.ErrorIs(t, rejected[root], forkchoice.ErrBlockInvalid)
				require.ErrorContains(t, rejected[root], "parent execution payload envelope is rejected")
				require.Contains(t, seen, common.Hash(root))
				_, imported := store.GetHeader(root)
				require.False(t, imported)
				require.Zero(t, engine.newPayloadCalls)
				require.Equal(t, test.wantRequests, sentinel.calls)
			}
			require.Equal(t, 1, cfg.chainTipRejections.reasons["process block failed"].attempts)
		})
	}
}

func TestChainTipBatchReplaysStoredParentPayload(t *testing.T) {
	t.Run("peer-fetched unavailable parent leaves full child unpersisted", func(t *testing.T) {
		cfg, graph, parentRoot, fullChild, _, engine := newChainTipBatchFixture(t, execution_client.PayloadStatusNone)
		fullRoot, err := fullChild.Block.HashSSZ()
		require.NoError(t, err)
		envelope := graph.envelopes[parentRoot]
		peerRPC, sentinel := newChainTipBatchEnvelopeRPC(t, cfg.beaconCfg, envelope)
		cfg.rpc = peerRPC
		delete(graph.envelopes, parentRoot)
		seen := make(map[common.Hash]struct{})

		processChainTipBatch(t.Context(), cfg, Args{targetSlot: fullChild.Block.Slot + 1}, []*cltypes.SignedBeaconBlock{fullChild}, seen, make(map[common.Hash]error))

		require.Equal(t, 1, sentinel.calls)
		require.Equal(t, 1, engine.newPayloadCalls)
		var storedSlot *uint64
		require.NoError(t, cfg.indiciesDB.View(t.Context(), func(tx kv.Tx) error {
			storedSlot, err = beacon_indicies.ReadBlockSlotByBlockRoot(tx, common.Hash(fullRoot))
			return err
		}))
		require.Nil(t, storedSlot)
		require.Zero(t, graph.added[common.Hash(fullRoot)])
		require.NotContains(t, seen, common.Hash(fullRoot))
	})

	t.Run("valid replay processes full child", func(t *testing.T) {
		cfg, graph, parentRoot, fullChild, _, engine := newChainTipBatchFixture(t, execution_client.PayloadStatusValidated)
		fullRoot, err := fullChild.Block.HashSSZ()
		require.NoError(t, err)
		seen := make(map[common.Hash]struct{})

		reachedTarget := processChainTipBatch(t.Context(), cfg, Args{targetSlot: fullChild.Block.Slot + 1}, []*cltypes.SignedBeaconBlock{fullChild}, seen, make(map[common.Hash]error))

		require.False(t, reachedTarget)
		require.Equal(t, 1, engine.newPayloadCalls)
		require.Equal(t, 1, graph.added[common.Hash(fullRoot)])
		require.Contains(t, seen, common.Hash(fullRoot))
		status, found := cfg.forkChoice.GetRecentExecutionPayloadStatusByRoot(parentRoot)
		require.True(t, found)
		require.EqualValues(t, execution_client.PayloadStatusValidated, status)
	})

	t.Run("missing replay verdict preserves full child for retry", func(t *testing.T) {
		cfg, graph, _, fullChild, _, engine := newChainTipBatchFixture(t, execution_client.PayloadStatusNone)
		fullRoot, err := fullChild.Block.HashSSZ()
		require.NoError(t, err)
		seen := make(map[common.Hash]struct{})

		processChainTipBatch(t.Context(), cfg, Args{targetSlot: fullChild.Block.Slot + 1}, []*cltypes.SignedBeaconBlock{fullChild}, seen, make(map[common.Hash]error))

		require.Equal(t, 1, engine.newPayloadCalls)
		require.Zero(t, graph.added[common.Hash(fullRoot)])
		require.NotContains(t, seen, common.Hash(fullRoot))
	})

	for _, test := range []struct {
		name       string
		emptyFirst bool
	}{
		{name: "full then empty"},
		{name: "empty then full", emptyFirst: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, graph, _, fullChild, emptyChild, engine := newChainTipBatchFixture(t, execution_client.PayloadStatusNone)
			fullRoot, err := fullChild.Block.HashSSZ()
			require.NoError(t, err)
			emptyRoot, err := emptyChild.Block.HashSSZ()
			require.NoError(t, err)
			seen := make(map[common.Hash]struct{})
			blocks := []*cltypes.SignedBeaconBlock{fullChild, emptyChild}
			if test.emptyFirst {
				blocks = []*cltypes.SignedBeaconBlock{emptyChild, fullChild}
			}

			processChainTipBatch(t.Context(), cfg, Args{targetSlot: fullChild.Block.Slot + 1}, blocks, seen, make(map[common.Hash]error))

			require.Equal(t, 1, engine.newPayloadCalls)
			require.Zero(t, graph.added[common.Hash(fullRoot)])
			require.Equal(t, 1, graph.added[common.Hash(emptyRoot)])
			require.NotContains(t, seen, common.Hash(fullRoot))
			require.Contains(t, seen, common.Hash(emptyRoot))
		})
	}
}

func TestStoredParentEnvelopesRestoresReadableParents(t *testing.T) {
	storedRoot := common.Hash{1}
	revokedRoot := common.Hash{2}
	transientRoot := common.Hash{3}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: storedRoot}}
	present := map[common.Hash]bool{
		storedRoot:    true,
		revokedRoot:   true,
		transientRoot: true,
	}

	got, storedRoots := storedParentEnvelopes(
		[][32]byte{storedRoot, revokedRoot, transientRoot},
		func(root common.Hash) bool { return present[root] },
		func(root common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
			if root == storedRoot {
				return envelope, nil
			}
			if root == transientRoot {
				return nil, context.DeadlineExceeded
			}
			present[root] = false
			return &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: common.Hash{9}}}, nil
		},
	)

	require.Equal(t, map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{storedRoot: envelope}, got)
	require.Equal(t, map[common.Hash]struct{}{storedRoot: {}, transientRoot: {}}, storedRoots)
}

func TestStoredParentReplayRootsExcludeKnownAndIncludeTargetChildren(t *testing.T) {
	knownParent := common.Hash{1}
	unknownParent := common.Hash{2}
	parent := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	knownChild := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	knownChild.Block.ParentRoot = knownParent
	unknownChild := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	unknownChild.Block.ParentRoot = unknownParent
	knownChildRoot, err := knownChild.Block.HashSSZ()
	require.NoError(t, err)
	envelopes := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{
		knownParent:   {},
		unknownParent: {},
	}
	storedRoots := map[common.Hash]struct{}{knownParent: {}, unknownParent: {}}

	got := storedParentReplayRoots(
		[]*cltypes.SignedBeaconBlock{knownChild, unknownChild},
		^uint64(0),
		envelopes,
		storedRoots,
		func(common.Hash) *cltypes.SignedBeaconBlock { return parent },
		func(root common.Hash) bool { return root == common.Hash(knownChildRoot) },
		func(common.Hash) bool { return false },
	)

	require.Equal(t, map[common.Hash]struct{}{unknownParent: {}}, got)

	got = storedParentReplayRoots(
		[]*cltypes.SignedBeaconBlock{unknownChild},
		unknownChild.Block.Slot,
		envelopes,
		storedRoots,
		func(common.Hash) *cltypes.SignedBeaconBlock { return parent },
		func(common.Hash) bool { return false },
		func(common.Hash) bool { return false },
	)

	require.Equal(t, map[common.Hash]struct{}{unknownParent: {}}, got)
}

func TestStoredParentReplayRootsExcludeEmptyChildren(t *testing.T) {
	parent1 := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	parent1.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = common.Hash{1}
	parent1Root, err := parent1.Block.HashSSZ()
	require.NoError(t, err)
	parent2 := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	parent2.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = common.Hash{2}
	parent2Root, err := parent2.Block.HashSSZ()
	require.NoError(t, err)

	emptyChild := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	emptyChild.Block.ParentRoot = parent1Root
	emptyChild.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = common.Hash{3}
	knownFullChild := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	knownFullChild.Block.ParentRoot = parent1Root
	knownFullChild.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = common.Hash{1}
	knownFullChildRoot, err := knownFullChild.Block.HashSSZ()
	require.NoError(t, err)
	unknownFullChild := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	unknownFullChild.Block.ParentRoot = parent2Root
	unknownFullChild.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = common.Hash{2}

	envelopes := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{
		common.Hash(parent1Root): {},
		common.Hash(parent2Root): {},
	}
	storedRoots := map[common.Hash]struct{}{
		common.Hash(parent1Root): {},
		common.Hash(parent2Root): {},
	}
	parents := map[common.Hash]*cltypes.SignedBeaconBlock{
		common.Hash(parent1Root): parent1,
		common.Hash(parent2Root): parent2,
	}
	parentBlock := func(root common.Hash) *cltypes.SignedBeaconBlock { return parents[root] }
	knownBlock := func(root common.Hash) bool { return root == common.Hash(knownFullChildRoot) }
	seenBlock := func(common.Hash) bool { return false }

	got := storedParentReplayRoots(
		[]*cltypes.SignedBeaconBlock{emptyChild, knownFullChild, parent2, unknownFullChild},
		^uint64(0),
		envelopes,
		storedRoots,
		parentBlock,
		knownBlock,
		seenBlock,
	)
	require.Equal(t, map[common.Hash]struct{}{common.Hash(parent2Root): {}}, got)

	got = storedParentReplayRoots(
		[]*cltypes.SignedBeaconBlock{emptyChild},
		^uint64(0),
		envelopes,
		storedRoots,
		parentBlock,
		knownBlock,
		seenBlock,
	)
	require.Empty(t, got)
}

func TestParentEnvelopeRequiredOnlyForFullBranch(t *testing.T) {
	parent := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	parent.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = common.Hash{1}
	child := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	child.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = common.Hash{1}

	require.True(t, parentEnvelopeRequired(child, parent))
	child.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = common.Hash{2}
	require.False(t, parentEnvelopeRequired(child, parent))
	require.False(t, parentEnvelopeRequired(nil, parent))
	require.False(t, parentEnvelopeRequired(child, nil))
	child.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = common.Hash{1}
	require.False(t, parentEnvelopeNeedsRecovery(child, parent, true, execution_client.PayloadStatusValidated, true))
	require.False(t, parentEnvelopeNeedsRecovery(child, parent, true, execution_client.PayloadStatusNotValidated, true))
	require.True(t, parentEnvelopeNeedsRecovery(child, parent, true, execution_client.PayloadStatusNone, true))
	require.True(t, parentEnvelopeNeedsRecovery(child, parent, true, execution_client.PayloadStatusNone, false))
	require.False(t, parentEnvelopeNeedsRecovery(child, parent, true, execution_client.PayloadStatusInvalidated, true))
	require.False(t, parentEnvelopeNeedsRecovery(child, parent, false, execution_client.PayloadStatusInvalidated, true))
	require.True(t, parentEnvelopeNeedsRecovery(child, parent, false, execution_client.PayloadStatusNone, false))
}

func TestEnsureStoredParentPayloadAcceptedReplaysMissingVerdict(t *testing.T) {
	root := common.Hash{1}
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	payload.BlockHash = common.Hash{2}
	payload.GasLimit = 36_000_000
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		BeaconBlockRoot: root,
		Payload:         payload,
	}}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	store := &storedParentPayloadTestStore{
		has:          true,
		status:       execution_client.PayloadStatusNone,
		statusFound:  true,
		block:        block,
		markRetained: true,
	}
	engine := &testExecutionEngine{payloadStatus: execution_client.PayloadStatusNotValidated}
	cfg := &Cfg{
		beaconCfg:             &clparams.MainnetBeaconConfig,
		executionClient:       engine,
		gloasPayloadValidator: engine,
	}

	reason := ensureStoredParentPayloadAccepted(t.Context(), cfg, store, root, envelope)

	require.NoError(t, reason)
	require.Equal(t, 1, engine.newPayloadCalls)
	require.EqualValues(t, execution_client.PayloadStatusNotValidated, store.marked)
	require.Equal(t, payload.GasLimit, store.markedGas)
	require.Len(t, store.requeued, 1)
}

func TestEnsureStoredParentPayloadAcceptedDoesNotCacheBlobMismatch(t *testing.T) {
	root := common.Hash{1}
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	payload.BlockHash = common.Hash{2}
	payload.GasLimit = 36_000_000
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		BeaconBlockRoot: root,
		Payload:         payload,
	}}
	store := &storedParentPayloadTestStore{
		has:          true,
		block:        cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion),
		markRetained: true,
	}
	engine := &testExecutionEngine{newPayloadFn: func(context.Context, *cltypes.Eth1Block) (execution_client.PayloadStatus, error) {
		return execution_client.PayloadStatusInvalidated, misc.ErrMismatchBlobHashes
	}}
	cfg := &Cfg{
		beaconCfg:             &clparams.MainnetBeaconConfig,
		executionClient:       engine,
		gloasPayloadValidator: engine,
	}

	require.ErrorIs(t, ensureStoredParentPayloadAccepted(t.Context(), cfg, store, root, envelope), misc.ErrMismatchBlobHashes)
	require.Error(t, ensureStoredParentPayloadAccepted(t.Context(), cfg, store, root, envelope))
	require.Equal(t, 1, engine.newPayloadCalls)
	require.Zero(t, store.markedGas)
	require.Zero(t, store.marked)
	require.Empty(t, store.requeued)
}

func TestEnsureStoredParentPayloadAcceptedRejectsInvalidVerdict(t *testing.T) {
	root := common.Hash{1}
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		BeaconBlockRoot: root,
		Payload:         payload,
	}}
	store := &storedParentPayloadTestStore{
		has:         true,
		status:      execution_client.PayloadStatusInvalidated,
		statusFound: true,
		block:       cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion),
	}

	reason := ensureStoredParentPayloadAccepted(t.Context(), &Cfg{}, store, root, envelope)

	require.Error(t, reason)
	require.Empty(t, store.requeued)
}

func TestEnsureStoredParentPayloadAcceptedRestoresGasLimitForKnownVerdict(t *testing.T) {
	root := common.Hash{1}
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	payload.BlockHash = common.Hash{2}
	payload.GasLimit = 36_000_000
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		BeaconBlockRoot: root,
		Payload:         payload,
	}}
	store := &storedParentPayloadTestStore{
		has:          true,
		status:       execution_client.PayloadStatusNotValidated,
		statusFound:  true,
		markRetained: true,
	}

	reason := ensureStoredParentPayloadAccepted(t.Context(), &Cfg{}, store, root, envelope)

	require.NoError(t, reason)
	require.Equal(t, payload.GasLimit, store.markedGas)
}

func TestEnsureStoredParentPayloadAcceptedWithoutExecutionClient(t *testing.T) {
	root := common.Hash{1}
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		BeaconBlockRoot: root,
		Payload:         payload,
	}}
	store := &storedParentPayloadTestStore{
		has:          true,
		block:        cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion),
		markRetained: true,
	}

	reason := ensureStoredParentPayloadAccepted(t.Context(), &Cfg{}, store, root, envelope)

	require.NoError(t, reason)
	require.EqualValues(t, execution_client.PayloadStatusNotValidated, store.marked)
	require.Empty(t, store.requeued)
}

func TestStoredParentPayloadReplaySharesBudgetAndCachesResult(t *testing.T) {
	root := common.Hash{1}
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		BeaconBlockRoot: root,
		Payload:         payload,
	}}
	store := &storedParentPayloadTestStore{
		has:          true,
		block:        cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion),
		markRetained: true,
	}
	replay := storedParentPayloadReplay{
		deadline: time.Now().Add(10 * time.Millisecond),
		results:  make(map[common.Hash]error),
	}
	engine := &testExecutionEngine{}
	engine.newPayloadFn = func(ctx context.Context, _ *cltypes.Eth1Block) (execution_client.PayloadStatus, error) {
		<-ctx.Done()
		return execution_client.PayloadStatusNone, ctx.Err()
	}
	cfg := &Cfg{
		beaconCfg:             &clparams.MainnetBeaconConfig,
		executionClient:       engine,
		gloasPayloadValidator: engine,
	}

	require.Error(t, replay.rejection(t.Context(), cfg, store, root, envelope, forkchoice.ErrIgnore))
	require.Error(t, replay.rejection(t.Context(), cfg, store, root, envelope, forkchoice.ErrIgnore))
	require.Equal(t, 1, engine.newPayloadCalls)
}

func TestStoredParentPayloadReplayReservesBudgetForLaterRoots(t *testing.T) {
	firstRoot := common.Hash{1}
	secondRoot := common.Hash{2}
	firstPayload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	firstPayload.BlockHash = common.Hash{3}
	secondPayload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	secondPayload.BlockHash = common.Hash{4}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	firstStore := &storedParentPayloadTestStore{has: true, block: block, markRetained: true}
	secondStore := &storedParentPayloadTestStore{has: true, block: block, markRetained: true}
	validator := &orderedPayloadValidator{slow: firstPayload.BlockHash}
	replay := storedParentPayloadReplay{
		deadline:  time.Now().Add(100 * time.Millisecond),
		remaining: 2,
		results:   make(map[common.Hash]error),
	}
	cfg := &Cfg{
		beaconCfg:             &clparams.MainnetBeaconConfig,
		executionClient:       &testExecutionEngine{},
		gloasPayloadValidator: validator,
	}

	require.Error(t, replay.rejection(t.Context(), cfg, firstStore, firstRoot, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		BeaconBlockRoot: firstRoot,
		Payload:         firstPayload,
	}}, nil))
	require.NoError(t, replay.rejection(t.Context(), cfg, secondStore, secondRoot, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		BeaconBlockRoot: secondRoot,
		Payload:         secondPayload,
	}}, nil))
	require.Equal(t, []common.Hash{firstPayload.BlockHash, secondPayload.BlockHash}, validator.calls)
}

func TestStoredParentPayloadReplayRejectsApplyFailure(t *testing.T) {
	root := common.Hash{1}
	store := &storedParentPayloadTestStore{has: true, markRetained: true}
	replay := storedParentPayloadReplay{deadline: time.Now(), remaining: 1, results: make(map[common.Hash]error)}

	reason := replay.rejection(
		t.Context(),
		&Cfg{},
		store,
		root,
		&cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
			BeaconBlockRoot: root,
			Payload:         cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig),
		}},
		forkchoice.ErrInvalidExecutionPayloadEnvelope,
	)

	require.ErrorIs(t, reason, forkchoice.ErrInvalidExecutionPayloadEnvelope)
	require.EqualValues(t, execution_client.PayloadStatusNone, store.marked)
	require.Zero(t, replay.remaining)
}

func TestStoredParentPayloadReplayWithoutVerdictKeepsStatus(t *testing.T) {
	const unchanged = execution_client.PayloadStatus(99)
	root := common.Hash{1}
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	payload.Transactions = solid.NewProgressiveTransactionsSSZFromTransactions(nil)
	payload.BlockHash = common.Hash{2}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		BeaconBlockRoot: root,
		Payload:         payload,
	}}
	newStore := func() *storedParentPayloadTestStore {
		return &storedParentPayloadTestStore{
			has:          true,
			block:        cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion),
			markRetained: true,
			marked:       unchanged,
		}
	}

	t.Run("exhausted budget", func(t *testing.T) {
		store := newStore()
		engine := &testExecutionEngine{payloadStatus: execution_client.PayloadStatusValidated}
		cfg := &Cfg{beaconCfg: &clparams.MainnetBeaconConfig, executionClient: engine, gloasPayloadValidator: engine}
		replay := storedParentPayloadReplay{
			budget:    gloasPayloadRetryBudget,
			deadline:  time.Now().Add(-time.Millisecond),
			remaining: 1,
			results:   make(map[common.Hash]error),
		}

		require.Error(t, replay.rejection(t.Context(), cfg, store, root, envelope, forkchoice.ErrIgnore))
		require.Zero(t, engine.newPayloadCalls)
		require.Equal(t, unchanged, store.marked)
		require.Empty(t, store.requeued)
	})

	t.Run("deadline during validation", func(t *testing.T) {
		store := newStore()
		// The EL call goes through a real store's admission, which classifies the interrupted call.
		fixture, _, _, _, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
		engine.newPayloadFn = func(ctx context.Context, _ *cltypes.Eth1Block) (execution_client.PayloadStatus, error) {
			<-ctx.Done()
			return execution_client.PayloadStatusNone, ctx.Err()
		}
		cfg := &Cfg{beaconCfg: &clparams.MainnetBeaconConfig, executionClient: engine, gloasPayloadValidator: fixture.forkChoice}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel()

		require.Error(t, ensureStoredParentPayloadAccepted(ctx, cfg, store, root, envelope))
		require.Equal(t, 1, engine.newPayloadCalls)
		require.Equal(t, unchanged, store.marked)
		require.Empty(t, store.requeued)
	})
}

// A stored parent below the EL's validation depth is sent to the EL, which inserts it, and is not
// requeued after its NotValidated answer.
func TestEnsureStoredParentPayloadAcceptedDoesNotRequeueBelowTheELValidationDepth(t *testing.T) {
	cfg, graph, headRoot, _, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	engine.payloadStatus = execution_client.PayloadStatusNotValidated
	envelope := graph.envelopes[headRoot]
	const number = 10
	envelope.Message.Payload.BlockNumber = number
	for _, tc := range []struct {
		name     string
		below    uint64
		requeued bool
	}{
		{name: "below the depth", below: number + 1},
		{name: "at the depth", below: number, requeued: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &storedParentPayloadTestStore{has: true, block: graph.parents[headRoot], markRetained: true}
			calls := engine.newPayloadCalls
			cfg.executionClient = validationDepthEngine{testExecutionEngine: engine, below: tc.below}

			require.NoError(t, ensureStoredParentPayloadAccepted(t.Context(), cfg, store, headRoot, envelope))
			require.Equal(t, calls+1, engine.newPayloadCalls)
			require.EqualValues(t, execution_client.PayloadStatusNotValidated, store.marked)
			require.Equal(t, tc.requeued, len(store.requeued) == 1)
		})
	}
}

// The cause of a rejected block is passed on to the descendants that cannot attach, across
// batches of one run, instead of a bare missing parent.
func TestChainTipBatchPassesARejectionToDescendants(t *testing.T) {
	cfg, _, _, fullChild, _, _ := newChainTipBatchFixture(t, execution_client.PayloadStatusInvalidated)
	fullRoot, err := fullChild.Block.HashSSZ()
	require.NoError(t, err)
	grandchild, grandchildRoot := newChildOnEmptyParent(t, cfg.beaconCfg, fullChild, fullChild.Block.Slot+1)
	greatGrandchild, greatGrandchildRoot := newChildOnEmptyParent(t, cfg.beaconCfg, grandchild, grandchild.Block.Slot+1)
	seen := make(map[common.Hash]struct{})
	rejected := make(map[common.Hash]error)
	args := Args{targetSlot: greatGrandchild.Block.Slot + 1}
	// A recent summary keeps the rejections aggregated, so the recorded labels can be read.
	cfg.chainTipRejections = newChainTipRejections(time.Hour, nil)
	cfg.chainTipRejections.lastLog = time.Now()

	processChainTipBatch(t.Context(), cfg, args, []*cltypes.SignedBeaconBlock{fullChild, grandchild}, seen, rejected)
	reason := rejected[common.Hash(fullRoot)]
	require.ErrorContains(t, reason, "parent payload invalid")
	require.Equal(t, reason, rejected[grandchildRoot])

	processChainTipBatch(t.Context(), cfg, args, []*cltypes.SignedBeaconBlock{greatGrandchild}, seen, rejected)
	require.Equal(t, reason, rejected[greatGrandchildRoot])

	replay := cfg.chainTipRejections.reasons["parent payload not accepted"]
	require.NotNil(t, replay)
	require.Equal(t, common.Hash(fullRoot), replay.lastErrRoot)
	missing := cfg.chainTipRejections.reasons["parent not in fork graph"]
	require.NotNil(t, missing)
	require.Equal(t, 2, missing.attempts)
	require.Equal(t, greatGrandchildRoot, missing.lastRoot)
	require.ErrorIs(t, missing.lastErr, reason)
}

func TestChainTipBatchPreservesTerminalParentError(t *testing.T) {
	for _, tc := range []struct {
		name           string
		rejectEnvelope bool
		wantError      string
	}{
		{name: "rejected envelope", rejectEnvelope: true, wantError: "parent execution payload envelope is rejected"},
		{name: "invalid payload", wantError: "parent execution payload is invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, graph, parentRoot, child, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
			payloadHash := graph.envelopes[parentRoot].Message.Payload.BlockHash
			delete(graph.envelopes, parentRoot)
			if tc.rejectEnvelope {
				cfg.forkChoice.RejectEnvelope(parentRoot)
			} else {
				cfg.forkChoice.MarkPayloadInvalid(parentRoot, payloadHash)
			}
			childRoot, err := child.Block.HashSSZ()
			require.NoError(t, err)
			grandchild, grandchildRoot := newChildOnEmptyParent(t, cfg.beaconCfg, child, child.Block.Slot+1)
			cfg.chainTipRejections = newChainTipRejections(time.Hour, nil)
			cfg.chainTipRejections.lastLog = time.Now()
			seen := make(map[common.Hash]struct{})
			rejected := make(map[common.Hash]error)

			reachedTarget := processChainTipBatch(t.Context(), cfg, Args{targetSlot: grandchild.Block.Slot + 1}, []*cltypes.SignedBeaconBlock{child, grandchild}, seen, rejected)

			require.False(t, reachedTarget)
			cause := rejected[childRoot]
			require.ErrorIs(t, cause, forkchoice.ErrBlockInvalid)
			require.ErrorContains(t, cause, tc.wantError)
			require.Equal(t, cause, rejected[grandchildRoot])
			require.Contains(t, seen, common.Hash(childRoot))
			_, imported := cfg.forkChoice.GetHeader(childRoot)
			require.False(t, imported)
			require.NotContains(t, cfg.chainTipRejections.reasons, errParentEnvelopeUnavailable.Error())
			for _, reason := range []string{"process block failed", "parent not in fork graph"} {
				summary := cfg.chainTipRejections.reasons[reason]
				require.NotNil(t, summary)
				require.ErrorIs(t, summary.lastErr, cause)
			}
			require.Zero(t, engine.newPayloadCalls)
		})
	}
}

// The fetched envelope of a FULL parent that is only in the batch is validated against that batch block.
func TestChainTipBatchImportsChildOfBatchParentWithPeerEnvelope(t *testing.T) {
	cfg, graph, parentRoot, fullChild, _, _ := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	parent := graph.parents[parentRoot]
	delete(graph.parents, parentRoot)
	graph.insertOnAdd = true
	peerRPC, sentinel := newChainTipBatchEnvelopeRPC(t, cfg.beaconCfg, graph.envelopes[parentRoot])
	cfg.rpc = peerRPC
	delete(graph.envelopes, parentRoot)
	fullRoot, err := fullChild.Block.HashSSZ()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	processChainTipBatch(ctx, cfg, Args{targetSlot: fullChild.Block.Slot + 1}, []*cltypes.SignedBeaconBlock{parent, fullChild}, make(map[common.Hash]struct{}), make(map[common.Hash]error))

	require.Equal(t, 1, graph.added[parentRoot])
	require.Equal(t, 1, graph.added[common.Hash(fullRoot)])
	require.Equal(t, 1, sentinel.calls)
}

// A parent envelope that no peer serves stalls the batch until its deadline; the child is reported
// under the missing envelope, not under the deadline that ended its processing.
func TestChainTipBatchReportsAParentEnvelopeNoPeerServes(t *testing.T) {
	cfg, graph, parentRoot, fullChild, _, _ := newChainTipBatchFixture(t, execution_client.PayloadStatusNone)
	fullRoot, err := fullChild.Block.HashSSZ()
	require.NoError(t, err)
	delete(graph.envelopes, parentRoot)
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, cfg.beaconCfg)
	cfg.rpc = rpc.NewBeaconRpcP2P(t.Context(), &chainTipBatchEnvelopeSentinel{}, cfg.beaconCfg, clock)
	cfg.chainTipRejections = newChainTipRejections(time.Hour, nil)
	cfg.chainTipRejections.lastLog = time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	rejected := make(map[common.Hash]error)

	processChainTipBatch(ctx, cfg, Args{targetSlot: fullChild.Block.Slot + 1}, []*cltypes.SignedBeaconBlock{fullChild}, make(map[common.Hash]struct{}), rejected)

	require.ErrorIs(t, rejected[common.Hash(fullRoot)], errParentEnvelopeUnavailable)
	require.Contains(t, cfg.chainTipRejections.reasons, errParentEnvelopeUnavailable.Error())
	require.NotContains(t, cfg.chainTipRejections.reasons, "process block failed")
}

func TestChainTipSyncFlushesCollectorWithStageContext(t *testing.T) {
	cfg, _, headRoot, _, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	engine.supportInsertion = true
	collector := &gloasCollectorTest{}
	cfg.blockCollector = collector
	header, ok := cfg.forkChoice.GetHeader(headRoot)
	require.True(t, ok)
	targetSlot := header.Slot + 1

	ctx, cancel := context.WithCancel(t.Context())
	_ = chainTipSync(ctx, log.Root(), cfg, Args{seenSlot: targetSlot, targetSlot: targetSlot})
	cancel()

	require.Equal(t, 1, collector.flushCalls)
	require.Error(t, collector.flushCtx.Err(), "the flush must stop with the stage context")
}

// A stop that lands before the flush skips it: the first engine call of a flush panics on the
// embedded EL when the context is already cancelled.
func TestChainTipSyncSkipsFlushWhenStopped(t *testing.T) {
	cfg, _, headRoot, _, _, engine := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	engine.supportInsertion = true
	collector := &gloasCollectorTest{}
	cfg.blockCollector = collector
	header, ok := cfg.forkChoice.GetHeader(headRoot)
	require.True(t, ok)
	targetSlot := header.Slot + 1

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_ = chainTipSync(ctx, log.Root(), cfg, Args{seenSlot: targetSlot, targetSlot: targetSlot})

	require.Zero(t, collector.flushCalls)
}

// A chain tip batch imports blocks and parent envelopes without checking their data, so it asks
// the custody backfill for the slots it imported, the parent whose envelope it handled included.
func TestChainTipBatchRequestsCustodyBackfillForImportedSlots(t *testing.T) {
	cfg, graph, parentRoot, child, _, _ := newChainTipBatchFixture(t, execution_client.PayloadStatusValidated)
	parent := graph.parents[parentRoot]
	peerDas := das_mock_services.NewMockPeerDas(gomock.NewController(t))
	peerDas.EXPECT().BackfillCustodyColumns(parent.Block.Slot, child.Block.Slot).Times(1)
	cfg.peerDas = peerDas

	processChainTipBatch(t.Context(), cfg, Args{targetSlot: child.Block.Slot}, []*cltypes.SignedBeaconBlock{child}, make(map[common.Hash]struct{}), make(map[common.Hash]error))

	childRoot, err := child.Block.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, 1, graph.added[common.Hash(childRoot)])
}

// A batch that imports nothing asks for no backfill.
func TestChainTipBatchWithoutImportsRequestsNoCustodyBackfill(t *testing.T) {
	cfg, _, _, child, _, _ := newChainTipBatchFixture(t, execution_client.PayloadStatusValidated)
	cfg.peerDas = das_mock_services.NewMockPeerDas(gomock.NewController(t)) // any call fails the test
	orphan := cltypes.NewSignedBeaconBlock(cfg.beaconCfg, clparams.GloasVersion)
	orphan.Block.Slot = child.Block.Slot
	orphan.Block.ParentRoot = common.Hash{0xde, 0xad}

	processChainTipBatch(t.Context(), cfg, Args{targetSlot: orphan.Block.Slot}, []*cltypes.SignedBeaconBlock{orphan}, make(map[common.Hash]struct{}), make(map[common.Hash]error))
}

// newChildOnEmptyParent builds a block that does not build on the payload of its parent, so that no
// parent envelope is needed to import it.
func newChildOnEmptyParent(t *testing.T, beaconCfg *clparams.BeaconChainConfig, parent *cltypes.SignedBeaconBlock, slot uint64) (*cltypes.SignedBeaconBlock, common.Hash) {
	t.Helper()
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	block := cltypes.NewSignedBeaconBlock(beaconCfg, clparams.GloasVersion)
	block.Block.Slot = slot
	block.Block.ParentRoot = parentRoot
	block.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = parent.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	return block, root
}

const ancestorPeer = "orphan-peer"

type ancestorRequest struct {
	pid          string
	start, count uint64
}

// chainTipAncestorSentinel answers blocks_by_range sent to its one peer and nothing else.
type chainTipAncestorSentinel struct {
	chainTipBatchEnvelopeSentinel
	t        *testing.T
	clock    eth_clock.EthereumClock
	cfg      *clparams.BeaconChainConfig
	serve    func(call int, start, count uint64) []*cltypes.SignedBeaconBlock
	requests []ancestorRequest
	// onRequest runs once the request is recorded.
	onRequest func(ctx context.Context)
}

func (s *chainTipAncestorSentinel) SendPeerRequest(ctx context.Context, req *sentinelproto.RequestDataWithPeer, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	require.NoError(s.t, ctx.Err())
	wire := &cltypes.BeaconBlocksByRangeRequest{}
	require.NoError(s.t, ssz_snappy.DecodeAndReadNoForkDigest(bytes.NewReader(req.Data), wire, clparams.GloasVersion))
	s.requests = append(s.requests, ancestorRequest{pid: req.Pid, start: wire.StartSlot, count: wire.Count})
	if s.onRequest != nil {
		s.onRequest(ctx)
	}
	if req.Pid != ancestorPeer || s.serve == nil {
		return nil, errors.New("peer does not answer")
	}
	var response bytes.Buffer
	for i, block := range s.serve(len(s.requests)-1, wire.StartSlot, wire.Count) {
		digest, err := s.clock.ComputeForkDigest(block.Block.Slot / s.cfg.SlotsPerEpoch)
		require.NoError(s.t, err)
		if i > 0 {
			require.NoError(s.t, response.WriteByte(0))
		}
		require.NoError(s.t, ssz_snappy.EncodeAndWrite(&response, block, digest[:]...))
	}
	return &sentinelproto.ResponseData{Data: response.Bytes(), Peer: &sentinelproto.Peer{Pid: req.Pid}}, nil
}

// servingChain answers with the blocks of chain inside the requested range, at most limit of them when limit is not zero.
func servingChain(limit int, chain ...*cltypes.SignedBeaconBlock) func(int, uint64, uint64) []*cltypes.SignedBeaconBlock {
	return func(_ int, start, count uint64) []*cltypes.SignedBeaconBlock {
		var answer []*cltypes.SignedBeaconBlock
		for _, block := range chain {
			if block.Block.Slot >= start && block.Block.Slot < start+count && (limit == 0 || len(answer) < limit) {
				answer = append(answer, block)
			}
		}
		return answer
	}
}

type ancestorLookupFixture struct {
	t         *testing.T
	cfg       *Cfg
	graph     *chainTipBatchForkGraph
	sentinel  *chainTipAncestorSentinel
	parent    *cltypes.SignedBeaconBlock
	fullChild *cltypes.SignedBeaconBlock
}

// newAncestorLookupFixture imports one block, parent, at slot 64 above an anchor at slot 63.
func newAncestorLookupFixture(t *testing.T) *ancestorLookupFixture {
	t.Helper()
	f := newAncestorLookupFixtureWithoutParent(t)
	require.NoError(t, f.cfg.forkChoice.OnBlock(t.Context(), f.parent, false, false, false))
	return f
}

// newAncestorLookupFixtureWithoutParent is the fixture before parent is imported; fullChild builds on
// parent's payload.
func newAncestorLookupFixtureWithoutParent(t *testing.T) *ancestorLookupFixture {
	t.Helper()
	cfg, graph, parentRoot, fullChild, _, _ := newChainTipBatchFixtureWithRecordedStatus(t, execution_client.PayloadStatusValidated, false)
	parent := graph.parents[parentRoot]
	delete(graph.parents, parentRoot)
	graph.insertOnAdd = true
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, cfg.beaconCfg)
	sentinel := &chainTipAncestorSentinel{t: t, clock: clock, cfg: cfg.beaconCfg}
	cfg.rpc = rpc.NewBeaconRpcP2P(t.Context(), sentinel, cfg.beaconCfg, clock)
	cfg.ethClock = clock
	return &ancestorLookupFixture{t: t, cfg: cfg, graph: graph, sentinel: sentinel, parent: parent, fullChild: fullChild}
}

func (f *ancestorLookupFixture) child(parent *cltypes.SignedBeaconBlock, slot uint64) (*cltypes.SignedBeaconBlock, common.Hash) {
	return newChildOnEmptyParent(f.t, f.cfg.beaconCfg, parent, slot)
}

func (f *ancestorLookupFixture) tickTo(slot uint64) {
	f.cfg.forkChoice.OnTick(slot * f.cfg.beaconCfg.SecondsPerSlot * 1000)
}

func (f *ancestorLookupFixture) pinCurrentSlot(slot *uint64) {
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(f.t))
	clock.EXPECT().GetCurrentSlot().DoAndReturn(func() uint64 { return *slot }).AnyTimes()
	f.cfg.ethClock = clock
}

func (f *ancestorLookupFixture) listen(targetSlot uint64, batches ...[]*cltypes.SignedBeaconBlock) error {
	f.t.Helper()
	respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], len(batches))
	for _, batch := range batches {
		respCh <- &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{Data: batch, Peer: ancestorPeer}
	}
	ctx, cancel := context.WithTimeout(f.t.Context(), 2*time.Second)
	defer cancel()
	return listenToIncomingBlocksUntilANewBlockIsReceived(ctx, log.Root(), f.cfg, Args{targetSlot: targetSlot}, respCh, make(chan error))
}

// process runs one batch from the serving peer through the lookup and the import, as the chain tip loop does.
func (f *ancestorLookupFixture) process(targetSlot uint64, rejected map[common.Hash]error, batch ...*cltypes.SignedBeaconBlock) bool {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.t.Context(), 2*time.Second)
	defer cancel()
	served := &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{Data: batch, Peer: ancestorPeer}
	return processChainTipBatch(ctx, f.cfg, Args{targetSlot: targetSlot}, withMissingAncestors(ctx, f.cfg, served, rejected), make(map[common.Hash]struct{}), rejected)
}

// The peer that served a block whose parent is unknown is asked for the slots below it, and the
// ancestors it returns are imported before the block.
func TestChainTipSyncImportsAnOrphanAfterFetchingItsAncestorsFromItsPeer(t *testing.T) {
	f := newAncestorLookupFixture(t)
	require.EqualValues(t, 63, f.cfg.forkChoice.AnchorSlot())
	require.EqualValues(t, 64, f.parent.Block.Slot)
	middle, middleRoot := f.child(f.parent, 65)
	tip, tipRoot := f.child(middle, 66)
	f.sentinel.serve = servingChain(0, middle)
	f.tickTo(tip.Block.Slot)

	err := f.listen(tip.Block.Slot, []*cltypes.SignedBeaconBlock{tip})

	require.NoError(t, err)
	require.Equal(t, 1, f.graph.added[middleRoot])
	require.Equal(t, 1, f.graph.added[tipRoot])
	require.Equal(t, []ancestorRequest{{pid: ancestorPeer, start: 64, count: 2}}, f.sentinel.requests)
	require.Zero(t, f.sentinel.calls, "the request must not go to a random peer")
}

// The fetched branch is a branch like any other: it takes the head once it outweighs the node's own.
func TestChainTipBatchSwitchesHeadToTheHeavierFetchedBranch(t *testing.T) {
	f := newAncestorLookupFixture(t)
	own, ownRoot := f.child(f.parent, 65)
	middle, _ := f.child(f.parent, 66)
	tip, tipRoot := f.child(middle, 67)
	f.tickTo(tip.Block.Slot)
	require.NoError(t, f.cfg.forkChoice.OnBlock(t.Context(), own, false, false, false))
	vote := func(root common.Hash, slot uint64) common.Hash {
		f.cfg.forkChoice.ProcessAttestingIndicies(&solid.Attestation{
			Data: &solid.AttestationData{Slot: slot, BeaconBlockRoot: root},
		}, []uint64{0})
		head, _, err := f.cfg.forkChoice.GetHead(nil)
		require.NoError(t, err)
		return head
	}
	require.Equal(t, ownRoot, vote(ownRoot, own.Block.Slot))
	f.sentinel.serve = servingChain(0, middle)

	require.NoError(t, f.listen(tip.Block.Slot, []*cltypes.SignedBeaconBlock{tip}))

	require.Contains(t, f.graph.added, tipRoot)
	head, _, err := f.cfg.forkChoice.GetHead(nil)
	require.NoError(t, err)
	require.Equal(t, ownRoot, head, "the fetched branch has no votes yet")
	require.Equal(t, tipRoot, vote(tipRoot, tip.Block.Slot))
}

func TestChainTipAncestorLookupLinksAForkRootedAtTheAnchor(t *testing.T) {
	f := newAncestorLookupFixture(t)
	anchorRoot := f.parent.Block.ParentRoot
	_, anchorHasBlock := f.cfg.forkChoice.GetBlock(anchorRoot)
	_, anchorHasHeader := f.cfg.forkChoice.GetHeader(anchorRoot)
	require.False(t, anchorHasBlock)
	require.True(t, anchorHasHeader)
	fork, _ := f.child(f.parent, f.parent.Block.Slot+1)
	fork.Block.ParentRoot = anchorRoot
	forkRoot, err := fork.Block.HashSSZ()
	require.NoError(t, err)
	tip, tipRoot := f.child(fork, fork.Block.Slot+1)
	f.sentinel.serve = servingChain(0, fork)
	f.tickTo(tip.Block.Slot)

	f.process(tip.Block.Slot, make(map[common.Hash]error), tip)

	require.Equal(t, 1, f.graph.added[forkRoot])
	require.Equal(t, 1, f.graph.added[tipRoot])
}

func TestChainTipAncestorLookupImportsOnlyTheBlocksLinkedToTheOrphan(t *testing.T) {
	f := newAncestorLookupFixture(t)
	unrelated, unrelatedRoot := f.child(f.parent, 65)
	missing, missingRoot := f.child(f.parent, 66)
	tip, tipRoot := f.child(missing, 67)
	f.sentinel.serve = servingChain(0, unrelated, missing)
	f.tickTo(tip.Block.Slot)

	f.process(tip.Block.Slot, make(map[common.Hash]error), tip)

	require.Equal(t, 1, f.graph.added[missingRoot])
	require.Equal(t, 1, f.graph.added[tipRoot])
	require.NotContains(t, f.graph.added, unrelatedRoot)
}

// Fork choice admits blocks above the first slot of the finalized epoch, not above its last slot.
func TestChainTipAncestorLookupStartsRightAboveTheFinalizedEpochStart(t *testing.T) {
	for name, tc := range map[string]struct {
		orphanSlotsAboveFinalizedStart uint64
		windowWouldUnderflow           bool
		justifiedEpoch                 uint64
	}{
		"an orphan above the window":                 {10, false, 7},
		"a one slot window":                          {2, true, 7},
		"a window that would underflow":              {4, true, 7},
		"a justified checkpoint above the finalized": {10, false, 8},
	} {
		t.Run(name, func(t *testing.T) {
			f := newAncestorLookupFixture(t)
			noAnchorFloor := uint64(0)
			f.graph.anchorSlot = &noAnchorFloor
			finalized := f.cfg.forkChoice.FinalizedCheckpoint()
			require.EqualValues(t, 7, finalized.Epoch)
			finalizedStart := finalized.Epoch * f.cfg.beaconCfg.SlotsPerEpoch
			orphan := cltypes.NewSignedBeaconBlock(f.cfg.beaconCfg, clparams.GloasVersion)
			orphan.Block.Slot = finalizedStart + tc.orphanSlotsAboveFinalizedStart
			orphan.Block.ParentRoot = common.Hash{0xde, 0xad}
			require.Equal(t, tc.windowWouldUnderflow, orphan.Block.Slot < uint64(ancestorLookupWindow))
			if tc.justifiedEpoch != finalized.Epoch {
				parentRoot, err := f.parent.Block.HashSSZ()
				require.NoError(t, err)
				f.graph.parentState.SetCurrentJustifiedCheckpoint(solid.Checkpoint{Epoch: tc.justifiedEpoch, Root: parentRoot})
				justifying, _ := f.child(f.parent, (tc.justifiedEpoch+1)*f.cfg.beaconCfg.SlotsPerEpoch)
				f.tickTo(justifying.Block.Slot + 1)
				require.NoError(t, f.cfg.forkChoice.OnBlock(t.Context(), justifying, false, false, false))
			}
			require.Equal(t, tc.justifiedEpoch, f.cfg.forkChoice.JustifiedCheckpoint().Epoch)
			f.sentinel.serve = servingChain(0)

			f.process(orphan.Block.Slot, make(map[common.Hash]error), orphan)

			require.Equal(t, []ancestorRequest{{pid: ancestorPeer, start: finalizedStart + 1, count: tc.orphanSlotsAboveFinalizedStart - 1}}, f.sentinel.requests)
		})
	}
}

// No slot is left to ask for when the orphan is at or right above the anchor, such as the anchor
// block itself.
func TestChainTipAncestorLookupAsksNothingWhenTheWindowIsEmpty(t *testing.T) {
	for name, slotAboveAnchor := range map[string]uint64{"at the anchor": 0, "right above the anchor": 1} {
		t.Run(name, func(t *testing.T) {
			f := newAncestorLookupFixture(t)
			orphan := cltypes.NewSignedBeaconBlock(f.cfg.beaconCfg, clparams.GloasVersion)
			orphan.Block.Slot = f.cfg.forkChoice.AnchorSlot() + slotAboveAnchor
			orphan.Block.ParentRoot = common.Hash{0xde, 0xad}
			f.tickTo(f.parent.Block.Slot)

			f.process(orphan.Block.Slot, make(map[common.Hash]error), orphan)

			require.Empty(t, f.sentinel.requests)
		})
	}
}

func TestChainTipAncestorLookupDropsAResponseOutsideTheRange(t *testing.T) {
	const orphanSlot = 200
	for name, servedSlots := range map[string][]uint64{
		"below the range":        {100},
		"at the orphan slot":     {orphanSlot},
		"ending above the range": {orphanSlot - 1, orphanSlot + 1},
	} {
		t.Run(name, func(t *testing.T) {
			f := newAncestorLookupFixture(t)
			served, servedRoot := f.child(f.parent, servedSlots[0])
			response := []*cltypes.SignedBeaconBlock{served}
			for _, slot := range servedSlots[1:] {
				block, _ := f.child(served, slot)
				response = append(response, block)
			}
			tip, tipRoot := f.child(served, orphanSlot)
			f.sentinel.serve = func(int, uint64, uint64) []*cltypes.SignedBeaconBlock { return response }
			f.tickTo(tip.Block.Slot)
			rejected := make(map[common.Hash]error)

			f.process(tip.Block.Slot, rejected, tip)

			require.Equal(t, []ancestorRequest{{pid: ancestorPeer, start: 136, count: maxChainTipRequestCount}}, f.sentinel.requests)
			require.NotContains(t, f.graph.added, servedRoot)
			require.NotContains(t, f.graph.added, tipRoot)
			require.Contains(t, rejected, tipRoot)
		})
	}
}

func TestChainTipAncestorLookupDropsAResponseWithSlotsNotIncreasing(t *testing.T) {
	for name, order := range map[string][]int{"descending": {1, 0}, "repeated": {0, 0, 1}, "repeated after the first": {0, 1, 1}} {
		t.Run(name, func(t *testing.T) {
			f := newAncestorLookupFixture(t)
			first, firstRoot := f.child(f.parent, 65)
			second, secondRoot := f.child(first, 66)
			tip, tipRoot := f.child(second, 67)
			ancestors := []*cltypes.SignedBeaconBlock{first, second}
			f.sentinel.serve = func(int, uint64, uint64) []*cltypes.SignedBeaconBlock {
				var answer []*cltypes.SignedBeaconBlock
				for _, i := range order {
					answer = append(answer, ancestors[i])
				}
				return answer
			}
			f.tickTo(tip.Block.Slot)

			f.process(tip.Block.Slot, make(map[common.Hash]error), tip)

			require.NotEmpty(t, f.sentinel.requests)
			require.NotContains(t, f.graph.added, firstRoot)
			require.NotContains(t, f.graph.added, secondRoot)
			require.NotContains(t, f.graph.added, tipRoot)
		})
	}
}

// A peer may answer with fewer blocks than asked, so the lookup asks again above what it got.
func TestChainTipAncestorLookupContinuesAfterAPartialRangeResponse(t *testing.T) {
	f := newAncestorLookupFixture(t)
	first, firstRoot := f.child(f.parent, 65)
	second, secondRoot := f.child(first, 66)
	third, thirdRoot := f.child(second, 67)
	fourth, fourthRoot := f.child(third, 68)
	tip, tipRoot := f.child(fourth, 69)
	f.sentinel.serve = servingChain(3, first, second, third, fourth)
	f.tickTo(tip.Block.Slot)

	f.process(tip.Block.Slot, make(map[common.Hash]error), tip)

	require.Equal(t, []ancestorRequest{
		{pid: ancestorPeer, start: 64, count: 5},
		{pid: ancestorPeer, start: 68, count: 1},
	}, f.sentinel.requests)
	for _, root := range []common.Hash{firstRoot, secondRoot, thirdRoot, fourthRoot, tipRoot} {
		require.Equal(t, 1, f.graph.added[root])
	}
}

// Each response is checked against the range of its own request, not against the window.
func TestChainTipAncestorLookupDropsAResponseBelowItsOwnRequest(t *testing.T) {
	f := newAncestorLookupFixture(t)
	first, _ := f.child(f.parent, 65)
	second, _ := f.child(first, 66)
	third, _ := f.child(second, 67)
	tip, tipRoot := f.child(third, 68)
	responses := [][]*cltypes.SignedBeaconBlock{{first}, {first, second}, {third}}
	f.sentinel.serve = func(call int, _, _ uint64) []*cltypes.SignedBeaconBlock { return responses[call] }
	f.tickTo(tip.Block.Slot)

	f.process(tip.Block.Slot, make(map[common.Hash]error), tip)

	require.Len(t, f.sentinel.requests, 2)
	require.NotContains(t, f.graph.added, tipRoot)
}

// The lookup links what it has before asking again, so a parent that is not at the slot below the
// orphan is found with one request.
func TestChainTipAncestorLookupLinksBeforeAskingAgain(t *testing.T) {
	f := newAncestorLookupFixture(t)
	middle, middleRoot := f.child(f.parent, 66)
	tip, tipRoot := f.child(middle, 71)
	f.sentinel.serve = servingChain(0, middle)
	f.tickTo(tip.Block.Slot)

	f.process(tip.Block.Slot, make(map[common.Hash]error), tip)

	require.Len(t, f.sentinel.requests, 1)
	require.Equal(t, 1, f.graph.added[middleRoot])
	require.Equal(t, 1, f.graph.added[tipRoot])
}

func TestChainTipAncestorLookupStopsWhenAResponseEndsRightBelowTheOrphan(t *testing.T) {
	f := newAncestorLookupFixture(t)
	missing, _ := f.child(f.parent, 65)
	unrelated, unrelatedRoot := f.child(f.parent, 66)
	tip, tipRoot := f.child(missing, 67)
	f.sentinel.serve = servingChain(0, unrelated)
	f.tickTo(tip.Block.Slot)
	rejected := make(map[common.Hash]error)

	f.process(tip.Block.Slot, rejected, tip)

	require.Equal(t, []ancestorRequest{{pid: ancestorPeer, start: 64, count: 3}}, f.sentinel.requests)
	require.NotContains(t, f.graph.added, unrelatedRoot)
	require.NotContains(t, f.graph.added, tipRoot)
	require.Contains(t, rejected, tipRoot)
}

// A peer that keeps answering with one block that does not reach the orphan's parent costs
// at most four requests, the first of them for the whole 64 slot window.
func TestChainTipAncestorLookupCrossesAnEmptyPage(t *testing.T) {
	f := newAncestorLookupFixture(t)
	missing, _ := f.child(f.parent, 199)
	tip, tipRoot := f.child(missing, 200)
	f.sentinel.serve = func(_ int, start, count uint64) []*cltypes.SignedBeaconBlock {
		if start <= 199 && 199 < start+count {
			return []*cltypes.SignedBeaconBlock{missing}
		}
		return nil
	}
	f.tickTo(tip.Block.Slot)
	currentSlot := tip.Block.Slot
	f.pinCurrentSlot(&currentSlot)
	rejected := make(map[common.Hash]error)

	f.process(tip.Block.Slot, rejected, tip)

	require.NotContains(t, rejected, tipRoot)
	require.Contains(t, f.graph.added, tipRoot)
}

func TestChainTipAncestorLookupRejectsABlockPastThePage(t *testing.T) {
	f := newAncestorLookupFixture(t)
	missing, _ := f.child(f.parent, 199)
	tip, tipRoot := f.child(missing, 200)
	f.sentinel.serve = func(int, uint64, uint64) []*cltypes.SignedBeaconBlock {
		return []*cltypes.SignedBeaconBlock{missing}
	}
	f.tickTo(tip.Block.Slot)
	currentSlot := tip.Block.Slot
	f.pinCurrentSlot(&currentSlot)
	rejected := make(map[common.Hash]error)

	f.process(tip.Block.Slot, rejected, tip)

	require.Len(t, f.sentinel.requests, 1, "a block outside the requested page fails the lookup")
	require.NotContains(t, f.graph.added, tipRoot)
}

func TestChainTipAncestorLookupStopsAfterFourRequests(t *testing.T) {
	f := newAncestorLookupFixture(t)
	unknownParent, _ := f.child(f.parent, 150)
	tip, tipRoot := f.child(unknownParent, 200)
	f.sentinel.serve = func(_ int, start, _ uint64) []*cltypes.SignedBeaconBlock {
		block := cltypes.NewSignedBeaconBlock(f.cfg.beaconCfg, clparams.GloasVersion)
		block.Block.Slot = start
		block.Block.ParentRoot = common.Hash{0xde, 0xad, byte(start)}
		return []*cltypes.SignedBeaconBlock{block}
	}
	f.tickTo(tip.Block.Slot)
	currentSlot := tip.Block.Slot
	f.pinCurrentSlot(&currentSlot)
	rejected := make(map[common.Hash]error)

	f.process(tip.Block.Slot, rejected, tip)

	require.Equal(t, []ancestorRequest{
		{pid: ancestorPeer, start: 136, count: maxChainTipRequestCount},
		{pid: ancestorPeer, start: 137, count: maxChainTipRequestCount},
		{pid: ancestorPeer, start: 138, count: maxChainTipRequestCount},
		{pid: ancestorPeer, start: 139, count: maxChainTipRequestCount},
	}, f.sentinel.requests)
	require.Contains(t, rejected, tipRoot)
	require.NotContains(t, f.graph.added, tipRoot)

	f.process(tip.Block.Slot, rejected, tip)
	require.Len(t, f.sentinel.requests, 4, "reaching the cap is a failed lookup")
}

// A parent that was rejected for its own fault is not fetched again; the child inherits the cause.
func TestChainTipAncestorLookupSkipsParentsRejectedInThisSync(t *testing.T) {
	f := newAncestorLookupFixture(t)
	middle, middleRoot := f.child(f.parent, 65)
	tip, tipRoot := f.child(middle, 66)
	f.sentinel.serve = servingChain(0, middle)
	f.tickTo(tip.Block.Slot)
	cause := errors.New("parent payload invalid")
	rejected := map[common.Hash]error{middleRoot: cause}

	f.process(tip.Block.Slot, rejected, tip)

	require.Empty(t, f.sentinel.requests)
	require.Equal(t, cause, rejected[tipRoot])
}

func TestChainTipSyncSkipsLookupForParentRejectedInAnEarlierBatch(t *testing.T) {
	f := newAncestorLookupFixture(t)
	f.tickTo(65)
	tooEarly, _ := f.child(f.parent, 66)
	child, _ := f.child(tooEarly, 67)
	reachesTarget, _ := f.child(f.parent, 65)
	f.sentinel.serve = servingChain(0, tooEarly)

	err := f.listen(reachesTarget.Block.Slot, []*cltypes.SignedBeaconBlock{tooEarly}, []*cltypes.SignedBeaconBlock{child}, []*cltypes.SignedBeaconBlock{reachesTarget})

	require.NoError(t, err)
	require.Empty(t, f.sentinel.requests)
}

// A parent that was only rejected for its own missing parent is the thing to look up.
func TestChainTipAncestorLookupFollowsAParentRejectedOnlyForItsOwnParent(t *testing.T) {
	f := newAncestorLookupFixture(t)
	middle, middleRoot := f.child(f.parent, 65)
	tip, tipRoot := f.child(middle, 66)
	f.sentinel.serve = servingChain(0, middle)
	f.tickTo(tip.Block.Slot)
	rejected := map[common.Hash]error{middleRoot: nil}

	f.process(tip.Block.Slot, rejected, tip)

	require.Len(t, f.sentinel.requests, 1)
	require.Equal(t, 1, f.graph.added[middleRoot])
	require.Equal(t, 1, f.graph.added[tipRoot])
}

// Every new orphan has another missing parent, so a fork deeper than the window would otherwise
// start a lookup on each poll.
func TestChainTipAncestorLookupWaitsASlotAfterAFailure(t *testing.T) {
	type serveFunc = func(int, uint64, uint64) []*cltypes.SignedBeaconBlock
	always := func(blocks ...*cltypes.SignedBeaconBlock) serveFunc {
		return func(int, uint64, uint64) []*cltypes.SignedBeaconBlock { return blocks }
	}
	for name, serve := range map[string]func(chain []*cltypes.SignedBeaconBlock) serveFunc{
		"an empty response": func([]*cltypes.SignedBeaconBlock) serveFunc { return servingChain(0) },
		"a transport error": func([]*cltypes.SignedBeaconBlock) serveFunc { return nil },
		"a chain that does not reach a known block": func(chain []*cltypes.SignedBeaconBlock) serveFunc {
			return servingChain(0, chain...)
		},
		"a response outside the requested range": func(chain []*cltypes.SignedBeaconBlock) serveFunc {
			return always(chain[0])
		},
		"a response with repeated slots": func(chain []*cltypes.SignedBeaconBlock) serveFunc {
			return always(chain[1], chain[1])
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newAncestorLookupFixture(t)
			belowWindow, _ := f.child(f.parent, 100)
			missing, _ := f.child(belowWindow, 199)
			tip, tipRoot := f.child(missing, 200)
			f.sentinel.serve = serve([]*cltypes.SignedBeaconBlock{belowWindow, missing})
			currentSlot := tip.Block.Slot + 3
			f.tickTo(currentSlot)
			f.pinCurrentSlot(&currentSlot)

			f.process(tip.Block.Slot, make(map[common.Hash]error), tip)
			failedRequests := len(f.sentinel.requests)
			rejected := make(map[common.Hash]error)
			f.process(tip.Block.Slot, rejected, tip)
			require.Len(t, f.sentinel.requests, failedRequests)
			require.Contains(t, rejected, tipRoot)

			currentSlot++
			f.process(tip.Block.Slot, make(map[common.Hash]error), tip)
			require.Greater(t, len(f.sentinel.requests), failedRequests)
		})
	}
}

// The poll that delivers an orphan runs mid-slot, so a peer that stalls must not hold the caller
// beyond half a slot, however long the stage context lasts.
func TestChainTipAncestorLookupEndsWithinHalfASlot(t *testing.T) {
	f := newAncestorLookupFixture(t)
	shortSlots := *f.cfg.beaconCfg
	shortSlots.SecondsPerSlot = 1
	f.cfg.beaconCfg = &shortSlots
	const (
		halfSlot     = time.Second / 2
		stageTimeout = 10 * time.Second
	)
	missing, _ := f.child(f.parent, 65)
	tip, _ := f.child(missing, 66)
	var requestedAt, deadline time.Time
	var hasDeadline bool
	f.sentinel.onRequest = func(ctx context.Context) {
		requestedAt = time.Now()
		deadline, hasDeadline = ctx.Deadline()
		<-ctx.Done()
	}
	ctx, cancel := context.WithTimeout(t.Context(), stageTimeout)
	defer cancel()
	batch := &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{Data: []*cltypes.SignedBeaconBlock{tip}, Peer: ancestorPeer}

	started := time.Now()
	blocks := withMissingAncestors(ctx, f.cfg, batch, make(map[common.Hash]error))

	require.Equal(t, []*cltypes.SignedBeaconBlock{tip}, blocks)
	require.Len(t, f.sentinel.requests, 1)
	require.False(t, requestedAt.IsZero())
	require.True(t, hasDeadline)
	require.LessOrEqual(t, deadline.Sub(requestedAt), halfSlot)
	require.Less(t, time.Since(started), stageTimeout/2, "a stalled lookup outlived its half-slot deadline")
}

// A lookup that fails after the clock moved on throttles the slot it ended in.
func TestChainTipAncestorLookupThrottlesTheSlotItFailedIn(t *testing.T) {
	f := newAncestorLookupFixture(t)
	missing, _ := f.child(f.parent, 199)
	tip, _ := f.child(missing, 200)
	f.sentinel.serve = servingChain(0)
	f.tickTo(tip.Block.Slot)
	currentSlot := tip.Block.Slot + 3
	f.pinCurrentSlot(&currentSlot)
	f.sentinel.onRequest = func(context.Context) { currentSlot++ }

	f.process(tip.Block.Slot, make(map[common.Hash]error), tip)
	failedRequests := len(f.sentinel.requests)
	f.process(tip.Block.Slot, make(map[common.Hash]error), tip)

	require.Len(t, f.sentinel.requests, failedRequests)
}

// Sending nothing is not a failed lookup, so it must not hold back the next orphan of the slot.
func TestChainTipAncestorLookupWithAnEmptyWindowDoesNotThrottleTheSlot(t *testing.T) {
	for name, slotAboveAnchor := range map[string]uint64{"at the anchor": 0, "right above the anchor": 1} {
		t.Run(name, func(t *testing.T) {
			f := newAncestorLookupFixture(t)
			belowFloor := cltypes.NewSignedBeaconBlock(f.cfg.beaconCfg, clparams.GloasVersion)
			belowFloor.Block.Slot = f.cfg.forkChoice.AnchorSlot() + slotAboveAnchor
			belowFloor.Block.ParentRoot = common.Hash{0xde, 0xad}
			middle, middleRoot := f.child(f.parent, 65)
			tip, tipRoot := f.child(middle, 66)
			f.sentinel.serve = servingChain(0, middle)
			f.tickTo(tip.Block.Slot)
			currentSlot := tip.Block.Slot
			f.pinCurrentSlot(&currentSlot)

			f.process(belowFloor.Block.Slot, make(map[common.Hash]error), belowFloor)
			require.Empty(t, f.sentinel.requests)
			f.process(tip.Block.Slot, make(map[common.Hash]error), tip)

			require.Len(t, f.sentinel.requests, 1)
			require.Equal(t, 1, f.graph.added[middleRoot])
			require.Equal(t, 1, f.graph.added[tipRoot])
		})
	}
}

// A fetched ancestor that its child builds on as FULL needs its envelope, which comes from a random
// peer as for any other batch.
func TestChainTipAncestorLookupFetchesTheEnvelopeOfAFullAncestor(t *testing.T) {
	f := newAncestorLookupFixtureWithoutParent(t)
	parentRoot, err := f.parent.Block.HashSSZ()
	require.NoError(t, err)
	fullChildRoot, err := f.fullChild.Block.HashSSZ()
	require.NoError(t, err)
	tip, tipRoot := f.child(f.fullChild, 66)
	f.sentinel.response = encodeEnvelopeResponse(t, f.cfg.beaconCfg, f.graph.envelopes[parentRoot])
	delete(f.graph.envelopes, parentRoot)
	f.sentinel.serve = servingChain(0, f.parent, f.fullChild)
	f.tickTo(tip.Block.Slot)

	f.process(tip.Block.Slot, make(map[common.Hash]error), tip)

	require.Equal(t, []ancestorRequest{{pid: ancestorPeer, start: 64, count: 2}}, f.sentinel.requests)
	require.Equal(t, 1, f.sentinel.calls)
	for _, root := range []common.Hash{parentRoot, fullChildRoot, tipRoot} {
		require.Equal(t, 1, f.graph.added[root])
	}
}

func TestChainTipAncestorLookupDoesNotWaitAfterASuccess(t *testing.T) {
	f := newAncestorLookupFixture(t)
	firstMiddle, firstMiddleRoot := f.child(f.parent, 65)
	firstTip, firstTipRoot := f.child(firstMiddle, 66)
	secondMiddle, secondMiddleRoot := f.child(f.parent, 67)
	secondTip, secondTipRoot := f.child(secondMiddle, 68)
	f.sentinel.serve = servingChain(0, firstMiddle, secondMiddle)
	f.tickTo(secondTip.Block.Slot)
	currentSlot := secondTip.Block.Slot
	f.pinCurrentSlot(&currentSlot)

	f.process(firstTip.Block.Slot, make(map[common.Hash]error), firstTip)
	f.process(secondTip.Block.Slot, make(map[common.Hash]error), secondTip)

	require.Len(t, f.sentinel.requests, 2)
	for _, root := range []common.Hash{firstMiddleRoot, firstTipRoot, secondMiddleRoot, secondTipRoot} {
		require.Equal(t, 1, f.graph.added[root])
	}
}

func TestChainTipAncestorLookupDoesNotWaitAfterABatchWithoutAnOrphan(t *testing.T) {
	f := newAncestorLookupFixture(t)
	linked, linkedRoot := f.child(f.parent, 65)
	missing, missingRoot := f.child(f.parent, 66)
	tip, tipRoot := f.child(missing, 67)
	f.sentinel.serve = servingChain(0, missing)
	f.tickTo(tip.Block.Slot)
	currentSlot := tip.Block.Slot
	f.pinCurrentSlot(&currentSlot)

	f.process(linked.Block.Slot, make(map[common.Hash]error), linked)
	f.process(tip.Block.Slot, make(map[common.Hash]error), tip)

	require.Len(t, f.sentinel.requests, 1)
	for _, root := range []common.Hash{linkedRoot, missingRoot, tipRoot} {
		require.Equal(t, 1, f.graph.added[root])
	}
}

func TestChainTipAncestorLookupSkipsBatchesWithKnownParents(t *testing.T) {
	f := newAncestorLookupFixture(t)
	middle, middleRoot := f.child(f.parent, 65)
	tip, tipRoot := f.child(middle, 66)
	f.tickTo(tip.Block.Slot)

	f.process(tip.Block.Slot, make(map[common.Hash]error), middle, tip)

	require.Empty(t, f.sentinel.requests)
	require.Equal(t, 1, f.graph.added[middleRoot])
	require.Equal(t, 1, f.graph.added[tipRoot])
}

func TestChainTipBatchStillReportsUnknownParentWhenThePeerHasNoAncestors(t *testing.T) {
	f := newAncestorLookupFixture(t)
	missing, _ := f.child(f.parent, 65)
	tip, tipRoot := f.child(missing, 66)
	f.sentinel.serve = servingChain(0)
	f.tickTo(tip.Block.Slot)
	f.cfg.chainTipRejections = newChainTipRejections(time.Hour, nil)
	f.cfg.chainTipRejections.lastLog = time.Now()
	rejected := make(map[common.Hash]error)

	f.process(tip.Block.Slot, rejected, tip)

	require.NotEmpty(t, f.sentinel.requests)
	require.Contains(t, rejected, tipRoot)
	reason := f.cfg.chainTipRejections.reasons["parent not in fork graph"]
	require.NotNil(t, reason)
	require.Equal(t, 1, reason.attempts)
}

func TestChainTipAncestorLookupNeedsAServingPeerAndAnRpcClient(t *testing.T) {
	for name, tamper := range map[string]func(*ancestorLookupFixture, *peers.PeeredObject[[]*cltypes.SignedBeaconBlock]){
		"no serving peer": func(_ *ancestorLookupFixture, batch *peers.PeeredObject[[]*cltypes.SignedBeaconBlock]) {
			batch.Peer = ""
		},
		"no rpc client": func(f *ancestorLookupFixture, _ *peers.PeeredObject[[]*cltypes.SignedBeaconBlock]) { f.cfg.rpc = nil },
	} {
		t.Run(name, func(t *testing.T) {
			f := newAncestorLookupFixture(t)
			missing, _ := f.child(f.parent, 65)
			tip, _ := f.child(missing, 66)
			f.sentinel.serve = servingChain(0, missing)
			batch := &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{Data: []*cltypes.SignedBeaconBlock{tip}, Peer: ancestorPeer}
			tamper(f, batch)

			blocks := withMissingAncestors(t.Context(), f.cfg, batch, make(map[common.Hash]error))

			require.Equal(t, []*cltypes.SignedBeaconBlock{tip}, blocks)
			require.Empty(t, f.sentinel.requests)
		})
	}
}

const pollPeer = "poll-peer"

// pollSentinel fails the first `failures` poll requests, then answers them from pollPeer.
type pollSentinel struct {
	chainTipAncestorSentinel
	failures int
}

func (s *pollSentinel) SendRequest(context.Context, *sentinelproto.RequestData, ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	if s.failures > 0 {
		s.failures--
		return nil, errors.New("peer busy")
	}
	return &sentinelproto.ResponseData{Data: s.response, Peer: &sentinelproto.Peer{Pid: pollPeer}}, nil
}

func TestChainTipPollBatchCarriesThePeerThatServedIt(t *testing.T) {
	for name, failures := range map[string]int{"first attempt": 0, "retry after a failed attempt": 1} {
		t.Run(name, func(t *testing.T) {
			f := newAncestorLookupFixture(t)
			block, _ := f.child(f.parent, 65)
			digest, err := f.sentinel.clock.ComputeForkDigest(block.Block.Slot / f.cfg.beaconCfg.SlotsPerEpoch)
			require.NoError(t, err)
			var response bytes.Buffer
			require.NoError(t, ssz_snappy.EncodeAndWrite(&response, block, digest[:]...))
			sentinel := &pollSentinel{chainTipAncestorSentinel: *f.sentinel, failures: failures}
			sentinel.response = response.Bytes()
			f.cfg.rpc = rpc.NewBeaconRpcP2P(t.Context(), sentinel, f.cfg.beaconCfg, f.sentinel.clock)

			batch, err := fetchBlocksFromReqResp(t.Context(), f.cfg, block.Block.Slot, 1)

			require.NoError(t, err)
			require.Len(t, batch.Data, 1)
			require.Equal(t, pollPeer, batch.Peer)
		})
	}
}
