package stages

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
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
		return execution_client.PayloadStatusNone, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return execution_client.PayloadStatusNone, err
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
	envelopes   map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	added       map[common.Hash]int
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

func (g *chainTipBatchForkGraph) AddChainSegment(block *cltypes.SignedBeaconBlock, _ bool) (*state2.CachingBeaconState, fork_graph.ChainSegmentInsertionResult, error) {
	root, err := block.Block.HashSSZ()
	if err != nil {
		return nil, fork_graph.InvalidBlock, err
	}
	g.added[common.Hash(root)]++
	return nil, fork_graph.PreValidated, nil
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

func (g *chainTipBatchForkGraph) GetState(root common.Hash, alwaysCopy bool) (*state2.CachingBeaconState, error) {
	if _, ok := g.parents[root]; ok {
		if alwaysCopy {
			return g.parentState.Copy()
		}
		return g.parentState, nil
	}
	return g.ForkGraph.GetState(root, alwaysCopy)
}

func (g *chainTipBatchForkGraph) HasEnvelope(root common.Hash) bool {
	_, ok := g.envelopes[root]
	return ok || g.ForkGraph.HasEnvelope(root)
}

func (g *chainTipBatchForkGraph) ReadEnvelopeFromDisk(root common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	if envelope, ok := g.envelopes[root]; ok {
		return envelope, nil
	}
	return g.ForkGraph.ReadEnvelopeFromDisk(root)
}

func (g *chainTipBatchForkGraph) DumpEnvelopeOnDisk(root common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
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
	anchorRoot, err := anchorState.BlockRoot()
	require.NoError(t, err)

	parentState, err := anchorState.Copy()
	require.NoError(t, err)
	require.NoError(t, parentState.SetSlot(envelope.Message.Payload.SlotNumber))
	parentBid.ParentBlockRoot = anchorRoot
	envelope.Message.ParentBeaconBlockRoot = anchorRoot
	envelope.Message.Payload.Time = state2.ComputeTimestampAtSlot(parentState, parentState.Slot())
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(beaconCfg, envelope.Message.ExecutionRequests))
	envelope.Message.Payload.BlockHash = anchorPayloadHeaderHash(t, envelope.Message.Payload, anchorRoot, requestsHash)
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
	store.OnTick((parent.Block.Slot + 1) * beaconCfg.SecondsPerSlot)
	store.MarkPayloadStatus(parentRoot, envelope.Message.Payload.BlockHash, execution_client.PayloadStatusNone)
	status, found := store.GetRecentExecutionPayloadStatusByRoot(parentRoot)
	require.True(t, found)
	require.EqualValues(t, execution_client.PayloadStatusNone, status)

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

func newChainTipBatchEnvelopeRPC(t *testing.T, cfg *clparams.BeaconChainConfig, envelope *cltypes.SignedExecutionPayloadEnvelope) (*rpc.BeaconRpcP2P, *chainTipBatchEnvelopeSentinel) {
	t.Helper()

	clock := eth_clock.NewEthereumClock(0, common.Hash{}, cfg)
	digest, err := clock.ComputeForkDigest(cfg.GloasForkEpoch)
	require.NoError(t, err)
	var response bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&response, envelope, digest[:]...))
	sentinel := &chainTipBatchEnvelopeSentinel{response: response.Bytes()}
	return rpc.NewBeaconRpcP2P(t.Context(), sentinel, cfg, clock, nil), sentinel
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
			processChainTipBatch(t.Context(), cfg, Args{targetSlot: childB.Block.Slot + 1}, []*cltypes.SignedBeaconBlock{childA, childB}, seen)

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
	)

	require.True(t, reachedTarget)
	require.Equal(t, 1, engine.newPayloadCalls)
	require.Greater(t, replayBudget, gloasPayloadRetryBudget*3/4)
	require.Equal(t, 1, graph.added[common.Hash(childARoot)])
	require.Zero(t, graph.added[common.Hash(childBRoot)])
}

func TestChainTipBatchReplaysStoredParentPayload(t *testing.T) {
	t.Run("peer-fetched unavailable parent does not gate full child", func(t *testing.T) {
		cfg, graph, parentRoot, fullChild, _, engine := newChainTipBatchFixture(t, execution_client.PayloadStatusNone)
		fullRoot, err := fullChild.Block.HashSSZ()
		require.NoError(t, err)
		envelope := graph.envelopes[parentRoot]
		peerRPC, sentinel := newChainTipBatchEnvelopeRPC(t, cfg.beaconCfg, envelope)
		cfg.rpc = peerRPC
		delete(graph.envelopes, parentRoot)
		seen := make(map[common.Hash]struct{})

		processChainTipBatch(t.Context(), cfg, Args{targetSlot: fullChild.Block.Slot + 1}, []*cltypes.SignedBeaconBlock{fullChild}, seen)

		require.Equal(t, 1, sentinel.calls)
		require.Equal(t, 1, engine.newPayloadCalls)
		var storedSlot *uint64
		require.NoError(t, cfg.indiciesDB.View(t.Context(), func(tx kv.Tx) error {
			storedSlot, err = beacon_indicies.ReadBlockSlotByBlockRoot(tx, common.Hash(fullRoot))
			return err
		}))
		require.NotNil(t, storedSlot)
		require.Equal(t, fullChild.Block.Slot, *storedSlot)
	})

	t.Run("valid replay processes full child", func(t *testing.T) {
		cfg, graph, parentRoot, fullChild, _, engine := newChainTipBatchFixture(t, execution_client.PayloadStatusValidated)
		fullRoot, err := fullChild.Block.HashSSZ()
		require.NoError(t, err)
		seen := make(map[common.Hash]struct{})

		reachedTarget := processChainTipBatch(t.Context(), cfg, Args{targetSlot: fullChild.Block.Slot + 1}, []*cltypes.SignedBeaconBlock{fullChild}, seen)

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

		processChainTipBatch(t.Context(), cfg, Args{targetSlot: fullChild.Block.Slot + 1}, []*cltypes.SignedBeaconBlock{fullChild}, seen)

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

			processChainTipBatch(t.Context(), cfg, Args{targetSlot: fullChild.Block.Slot + 1}, blocks, seen)

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

	accepted := ensureStoredParentPayloadAccepted(t.Context(), cfg, store, root, envelope)

	require.True(t, accepted)
	require.Equal(t, 1, engine.newPayloadCalls)
	require.EqualValues(t, execution_client.PayloadStatusNotValidated, store.marked)
	require.Equal(t, payload.GasLimit, store.markedGas)
	require.Len(t, store.requeued, 1)
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

	accepted := ensureStoredParentPayloadAccepted(t.Context(), &Cfg{}, store, root, envelope)

	require.False(t, accepted)
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

	accepted := ensureStoredParentPayloadAccepted(t.Context(), &Cfg{}, store, root, envelope)

	require.True(t, accepted)
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

	accepted := ensureStoredParentPayloadAccepted(t.Context(), &Cfg{}, store, root, envelope)

	require.True(t, accepted)
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
		results:  make(map[common.Hash]bool),
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

	accepted := replay.accepted(t.Context(), cfg, store, root, envelope, forkchoice.ErrIgnore)
	replayed := replay.accepted(t.Context(), cfg, store, root, envelope, forkchoice.ErrIgnore)

	require.False(t, accepted)
	require.False(t, replayed)
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
		results:   make(map[common.Hash]bool),
	}
	cfg := &Cfg{
		beaconCfg:             &clparams.MainnetBeaconConfig,
		executionClient:       &testExecutionEngine{},
		gloasPayloadValidator: validator,
	}

	require.False(t, replay.accepted(t.Context(), cfg, firstStore, firstRoot, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		BeaconBlockRoot: firstRoot,
		Payload:         firstPayload,
	}}, nil))
	require.True(t, replay.accepted(t.Context(), cfg, secondStore, secondRoot, &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		BeaconBlockRoot: secondRoot,
		Payload:         secondPayload,
	}}, nil))
	require.Equal(t, []common.Hash{firstPayload.BlockHash, secondPayload.BlockHash}, validator.calls)
}

func TestStoredParentPayloadReplayRejectsApplyFailure(t *testing.T) {
	root := common.Hash{1}
	store := &storedParentPayloadTestStore{has: true, markRetained: true}
	replay := storedParentPayloadReplay{deadline: time.Now(), remaining: 1, results: make(map[common.Hash]bool)}

	accepted := replay.accepted(
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

	require.False(t, accepted)
	require.EqualValues(t, execution_client.PayloadStatusNone, store.marked)
	require.Zero(t, replay.remaining)
}

func TestStoredParentPayloadReplayWithoutVerdictKeepsStatus(t *testing.T) {
	const unchanged = execution_client.PayloadStatus(99)
	root := common.Hash{1}
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
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
			results:   make(map[common.Hash]bool),
		}

		require.False(t, replay.accepted(t.Context(), cfg, store, root, envelope, forkchoice.ErrIgnore))
		require.Zero(t, engine.newPayloadCalls)
		require.Equal(t, unchanged, store.marked)
		require.Empty(t, store.requeued)
	})

	t.Run("deadline during validation", func(t *testing.T) {
		store := newStore()
		engine := &testExecutionEngine{newPayloadFn: func(ctx context.Context, _ *cltypes.Eth1Block) (execution_client.PayloadStatus, error) {
			<-ctx.Done()
			return execution_client.PayloadStatusNone, ctx.Err()
		}}
		cfg := &Cfg{beaconCfg: &clparams.MainnetBeaconConfig, executionClient: engine, gloasPayloadValidator: engine}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel()

		require.False(t, ensureStoredParentPayloadAccepted(ctx, cfg, store, root, envelope))
		require.Equal(t, 1, engine.newPayloadCalls)
		require.Equal(t, unchanged, store.marked)
		require.Empty(t, store.requeued)
	})
}
