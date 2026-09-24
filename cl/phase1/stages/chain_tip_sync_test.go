package stages

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type blockingEnvelopeSentinel struct {
	sentinelproto.SentinelClient
}

type deadlineEnvelopeSentinel struct {
	sentinelproto.SentinelClient
	once     sync.Once
	deadline chan time.Duration
}

type testGloasEnvelopeAcceptor struct {
	mu              sync.Mutex
	retained        map[common.Hash]struct{}
	persisted       map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	retainOnSuccess bool
	accept          func(context.Context, *cltypes.SignedExecutionPayloadEnvelope) error
	calls           atomic.Int32
}

func (a *testGloasEnvelopeAcceptor) ReadEnvelopeFromDisk(root common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.persisted[root], nil
}

func (a *testGloasEnvelopeAcceptor) OnExecutionPayload(ctx context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope, _, _ bool) error {
	a.calls.Add(1)
	if a.accept != nil {
		if err := a.accept(ctx, envelope); err != nil {
			return err
		}
	}
	if a.retainOnSuccess {
		a.mu.Lock()
		if a.retained == nil {
			a.retained = make(map[common.Hash]struct{})
		}
		a.retained[envelope.Message.BeaconBlockRoot] = struct{}{}
		a.mu.Unlock()
	}
	return nil
}

func (a *testGloasEnvelopeAcceptor) HasEnvelope(root common.Hash) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.retained[root]
	return ok
}

func (s *blockingEnvelopeSentinel) SendRequest(ctx context.Context, _ *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *blockingEnvelopeSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
}

func (s *deadlineEnvelopeSentinel) SendRequest(ctx context.Context, _ *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	s.once.Do(func() {
		deadline, ok := ctx.Deadline()
		if !ok {
			s.deadline <- 0
			return
		}
		s.deadline <- time.Until(deadline)
	})
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *deadlineEnvelopeSentinel) PeersInfo(context.Context, *sentinelproto.PeersInfoRequest, ...grpc.CallOption) (*sentinelproto.PeersInfoResponse, error) {
	return &sentinelproto.PeersInfoResponse{}, nil
}

func TestRememberBlockAfterProcess(t *testing.T) {
	require.True(t, rememberBlockAfterProcess(nil))
	require.True(t, rememberBlockAfterProcess(errors.New("invalid block")))
	require.False(t, rememberBlockAfterProcess(fmt.Errorf("retry parent envelope: %w", forkchoice.ErrParentEnvelopePending)))
}

func TestResolveGloasEnvelopeHTTPURLAtUseTime(t *testing.T) {
	previous := clparams.ConfigurableCheckpointsURLs
	clparams.ConfigurableCheckpointsURLs = nil
	t.Cleanup(func() { clparams.ConfigurableCheckpointsURLs = previous })

	require.Equal(t, []string{"https://checkpoint.example"}, resolveGloasEnvelopeHTTPURLs(clparams.CaplinConfig{
		DisabledCheckpointSync: true,
		CheckpointSyncURLs:     []string{"https://checkpoint.example/"},
	}))
	require.Equal(t, []string{"https://checkpoint.example"}, resolveGloasEnvelopeHTTPURLs(clparams.CaplinConfig{
		DisabledCheckpointSync: true,
		CheckpointSyncURLs:     []string{"https://checkpoint.example/eth/v2/debug/beacon/states/finalized"},
	}))
}

func TestResolveGloasEnvelopeHTTPURLFallsBackToGlobal(t *testing.T) {
	previous := clparams.ConfigurableCheckpointsURLs
	clparams.ConfigurableCheckpointsURLs = []string{"https://checkpoint.example/"}
	t.Cleanup(func() { clparams.ConfigurableCheckpointsURLs = previous })

	require.Equal(t, []string{"https://checkpoint.example"}, resolveGloasEnvelopeHTTPURLs(clparams.CaplinConfig{DisabledCheckpointSync: true}))
}

func gloasEnvelopeFixture(t *testing.T) (*clparams.BeaconChainConfig, *cltypes.SignedBeaconBlock, [32]byte, []byte) {
	t.Helper()
	beaconCfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&beaconCfg)
	beaconCfg.AltairForkEpoch = 0
	beaconCfg.BellatrixForkEpoch = 0
	beaconCfg.CapellaForkEpoch = 0
	beaconCfg.DenebForkEpoch = 0
	beaconCfg.ElectraForkEpoch = 0
	beaconCfg.FuluForkEpoch = 0
	beaconCfg.GloasForkEpoch = 0
	beaconCfg.GloasForkVersion = 0x80000038
	beaconCfg.InitializeForkSchedule()

	block := cltypes.NewSignedBeaconBlock(&beaconCfg, clparams.GloasVersion)
	block.Block.Slot = 10
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&beaconCfg)}
	envelope.Message.BeaconBlockRoot = root
	wire, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	decoded := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&beaconCfg)}
	require.NoError(t, decoded.DecodeSSZStrict(wire, int(clparams.GloasVersion)))
	require.NoError(t, decoded.ValidateForConfig(&beaconCfg))
	return &beaconCfg, block, root, wire
}

func gloasEnvelopeWireWithPayloadHash(t *testing.T, beaconCfg *clparams.BeaconChainConfig, root [32]byte, payloadHash common.Hash) []byte {
	t.Helper()
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(beaconCfg)}
	envelope.Message.BeaconBlockRoot = root
	envelope.Message.Payload.BlockHash = payloadHash
	wire, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	return wire
}

func TestFetchParentEnvelopesPrefersCheckpointAPI(t *testing.T) {
	beaconCfg, block, root, wire := gloasEnvelopeFixture(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, fmt.Sprintf("/eth/v1/beacon/execution_payload_envelopes/0x%x", root), r.URL.Path)
		require.Equal(t, "application/octet-stream", r.Header.Get("Accept"))
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, err := bytes.NewReader(wire).WriteTo(w)
		require.NoError(t, err)
	}))
	defer server.Close()
	unavailableServer := httptest.NewServer(http.NotFoundHandler())
	defer unavailableServer.Close()
	blockedServer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer blockedServer.Close()

	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, beaconCfg)
	rpcClient := rpc.NewBeaconRpcP2P(t.Context(), &blockingEnvelopeSentinel{}, beaconCfg, clock, nil)
	cfg := &Cfg{
		rpc:                   rpcClient,
		beaconCfg:             beaconCfg,
		gloasEnvelopeAcceptor: &testGloasEnvelopeAcceptor{retainOnSuccess: true},
		gloasEnvelopeHTTPURLs: resolveGloasEnvelopeHTTPURLs(clparams.CaplinConfig{
			CheckpointSyncURLs: []string{
				blockedServer.URL + "/eth/v2/debug/beacon/states/finalized",
				unavailableServer.URL + "/eth/v2/debug/beacon/states/finalized",
				server.URL + "/eth/v2/debug/beacon/states/finalized",
			},
		}),
	}
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	got := fetchParentEnvelopes(ctx, cfg, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root})
	require.Contains(t, got, common.Hash(root))
	require.Equal(t, common.Hash(root), got[common.Hash(root)].Message.BeaconBlockRoot)
}

func TestFetchParentEnvelopesRetriesHTTPAfterP2PPhase(t *testing.T) {
	beaconCfg, block, root, wire := gloasEnvelopeFixture(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			http.NotFound(w, nil)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, err := bytes.NewReader(wire).WriteTo(w)
		require.NoError(t, err)
	}))
	defer server.Close()

	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, beaconCfg)
	rpcClient := rpc.NewBeaconRpcP2P(t.Context(), &blockingEnvelopeSentinel{}, beaconCfg, clock, nil)
	cfg := &Cfg{
		rpc:                   rpcClient,
		beaconCfg:             beaconCfg,
		gloasEnvelopeAcceptor: &testGloasEnvelopeAcceptor{retainOnSuccess: true},
		gloasEnvelopeHTTPURLs: []string{server.URL},
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	got := fetchParentEnvelopes(ctx, cfg, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root})
	require.Contains(t, got, common.Hash(root))
	require.GreaterOrEqual(t, calls.Load(), int32(2))
}

func TestFetchParentEnvelopesRejectsInvalidHTTPCandidateBeforeSelectingWinner(t *testing.T) {
	beaconCfg, block, root, _ := gloasEnvelopeFixture(t)
	badHash := common.Hash{1}
	goodHash := common.Hash{2}
	badWire := gloasEnvelopeWireWithPayloadHash(t, beaconCfg, root, badHash)
	goodWire := gloasEnvelopeWireWithPayloadHash(t, beaconCfg, root, goodHash)
	badRejected := make(chan struct{})
	server := func(wait <-chan struct{}, wire []byte) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if wait != nil {
				<-wait
			}
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, err := bytes.NewReader(wire).WriteTo(w)
			require.NoError(t, err)
		}))
	}
	badServer := server(nil, badWire)
	defer badServer.Close()
	goodServer := server(badRejected, goodWire)
	defer goodServer.Close()

	acceptor := &testGloasEnvelopeAcceptor{
		retainOnSuccess: true,
		accept: func(_ context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
			if envelope.Message.Payload.BlockHash == badHash {
				close(badRejected)
				return errors.New("invalid envelope")
			}
			return nil
		},
	}
	cfg := &Cfg{beaconCfg: beaconCfg, gloasEnvelopeAcceptor: acceptor}
	envelopes := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)
	remaining := fetchParentEnvelopesFromBeaconAPIs(t.Context(), cfg, []string{badServer.URL, goodServer.URL}, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root}, envelopes)
	require.Empty(t, remaining)
	require.Equal(t, goodHash, envelopes[common.Hash(root)].Message.Payload.BlockHash)
}

func TestFetchParentEnvelopesCoalescesMatchingPersistedEnvelopeRevalidation(t *testing.T) {
	beaconCfg, block, root, wire := gloasEnvelopeFixture(t)
	server := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, err := bytes.NewReader(wire).WriteTo(w)
			require.NoError(t, err)
		}))
	}
	serverA := server()
	defer serverA.Close()
	serverB := server()
	defer serverB.Close()
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(beaconCfg)}
	require.NoError(t, envelope.DecodeSSZStrict(wire, int(clparams.GloasVersion)))
	acceptor := &testGloasEnvelopeAcceptor{
		retained:  map[common.Hash]struct{}{common.Hash(root): {}},
		persisted: map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{common.Hash(root): envelope},
		accept: func(context.Context, *cltypes.SignedExecutionPayloadEnvelope) error {
			return fmt.Errorf("revalidated persisted envelope: %w", forkchoice.ErrIgnore)
		},
	}
	cfg := &Cfg{beaconCfg: beaconCfg, gloasEnvelopeAcceptor: acceptor}
	envelopes := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)

	remaining := fetchParentEnvelopesFromBeaconAPIs(t.Context(), cfg, []string{serverA.URL, serverB.URL}, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root}, envelopes)

	require.Empty(t, remaining)
	require.Contains(t, envelopes, common.Hash(root))
	require.Equal(t, int32(1), acceptor.calls.Load())
}

func TestFetchParentEnvelopesRetainsPartialHTTPResultAfterRequestTimeout(t *testing.T) {
	beaconCfg, blockA, rootA, wireA := gloasEnvelopeFixture(t)
	blockB := cltypes.NewSignedBeaconBlock(beaconCfg, clparams.GloasVersion)
	blockB.Block.Slot = blockA.Block.Slot + 1
	rootB, err := blockB.Block.HashSSZ()
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == fmt.Sprintf("/eth/v1/beacon/execution_payload_envelopes/0x%x", rootA) {
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, err := bytes.NewReader(wireA).WriteTo(w)
			require.NoError(t, err)
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()

	acceptor := &testGloasEnvelopeAcceptor{
		retainOnSuccess: true,
		accept: func(ctx context.Context, _ *cltypes.SignedExecutionPayloadEnvelope) error {
			return ctx.Err()
		},
	}
	cfg := &Cfg{beaconCfg: beaconCfg, gloasEnvelopeAcceptor: acceptor}
	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()
	envelopes := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)
	remaining := fetchParentEnvelopesFromBeaconAPIs(ctx, cfg, []string{server.URL}, []*cltypes.SignedBeaconBlock{blockA, blockB}, [][32]byte{rootA, rootB}, envelopes)
	require.Contains(t, envelopes, common.Hash(rootA))
	require.Equal(t, [][32]byte{rootB}, remaining)
}

func TestRetainAcceptedParentEnvelopesRequiresCanonicalRetention(t *testing.T) {
	beaconCfg, _, root, wire := gloasEnvelopeFixture(t)
	for _, tc := range []struct {
		name            string
		acceptErr       error
		retainOnSuccess bool
		want            bool
	}{
		{name: "validation error", acceptErr: errors.New("invalid")},
		{name: "success without retention"},
		{name: "successful retention", retainOnSuccess: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(beaconCfg)}
			require.NoError(t, envelope.DecodeSSZStrict(wire, int(clparams.GloasVersion)))
			acceptor := &testGloasEnvelopeAcceptor{
				retainOnSuccess: tc.retainOnSuccess,
				accept: func(context.Context, *cltypes.SignedExecutionPayloadEnvelope) error {
					return tc.acceptErr
				},
			}
			envelopes := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{common.Hash(root): envelope}
			retainAcceptedParentEnvelopes(t.Context(), &Cfg{beaconCfg: beaconCfg, gloasEnvelopeAcceptor: acceptor}, envelopes)
			require.Equal(t, tc.want, envelopes[common.Hash(root)] != nil)
		})
	}
}

func TestRetainAcceptedParentEnvelopesHandlesMissingAcceptor(t *testing.T) {
	beaconCfg, _, root, wire := gloasEnvelopeFixture(t)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(beaconCfg)}
	require.NoError(t, envelope.DecodeSSZStrict(wire, int(clparams.GloasVersion)))
	envelopes := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{common.Hash(root): envelope}

	require.NotPanics(t, func() {
		retainAcceptedParentEnvelopes(t.Context(), &Cfg{beaconCfg: beaconCfg}, envelopes)
	})
	require.Empty(t, envelopes)
}

func TestRetainAcceptedParentEnvelopesKeepsMatchingPersistedEnvelopeAfterRevalidation(t *testing.T) {
	beaconCfg, _, root, wire := gloasEnvelopeFixture(t)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(beaconCfg)}
	require.NoError(t, envelope.DecodeSSZStrict(wire, int(clparams.GloasVersion)))
	acceptor := &testGloasEnvelopeAcceptor{
		retained:  map[common.Hash]struct{}{common.Hash(root): {}},
		persisted: map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{common.Hash(root): envelope.Clone().(*cltypes.SignedExecutionPayloadEnvelope)},
		accept: func(context.Context, *cltypes.SignedExecutionPayloadEnvelope) error {
			return fmt.Errorf("revalidated persisted envelope: %w", forkchoice.ErrIgnore)
		},
	}
	envelopes := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{common.Hash(root): envelope}

	retainAcceptedParentEnvelopes(t.Context(), &Cfg{beaconCfg: beaconCfg, gloasEnvelopeAcceptor: acceptor}, envelopes)

	require.Same(t, envelope, envelopes[common.Hash(root)])
	require.Equal(t, int32(1), acceptor.calls.Load())
}

func TestRetainAcceptedParentEnvelopesRejectsDifferentPersistedEnvelopeAfterIgnore(t *testing.T) {
	beaconCfg, _, root, wire := gloasEnvelopeFixture(t)
	candidate := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(beaconCfg)}
	require.NoError(t, candidate.DecodeSSZStrict(wire, int(clparams.GloasVersion)))
	persisted := candidate.Clone().(*cltypes.SignedExecutionPayloadEnvelope)
	persisted.Signature[0] = 1
	acceptor := &testGloasEnvelopeAcceptor{
		retained:  map[common.Hash]struct{}{common.Hash(root): {}},
		persisted: map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{common.Hash(root): persisted},
		accept: func(context.Context, *cltypes.SignedExecutionPayloadEnvelope) error {
			return fmt.Errorf("ignored different envelope: %w", forkchoice.ErrIgnore)
		},
	}
	envelopes := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{common.Hash(root): candidate}

	retainAcceptedParentEnvelopes(t.Context(), &Cfg{beaconCfg: beaconCfg, gloasEnvelopeAcceptor: acceptor}, envelopes)

	require.NotContains(t, envelopes, common.Hash(root))
}

func TestRetainAcceptedParentEnvelopesDoesNotLetOneCandidateBlockAnother(t *testing.T) {
	beaconCfg, blockA, rootA, wireA := gloasEnvelopeFixture(t)
	blockB := cltypes.NewSignedBeaconBlock(beaconCfg, clparams.GloasVersion)
	blockB.Block.Slot = blockA.Block.Slot + 1
	rootB, err := blockB.Block.HashSSZ()
	require.NoError(t, err)
	wireB := gloasEnvelopeWireWithPayloadHash(t, beaconCfg, rootB, common.Hash{2})
	decode := func(root common.Hash, wire []byte) *cltypes.SignedExecutionPayloadEnvelope {
		envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(beaconCfg)}
		require.NoError(t, envelope.DecodeSSZStrict(wire, int(clparams.GloasVersion)))
		require.Equal(t, root, envelope.Message.BeaconBlockRoot)
		return envelope
	}
	acceptor := &testGloasEnvelopeAcceptor{
		retainOnSuccess: true,
		accept: func(ctx context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
			if envelope.Message.BeaconBlockRoot == common.Hash(rootA) {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		},
	}
	envelopes := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{
		common.Hash(rootA): decode(common.Hash(rootA), wireA),
		common.Hash(rootB): decode(common.Hash(rootB), wireB),
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	retainAcceptedParentEnvelopes(ctx, &Cfg{beaconCfg: beaconCfg, gloasEnvelopeAcceptor: acceptor}, envelopes)
	require.NotContains(t, envelopes, common.Hash(rootA))
	require.Contains(t, envelopes, common.Hash(rootB))
}

func TestFetchParentEnvelopesPreservesP2PBudgetWithHTTPFallback(t *testing.T) {
	beaconCfg, block, root, _ := gloasEnvelopeFixture(t)
	httpStarted := make(chan struct{})
	var startedOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		startedOnce.Do(func() { close(httpStarted) })
		<-r.Context().Done()
	}))
	defer server.Close()
	deadline := make(chan time.Duration, 1)
	sentinel := &deadlineEnvelopeSentinel{deadline: deadline}
	clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix()), common.Hash{}, beaconCfg)
	rpcClient := rpc.NewBeaconRpcP2P(t.Context(), sentinel, beaconCfg, clock, nil)
	cfg := &Cfg{
		rpc:                   rpcClient,
		beaconCfg:             beaconCfg,
		gloasEnvelopeHTTPURLs: []string{server.URL},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		fetchParentEnvelopes(ctx, cfg, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root})
	}()

	<-httpStarted
	remaining := <-deadline
	cancel()
	<-done
	require.Greater(t, remaining, 7*time.Second)
}
