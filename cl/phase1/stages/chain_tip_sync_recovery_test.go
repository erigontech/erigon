package stages

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
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

func TestStoredParentEnvelopesRestoresReadableParents(t *testing.T) {
	storedRoot := common.Hash{1}
	mismatchedRoot := common.Hash{2}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: storedRoot}}
	reads := 0

	got := storedParentEnvelopes(
		[][32]byte{storedRoot, storedRoot, mismatchedRoot},
		func(root common.Hash) bool { return root == storedRoot || root == mismatchedRoot },
		func(root common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
			reads++
			if root == storedRoot {
				return envelope, nil
			}
			return &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: common.Hash{9}}}, nil
		},
	)

	require.Equal(t, 2, reads)
	require.Equal(t, map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{storedRoot: envelope}, got)
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
	require.False(t, parentEnvelopeNeedsRecovery(child, parent, true, execution_client.PayloadStatusValidated, true, true))
	require.True(t, parentEnvelopeNeedsRecovery(child, parent, true, execution_client.PayloadStatusValidated, true, false))
	require.True(t, parentEnvelopeNeedsRecovery(child, parent, true, execution_client.PayloadStatusNone, true, true))
	require.True(t, parentEnvelopeNeedsRecovery(child, parent, true, execution_client.PayloadStatusNone, false, true))
	require.False(t, parentEnvelopeNeedsRecovery(child, parent, true, execution_client.PayloadStatusInvalidated, true, false))
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
		deadline: time.Now().Add(-time.Second),
		results:  make(map[common.Hash]bool),
	}
	engine := &testExecutionEngine{}
	engine.newPayloadFn = func(ctx context.Context) (execution_client.PayloadStatus, error) {
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
	replay := storedParentPayloadReplay{deadline: time.Now(), results: make(map[common.Hash]bool)}

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
}
