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
)

type storedParentPayloadTestStore struct {
	has          bool
	status       execution_client.PayloadStatus
	statusFound  bool
	block        *cltypes.SignedBeaconBlock
	markRetained bool
	marked       execution_client.PayloadStatus
	requeued     []forkchoice.PendingELPayload
}

func (s *storedParentPayloadTestStore) HasEnvelope(common.Hash) bool { return s.has }

func (s *storedParentPayloadTestStore) GetRecentExecutionPayloadStatusByRoot(common.Hash) (execution_client.PayloadStatus, bool) {
	return s.status, s.statusFound
}

func (s *storedParentPayloadTestStore) GetBlock(common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	return s.block, s.block != nil
}

func (s *storedParentPayloadTestStore) MarkPayloadStatusIfRetained(_ common.Hash, _ common.Hash, status execution_client.PayloadStatus) (execution_client.PayloadStatus, bool) {
	s.marked = status
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
	require.False(t, parentEnvelopeNeedsRecovery(child, parent, true, execution_client.PayloadStatusValidated, true))
	require.True(t, parentEnvelopeNeedsRecovery(child, parent, true, execution_client.PayloadStatusNone, true))
	require.True(t, parentEnvelopeNeedsRecovery(child, parent, true, execution_client.PayloadStatusNone, false))
}

func TestEnsureStoredParentPayloadAcceptedReplaysMissingVerdict(t *testing.T) {
	root := common.Hash{1}
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	payload.BlockHash = common.Hash{2}
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
	retries := 0

	accepted := ensureStoredParentPayloadAccepted(t.Context(), store, root, envelope, func(context.Context, *cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (execution_client.PayloadStatus, error) {
		retries++
		return execution_client.PayloadStatusNotValidated, nil
	})

	require.True(t, accepted)
	require.Equal(t, 1, retries)
	require.EqualValues(t, execution_client.PayloadStatusNotValidated, store.marked)
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

	accepted := ensureStoredParentPayloadAccepted(t.Context(), store, root, envelope, func(context.Context, *cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (execution_client.PayloadStatus, error) {
		t.Fatal("invalidated payload must not be replayed")
		return execution_client.PayloadStatusNone, nil
	})

	require.False(t, accepted)
	require.Empty(t, store.requeued)
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

	accepted := ensureStoredParentPayloadAccepted(t.Context(), store, root, envelope, nil)

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
	retries := 0

	accepted := replay.accepted(t.Context(), store, root, envelope, forkchoice.ErrIgnore, func(ctx context.Context, _ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (execution_client.PayloadStatus, error) {
		retries++
		<-ctx.Done()
		return execution_client.PayloadStatusNone, ctx.Err()
	})
	replayed := replay.accepted(t.Context(), store, root, envelope, forkchoice.ErrIgnore, func(context.Context, *cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (execution_client.PayloadStatus, error) {
		t.Fatal("cached parent payload must not be replayed")
		return execution_client.PayloadStatusNone, nil
	})

	require.False(t, accepted)
	require.False(t, replayed)
	require.Equal(t, 1, retries)
}

func TestStoredParentPayloadReplayRejectsApplyFailure(t *testing.T) {
	root := common.Hash{1}
	store := &storedParentPayloadTestStore{has: true, markRetained: true}
	replay := storedParentPayloadReplay{deadline: time.Now(), results: make(map[common.Hash]bool)}

	accepted := replay.accepted(
		t.Context(),
		store,
		root,
		&cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
			BeaconBlockRoot: root,
			Payload:         cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig),
		}},
		forkchoice.ErrInvalidExecutionPayloadEnvelope,
		nil,
	)

	require.False(t, accepted)
	require.EqualValues(t, execution_client.PayloadStatusNone, store.marked)
}
