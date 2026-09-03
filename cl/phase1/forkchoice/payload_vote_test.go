package forkchoice

import (
	"errors"
	"fmt"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/optimistic"
	"github.com/erigontech/erigon/common"
)

type ptcVoteForkGraph struct {
	fork_graph.ForkGraph
	envelopes        map[common.Hash]bool
	blocks           map[common.Hash]*cltypes.SignedBeaconBlock
	acceptedPayloads map[common.Hash]bool
}

func (g ptcVoteForkGraph) HasEnvelope(root common.Hash) bool {
	return g.envelopes[root]
}

func (g ptcVoteForkGraph) IsBlockInvalid(common.Hash) bool {
	return false
}

func (g ptcVoteForkGraph) MarkPayloadUnavailable(common.Hash) {}
func (g ptcVoteForkGraph) MarkPayloadAvailable(common.Hash)   {}
func (g ptcVoteForkGraph) IsPayloadUnavailable(common.Hash) bool {
	return false
}
func (g ptcVoteForkGraph) MarkPayloadAccepted(common.Hash, bool) {}
func (g ptcVoteForkGraph) ClearPayloadAccepted(common.Hash)      {}
func (g ptcVoteForkGraph) PayloadAccepted(root common.Hash) (bool, bool) {
	verified, ok := g.acceptedPayloads[root]
	return verified, ok
}

func (g ptcVoteForkGraph) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	block, ok := g.blocks[root]
	return block, ok
}

func (g ptcVoteForkGraph) GetHeader(root common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	block, ok := g.blocks[root]
	if !ok || block == nil || block.Block == nil {
		return nil, false
	}
	return &cltypes.BeaconBlockHeader{Slot: block.Block.Slot, ParentRoot: block.Block.ParentRoot}, true
}

type payloadVoteForkGraph struct {
	fork_graph.ForkGraph
	hasEnvelope        bool
	dumpedEnvelope     *common.Hash
	invalidatedHeader  *common.Hash
	unavailablePayload *common.Hash
	acceptedPayloads   map[common.Hash]bool
	retained           *bool
}

func (g payloadVoteForkGraph) IsBlockRetained(common.Hash) bool {
	return g.retained == nil || *g.retained
}

func (g payloadVoteForkGraph) WithRetainedBlock(_ common.Hash, fn func()) bool {
	if !g.IsBlockRetained(common.Hash{}) {
		return false
	}
	fn()
	return true
}

func (g payloadVoteForkGraph) HasEnvelope(common.Hash) bool {
	return g.hasEnvelope
}

func (g payloadVoteForkGraph) ReadEnvelopeFromDisk(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	return nil, nil
}

func (g payloadVoteForkGraph) DumpEnvelopeOnDisk(blockRoot common.Hash, _ *cltypes.SignedExecutionPayloadEnvelope) error {
	if g.dumpedEnvelope != nil {
		*g.dumpedEnvelope = blockRoot
	}
	return nil
}

func (g payloadVoteForkGraph) MarkHeaderAsInvalid(blockRoot common.Hash) {
	if g.invalidatedHeader != nil {
		*g.invalidatedHeader = blockRoot
	}
}

func (g payloadVoteForkGraph) IsBlockInvalid(blockRoot common.Hash) bool {
	return g.invalidatedHeader != nil && *g.invalidatedHeader == blockRoot
}

func (g payloadVoteForkGraph) MarkPayloadUnavailable(blockRoot common.Hash) {
	if g.unavailablePayload != nil {
		*g.unavailablePayload = blockRoot
	}
}

func (g payloadVoteForkGraph) MarkPayloadAvailable(blockRoot common.Hash) {
	if g.unavailablePayload != nil && *g.unavailablePayload == blockRoot {
		*g.unavailablePayload = common.Hash{}
	}
}

func (g payloadVoteForkGraph) IsPayloadUnavailable(blockRoot common.Hash) bool {
	return g.unavailablePayload != nil && *g.unavailablePayload == blockRoot
}

func (g payloadVoteForkGraph) MarkPayloadAccepted(blockRoot common.Hash, verified bool) {
	if g.acceptedPayloads != nil {
		g.acceptedPayloads[blockRoot] = verified
	}
}

func (g payloadVoteForkGraph) ClearPayloadAccepted(blockRoot common.Hash) {
	delete(g.acceptedPayloads, blockRoot)
}

func (g payloadVoteForkGraph) PayloadAccepted(blockRoot common.Hash) (bool, bool) {
	verified, ok := g.acceptedPayloads[blockRoot]
	return verified, ok
}

func TestGetPTCFromWindow(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	s := state2.New(cfg)
	s.SetVersion(clparams.GloasVersion)

	slotsPerEpoch := cfg.SlotsPerEpoch
	slot := 2*slotsPerEpoch + 5
	require.NoError(t, s.SetSlot(slot))

	ptcWindow := solid.NewUint64VectorOfVectors(int(3*slotsPerEpoch), 4)
	windowIndex := slotsPerEpoch + slot%slotsPerEpoch
	vec := ptcWindow.Get(int(windowIndex))
	for i := 0; i < vec.Length(); i++ {
		vec.Set(i, uint64(10+i))
	}
	s.SetPtcWindow(ptcWindow)

	ptc, err := s.GetPTCFromWindow(slot)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 11, 12, 13}, ptc)

	ptc[0] = 99
	require.Equal(t, uint64(10), ptcWindow.Get(int(windowIndex)).Get(0))
}

func TestGetPTCFromWindowRejectsSlotOutsideWindow(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	s := state2.New(cfg)
	s.SetVersion(clparams.GloasVersion)
	require.NoError(t, s.SetSlot(2*cfg.SlotsPerEpoch+5))
	s.SetPtcWindow(solid.NewUint64VectorOfVectors(int(3*cfg.SlotsPerEpoch), 4))

	_, err := s.GetPTCFromWindow(0)
	require.Error(t, err)
}

func TestPtcBoolToVote(t *testing.T) {
	require.Equal(t, int8(1), boolToVote(true))
	require.Equal(t, int8(-1), boolToVote(false))
}

func TestPtcPayloadTimelinessVoteCounting(t *testing.T) {
	root := common.HexToHash("0x01")
	f := newPtcVoteTestStore(root)

	for _, tc := range []struct {
		name       string
		trueVotes  int
		falseVotes int
		timely     bool
		want       bool
	}{
		{
			name:   "all unvoted does not reach true majority",
			timely: true,
			want:   false,
		},
		{
			name:   "all unvoted does not reach false majority",
			timely: false,
			want:   false,
		},
		{
			name:       "exactly at true threshold is not majority",
			trueVotes:  ptcVoteThreshold(),
			falseVotes: 0,
			timely:     true,
			want:       false,
		},
		{
			name:      "true votes over threshold reach majority",
			trueVotes: ptcVoteThreshold() + 1,
			timely:    true,
			want:      true,
		},
		{
			name:       "false votes over threshold reach majority",
			falseVotes: ptcVoteThreshold() + 1,
			timely:     false,
			want:       true,
		},
		{
			name:       "mixed votes exactly split do not reach majority",
			trueVotes:  ptcVoteThreshold(),
			falseVotes: ptcVoteThreshold(),
			timely:     true,
			want:       false,
		},
		{
			name:       "mixed votes count only explicit true votes",
			trueVotes:  ptcVoteThreshold() + 1,
			falseVotes: ptcVoteThreshold() - 1,
			timely:     true,
			want:       true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f.payloadTimelinessVote.Store(root, ptcVotes(tc.trueVotes, tc.falseVotes))
			require.Equal(t, tc.want, f.payloadTimeliness(root, tc.timely))
		})
	}
}

func TestPtcPayloadDataAvailabilityVoteCounting(t *testing.T) {
	root := common.HexToHash("0x02")
	f := newPtcVoteTestStore(root)

	for _, tc := range []struct {
		name       string
		trueVotes  int
		falseVotes int
		available  bool
		want       bool
	}{
		{
			name:      "all unvoted does not reach available majority",
			available: true,
			want:      false,
		},
		{
			name:      "all unvoted does not reach unavailable majority",
			available: false,
			want:      false,
		},
		{
			name:       "exactly at unavailable threshold is not majority",
			falseVotes: ptcVoteThreshold(),
			available:  false,
			want:       false,
		},
		{
			name:      "available votes over threshold reach majority",
			trueVotes: ptcVoteThreshold() + 1,
			available: true,
			want:      true,
		},
		{
			name:       "unavailable votes over threshold reach majority",
			falseVotes: ptcVoteThreshold() + 1,
			available:  false,
			want:       true,
		},
		{
			name:       "mixed votes exactly split do not reach majority",
			trueVotes:  ptcVoteThreshold(),
			falseVotes: ptcVoteThreshold(),
			available:  false,
			want:       false,
		},
		{
			name:       "mixed votes count only explicit unavailable votes",
			trueVotes:  ptcVoteThreshold() - 1,
			falseVotes: ptcVoteThreshold() + 1,
			available:  false,
			want:       true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f.payloadDataAvailabilityVote.Store(root, ptcVotes(tc.trueVotes, tc.falseVotes))
			require.Equal(t, tc.want, f.payloadDataAvailability(root, tc.available))
		})
	}
}

func TestPtcShouldBuildOnFullNoVotesCast(t *testing.T) {
	root := common.HexToHash("0x03")
	f := newPtcVoteTestStore(root)
	head := ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusFull}

	require.True(t, f.ShouldBuildOnFull(head, f.Slot()))

	f.payloadDataAvailabilityVote.Store(root, ptcVotes(0, 0))
	require.True(t, f.ShouldBuildOnFull(head, f.Slot()))
}

func TestPtcShouldBuildOnFullWithUnavailableMajority(t *testing.T) {
	root := common.HexToHash("0x04")
	f := newPtcVoteTestStore(root)
	f.payloadDataAvailabilityVote.Store(root, ptcVotes(0, ptcVoteThreshold()+1))

	require.False(t, f.ShouldBuildOnFull(ForkChoiceNode{
		Root:          root,
		PayloadStatus: cltypes.PayloadStatusFull,
	}, f.Slot()))
	require.False(t, f.ShouldBuildOnFull(ForkChoiceNode{
		Root:          root,
		PayloadStatus: cltypes.PayloadStatusEmpty,
	}, f.Slot()))
}

func TestPtcShouldBuildOnFullWithLatePayloadMajority(t *testing.T) {
	root := common.HexToHash("0x07")
	f := newPtcVoteTestStore(root)
	f.payloadTimelinessVote.Store(root, ptcVotes(0, ptcVoteThreshold()+1))

	require.False(t, f.ShouldBuildOnFull(ForkChoiceNode{
		Root:          root,
		PayloadStatus: cltypes.PayloadStatusFull,
	}, f.Slot()))
}

func TestPtcShouldBuildOnFullIgnoresVotesBeforePreviousSlot(t *testing.T) {
	root := common.HexToHash("0x05")
	f := newPtcVoteTestStore(root)
	f.time.Store(2 * f.beaconCfg.SecondsPerSlot)
	f.payloadDataAvailabilityVote.Store(root, ptcVotes(0, ptcVoteThreshold()+1))
	f.payloadTimelinessVote.Store(root, ptcVotes(0, ptcVoteThreshold()+1))

	require.True(t, f.ShouldBuildOnFull(ForkChoiceNode{
		Root:          root,
		PayloadStatus: cltypes.PayloadStatusFull,
	}, f.Slot()))
}

func TestShouldBuildOnFullUsesExplicitTargetSlot(t *testing.T) {
	root := common.HexToHash("0x08")
	f := newPtcVoteTestStore(root)
	f.forkGraph.(ptcVoteForkGraph).blocks[root].Block.Slot = 10
	f.payloadDataAvailabilityVote.Store(root, ptcVotes(0, ptcVoteThreshold()+1))
	f.payloadTimelinessVote.Store(root, ptcVotes(0, ptcVoteThreshold()+1))

	require.True(t, f.ShouldBuildOnFull(ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusFull}, 12))
	require.False(t, f.ShouldBuildOnFull(ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusEmpty}, 12))
	require.False(t, f.ShouldBuildOnFull(ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusPending}, 12))
	require.False(t, f.ShouldBuildOnFull(ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusFull}, 11))
	require.False(t, f.ShouldBuildOnFull(ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusEmpty}, 11))
	require.False(t, f.ShouldBuildOnFull(ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusPending}, 11))
}

func TestPtcIsPreviousSlotPayloadDecision(t *testing.T) {
	root := common.HexToHash("0x06")
	f := newPtcVoteTestStore(root)

	require.True(t, f.isPreviousSlotPayloadDecision(ForkChoiceNode{
		Root:          root,
		PayloadStatus: cltypes.PayloadStatusFull,
	}))
	require.True(t, f.isPreviousSlotPayloadDecision(ForkChoiceNode{
		Root:          root,
		PayloadStatus: cltypes.PayloadStatusEmpty,
	}))
	require.False(t, f.isPreviousSlotPayloadDecision(ForkChoiceNode{
		Root:          root,
		PayloadStatus: cltypes.PayloadStatusPending,
	}))

	f.time.Store(2 * f.beaconCfg.SecondsPerSlot)
	require.False(t, f.isPreviousSlotPayloadDecision(ForkChoiceNode{
		Root:          root,
		PayloadStatus: cltypes.PayloadStatusFull,
	}))
}

func TestGloasForkChoiceUsesPersistedPayload(t *testing.T) {
	root := common.HexToHash("0x1234")

	tests := []struct {
		name          string
		hasEnvelope   bool
		verified      bool
		optimistic    bool
		wantFullChild bool
	}{
		{
			name:          "envelope present without EL status remains EMPTY only",
			hasEnvelope:   true,
			verified:      false,
			wantFullChild: false,
		},
		{
			name:          "optimistic EL status produces FULL child",
			hasEnvelope:   true,
			optimistic:    true,
			wantFullChild: true,
		},
		{
			name:          "envelope present and verified produces FULL child",
			hasEnvelope:   true,
			verified:      true,
			wantFullChild: true,
		},
		{
			name:          "no envelope means EMPTY only",
			hasEnvelope:   false,
			verified:      false,
			wantFullChild: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newPayloadVoteTestStore(t, root, tt.hasEnvelope, tt.verified)
			if tt.optimistic {
				f.payloadStatusByRoot.Add(root, execution_client.PayloadStatusNotValidated)
			}

			children := f.getNodeChildren(ForkChoiceNode{
				Root:          root,
				PayloadStatus: cltypes.PayloadStatusPending,
			}, nil)
			require.Equal(t, tt.wantFullChild, hasPayloadStatus(children, cltypes.PayloadStatusFull))
			require.True(t, hasPayloadStatus(children, cltypes.PayloadStatusEmpty))

			require.Equal(t, tt.wantFullChild, f.ShouldExtendPayload(root))
			require.Equal(t, tt.wantFullChild, f.payloadTimeliness(root, true))
			require.Equal(t, tt.wantFullChild, f.payloadDataAvailability(root, true))
		})
	}
}

func TestIsPayloadVerifiedStrictSemantics(t *testing.T) {
	root := common.HexToHash("0x5678")

	t.Run("envelope on disk but not EL-verified", func(t *testing.T) {
		f := newPayloadVoteTestStore(t, root, true, false)
		require.False(t, f.IsPayloadVerified(root))
	})

	t.Run("missing envelope is locally unavailable in both vote directions", func(t *testing.T) {
		f := newPayloadVoteTestStore(t, root, false, false)
		require.False(t, f.payloadTimeliness(root, true))
		require.True(t, f.payloadTimeliness(root, false))
		require.False(t, f.payloadDataAvailability(root, true))
		require.True(t, f.payloadDataAvailability(root, false))
	})

	t.Run("EL-verified and envelope present", func(t *testing.T) {
		f := newPayloadVoteTestStore(t, root, true, true)
		require.True(t, f.IsPayloadVerified(root))
	})

	t.Run("EL-verified before envelope publication", func(t *testing.T) {
		f := newPayloadVoteTestStore(t, root, false, false)
		f.MarkPayloadVerified(root, common.HexToHash("0xabcd"))
		require.False(t, f.HasEnvelope(root))
		require.False(t, f.IsPayloadVerified(root))
	})

	t.Run("mark verified", func(t *testing.T) {
		f := newPayloadVoteTestStore(t, root, true, false)
		execHash := common.HexToHash("0xabcd")
		f.MarkPayloadVerified(root, execHash)
		require.True(t, f.IsPayloadVerified(root))

		status, ok := f.GetRecentExecutionPayloadStatusByRoot(root)
		require.True(t, ok)
		require.Equal(t, execution_client.PayloadStatus(execution_client.PayloadStatusValidated), status)
	})

	t.Run("nil cache returns false", func(t *testing.T) {
		f := &ForkChoiceStore{}
		require.False(t, f.IsPayloadVerified(root))
	})
}

func TestMarkPayloadInvalidRecordsELRejection(t *testing.T) {
	root := common.HexToHash("0x5678")
	execHash := common.HexToHash("0xabcd")
	invalidatedHeader := common.Hash{}

	f := newPayloadVoteTestStore(t, root, true, true)
	f.forkGraph = payloadVoteForkGraph{
		hasEnvelope:       true,
		invalidatedHeader: &invalidatedHeader,
	}
	f.MarkPayloadInvalid(root, execHash)

	require.False(t, f.IsPayloadVerified(root))
	children := f.getNodeChildren(ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusPending}, nil)
	require.False(t, hasPayloadStatus(children, cltypes.PayloadStatusFull))
	status, ok := f.GetRecentExecutionPayloadStatus(execHash)
	require.True(t, ok)
	require.Equal(t, execution_client.PayloadStatus(execution_client.PayloadStatusInvalidated), status)
	status, ok = f.GetRecentExecutionPayloadStatusByRoot(root)
	require.True(t, ok)
	require.Equal(t, execution_client.PayloadStatus(execution_client.PayloadStatusInvalidated), status)
	require.Equal(t, root, invalidatedHeader)
}

func TestPayloadValidationResultAfterPruneDoesNotRestoreStatus(t *testing.T) {
	root := common.HexToHash("0x5678")
	retained := false
	accepted := map[common.Hash]bool{}
	f := newPayloadVoteTestStore(t, root, true, false)
	f.forkGraph = payloadVoteForkGraph{hasEnvelope: true, retained: &retained, acceptedPayloads: accepted}
	envelope := &cltypes.ExecutionPayloadEnvelope{Payload: &cltypes.Eth1Block{BlockHash: common.HexToHash("0xabcd")}}
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{}}

	err := f.applyPayloadValidationResultLocked(execution_client.PayloadStatusValidated, nil, envelope, block, root)
	require.ErrorIs(t, err, ErrIgnore)
	require.Empty(t, accepted)
	_, ok := f.GetRecentExecutionPayloadStatusByRoot(root)
	require.False(t, ok)
}

func TestInvalidPayloadRemainsUnavailableAfterRootStatusEviction(t *testing.T) {
	root := common.HexToHash("0x5678")
	invalidatedHeader := common.Hash{}
	f := newPayloadVoteTestStore(t, root, true, false)
	f.forkGraph = payloadVoteForkGraph{hasEnvelope: true, invalidatedHeader: &invalidatedHeader}
	statusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
	require.NoError(t, err)
	f.payloadStatusByRoot = statusByRoot

	f.MarkPayloadInvalid(root, common.HexToHash("0xabcd"))
	f.payloadStatusByRoot.Add(common.HexToHash("0x9999"), execution_client.PayloadStatusValidated)
	_, cached := f.payloadStatusByRoot.Get(root)
	require.False(t, cached)
	status, found := f.GetRecentExecutionPayloadStatusByRoot(root)
	require.True(t, found)
	require.Equal(t, execution_client.PayloadStatus(execution_client.PayloadStatusInvalidated), status)
	require.Equal(t, root, invalidatedHeader)

	require.False(t, f.isPayloadAvailable(root))
	require.False(t, f.IsPayloadVerified(root))
}

func TestPayloadAvailabilityByEngineStatus(t *testing.T) {
	root := common.HexToHash("0x5678")
	for _, test := range []struct {
		name   string
		status execution_client.PayloadStatus
		want   bool
	}{
		{name: "engine error", status: execution_client.PayloadStatusNone},
		{name: "optimistic", status: execution_client.PayloadStatusNotValidated, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newPayloadVoteTestStore(t, root, true, false)
			f.payloadStatusByRoot.Add(root, test.status)

			require.Equal(t, test.want, f.isPayloadAvailable(root))
			require.False(t, f.IsPayloadVerified(root))
		})
	}
}

func TestValidateParentPayloadPathUsesValidationAvailability(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	parentRoot := common.HexToHash("0x5678")
	executionHash := common.HexToHash("0xabcd")
	parent := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	parent.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{BlockHash: executionHash}}
	child := cltypes.NewBeaconBlock(cfg, clparams.GloasVersion)
	child.ParentRoot = parentRoot
	child.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{ParentBlockHash: executionHash}}

	for _, test := range []struct {
		name       string
		status     execution_client.PayloadStatus
		withStatus bool
		wantErr    bool
	}{
		{name: "engine error", status: execution_client.PayloadStatusNone, withStatus: true, wantErr: true},
		{name: "optimistic", status: execution_client.PayloadStatusNotValidated, withStatus: true},
		{name: "status absent", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newPayloadVoteTestStore(t, parentRoot, true, false)
			f.forkGraph = ptcVoteForkGraph{
				envelopes: map[common.Hash]bool{parentRoot: true},
				blocks:    map[common.Hash]*cltypes.SignedBeaconBlock{parentRoot: parent},
			}
			if test.withStatus {
				f.payloadStatusByRoot.Add(parentRoot, test.status)
			}

			err := f.validateParentPayloadPath(child, true)
			if test.wantErr {
				require.ErrorIs(t, err, ErrParentEnvelopePending)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestApplyPayloadValidationResultRecordsRootAvailability(t *testing.T) {
	root := common.HexToHash("0x5678")
	for _, test := range []struct {
		name      string
		status    execution_client.PayloadStatus
		wantErr   error
		available bool
	}{
		{name: "engine error", status: execution_client.PayloadStatusNone, wantErr: errELBehind},
		{name: "optimistic", status: execution_client.PayloadStatusNotValidated, available: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newPayloadVoteTestStore(t, root, true, false)
			unavailableRoot := common.Hash{}
			invalidRoot := common.Hash{}
			f.forkGraph = payloadVoteForkGraph{
				hasEnvelope:        true,
				invalidatedHeader:  &invalidRoot,
				unavailablePayload: &unavailableRoot,
				acceptedPayloads:   make(map[common.Hash]bool),
			}
			gasLimits, err := lru.New[common.Hash, uint64](16)
			require.NoError(t, err)
			f.executionPayloadGasLimit = gasLimits
			statusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
			require.NoError(t, err)
			f.payloadStatusByRoot = statusByRoot
			f.headHash = root
			f.headPayloadStatus = cltypes.PayloadStatusFull
			envelope := cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)
			envelope.Payload.BlockHash = common.HexToHash("0xabcd")
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)

			var validationErr error
			if test.status == execution_client.PayloadStatusNone {
				validationErr = errors.New("engine unavailable")
			}
			err = f.applyPayloadValidationResultLocked(test.status, validationErr, envelope, block, root)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
			}
			status, found := f.GetRecentExecutionPayloadStatusByRoot(root)
			require.True(t, found)
			require.Equal(t, test.status, status)
			f.payloadStatusByRoot.Add(common.HexToHash("0x9999"), execution_client.PayloadStatusValidated)
			require.Equal(t, test.available, f.isPayloadAvailable(root))
			require.Equal(t, common.Hash{}, f.headHash)
			require.Equal(t, cltypes.PayloadStatusPending, f.headPayloadStatus)
		})
	}
}

func TestPayloadStatusTransitionsUpdateDurableAvailability(t *testing.T) {
	root := common.HexToHash("0x5678")
	execHash := common.HexToHash("0xabcd")
	for _, test := range []struct {
		name      string
		initial   execution_client.PayloadStatus
		next      execution_client.PayloadStatus
		available bool
		verified  bool
		changed   bool
		effective execution_client.PayloadStatus
	}{
		{name: "none to optimistic", initial: execution_client.PayloadStatusNone, next: execution_client.PayloadStatusNotValidated, available: true, changed: true, effective: execution_client.PayloadStatusNotValidated},
		{name: "none to validated", initial: execution_client.PayloadStatusNone, next: execution_client.PayloadStatusValidated, available: true, verified: true, changed: true, effective: execution_client.PayloadStatusValidated},
		{name: "none to invalidated", initial: execution_client.PayloadStatusNone, next: execution_client.PayloadStatusInvalidated, changed: true, effective: execution_client.PayloadStatusInvalidated},
		{name: "optimistic to none", initial: execution_client.PayloadStatusNotValidated, next: execution_client.PayloadStatusNone, available: true, effective: execution_client.PayloadStatusNotValidated},
		{name: "validated to none", initial: execution_client.PayloadStatusValidated, next: execution_client.PayloadStatusNone, available: true, verified: true, effective: execution_client.PayloadStatusValidated},
		{name: "validated to optimistic", initial: execution_client.PayloadStatusValidated, next: execution_client.PayloadStatusNotValidated, available: true, verified: true, effective: execution_client.PayloadStatusValidated},
		{name: "invalidated to none", initial: execution_client.PayloadStatusInvalidated, next: execution_client.PayloadStatusNone, effective: execution_client.PayloadStatusInvalidated},
		{name: "invalidated to optimistic", initial: execution_client.PayloadStatusInvalidated, next: execution_client.PayloadStatusNotValidated, effective: execution_client.PayloadStatusInvalidated},
		{name: "invalidated to validated", initial: execution_client.PayloadStatusInvalidated, next: execution_client.PayloadStatusValidated, effective: execution_client.PayloadStatusInvalidated},
	} {
		t.Run(test.name, func(t *testing.T) {
			unavailableRoot := common.Hash{}
			invalidRoot := common.Hash{}
			f := newPayloadVoteTestStore(t, root, true, false)
			f.forkGraph = payloadVoteForkGraph{
				hasEnvelope:        true,
				invalidatedHeader:  &invalidRoot,
				unavailablePayload: &unavailableRoot,
				acceptedPayloads:   make(map[common.Hash]bool),
			}
			statusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
			require.NoError(t, err)
			f.payloadStatusByRoot = statusByRoot

			f.MarkPayloadStatus(root, execHash, test.initial)
			f.payloadStatusByRoot.Add(common.HexToHash("0x9999"), execution_client.PayloadStatusValidated)
			f.headHash = root
			f.headPayloadStatus = cltypes.PayloadStatusFull

			effective := f.MarkPayloadStatus(root, execHash, test.next)
			require.Equal(t, test.effective, effective)
			require.Equal(t, test.available, f.isPayloadAvailable(root))
			require.Equal(t, test.verified, f.IsPayloadVerified(root))
			f.verifiedExecutionPayload.Add(common.HexToHash("0x9999"), struct{}{})
			require.Equal(t, test.verified, f.IsPayloadVerified(root))
			if test.changed {
				require.Equal(t, common.Hash{}, f.headHash)
				require.Equal(t, cltypes.PayloadStatusPending, f.headPayloadStatus)
			} else {
				require.Equal(t, root, f.headHash)
				require.Equal(t, cltypes.PayloadStatusFull, f.headPayloadStatus)
			}
		})
	}
}

func TestPayloadStatusGetterUsesDurableAuthorityAfterEviction(t *testing.T) {
	root := common.HexToHash("0x5678")
	for _, status := range []execution_client.PayloadStatus{
		execution_client.PayloadStatusNone,
		execution_client.PayloadStatusNotValidated,
		execution_client.PayloadStatusValidated,
		execution_client.PayloadStatusInvalidated,
	} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			unavailableRoot := common.Hash{}
			invalidRoot := common.Hash{}
			f := newPayloadVoteTestStore(t, root, true, false)
			f.forkGraph = payloadVoteForkGraph{
				hasEnvelope:        true,
				invalidatedHeader:  &invalidRoot,
				unavailablePayload: &unavailableRoot,
				acceptedPayloads:   make(map[common.Hash]bool),
			}
			statusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](1)
			require.NoError(t, err)
			f.payloadStatusByRoot = statusByRoot

			f.MarkPayloadStatus(root, common.HexToHash("0xabcd"), status)
			f.payloadStatusByRoot.Add(common.HexToHash("0x9999"), execution_client.PayloadStatusValidated)
			got, found := f.GetRecentExecutionPayloadStatusByRoot(root)
			require.True(t, found)
			require.Equal(t, status, got)
		})
	}
}

func TestStoreAnchorEnvelopePersistsWithoutMarkingVerified(t *testing.T) {
	root := common.HexToHash("0x5678")
	execHash := common.HexToHash("0xabcd")
	dumpedEnvelope := common.Hash{}

	f := newPayloadVoteTestStore(t, root, true, false)
	f.forkGraph = payloadVoteForkGraph{dumpedEnvelope: &dumpedEnvelope}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			BeaconBlockRoot: root,
			Payload:         &cltypes.Eth1Block{BlockHash: execHash},
		},
	}

	require.NoError(t, f.StoreAnchorEnvelope(root, envelope))
	require.False(t, f.IsPayloadVerified(root))
	status, ok := f.GetRecentExecutionPayloadStatus(execHash)
	require.False(t, ok)
	require.Equal(t, execution_client.PayloadStatus(0), status)
	require.Equal(t, root, dumpedEnvelope)
}

func TestStoreAnchorEnvelopeRejectsRootMismatch(t *testing.T) {
	root := common.HexToHash("0x5678")
	otherRoot := common.HexToHash("0x9999")
	execHash := common.HexToHash("0xabcd")

	f := newPayloadVoteTestStore(t, root, true, false)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			BeaconBlockRoot: otherRoot,
			Payload:         &cltypes.Eth1Block{BlockHash: execHash},
		},
	}

	err := f.StoreAnchorEnvelope(root, envelope)
	require.Error(t, err)
	require.False(t, f.IsPayloadVerified(root))
	_, ok := f.GetRecentExecutionPayloadStatus(execHash)
	require.False(t, ok)
}

func newPtcVoteTestStore(root common.Hash) *ForkChoiceStore {
	cfg := &clparams.MainnetBeaconConfig
	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{Slot: 0},
	}
	verifiedExecutionPayload, _ := lru.New[common.Hash, struct{}](16)
	verifiedExecutionPayload.Add(root, struct{}{})
	blocks := map[common.Hash]*cltypes.SignedBeaconBlock{root: block}
	envelopes := map[common.Hash]bool{root: true}
	fg := ptcVoteForkGraph{
		envelopes:        envelopes,
		blocks:           blocks,
		acceptedPayloads: map[common.Hash]bool{root: true},
	}
	f := &ForkChoiceStore{
		genesisTime: 0,
		beaconCfg:   cfg,
		forkGraph:   fg,

		verifiedExecutionPayload: verifiedExecutionPayload,
	}
	f.time.Store(cfg.SecondsPerSlot)
	return f
}

func newPayloadVoteTestStore(t *testing.T, root common.Hash, hasEnvelope, verified bool) *ForkChoiceStore {
	t.Helper()

	verifiedExecutionPayload, err := lru.New[common.Hash, struct{}](16)
	require.NoError(t, err)
	if verified {
		verifiedExecutionPayload.Add(root, struct{}{})
	}
	executionPayloadStatus, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
	require.NoError(t, err)
	payloadStatusByRoot, err := lru.New[common.Hash, execution_client.PayloadStatus](16)
	require.NoError(t, err)
	eth2Roots, err := lru.New[common.Hash, common.Hash](16)
	require.NoError(t, err)

	acceptedPayloads := map[common.Hash]bool{}
	if verified {
		acceptedPayloads[root] = true
	}
	f := &ForkChoiceStore{
		beaconCfg:                &clparams.MainnetBeaconConfig,
		forkGraph:                payloadVoteForkGraph{hasEnvelope: hasEnvelope, acceptedPayloads: acceptedPayloads},
		eth2Roots:                eth2Roots,
		verifiedExecutionPayload: verifiedExecutionPayload,
		executionPayloadStatus:   executionPayloadStatus,
		payloadStatusByRoot:      payloadStatusByRoot,
		optimisticStore:          optimistic.NewOptimisticStore(),
	}
	f.proposerBoostRoot.Store(common.Hash{})

	majority := int(f.beaconCfg.PtcSize/2) + 1
	f.payloadTimelinessVote.Store(root, ptcVotes(majority, 0))
	f.payloadDataAvailabilityVote.Store(root, ptcVotes(majority, 0))

	return f
}

func ptcVoteThreshold() int {
	return int(clparams.MainnetBeaconConfig.PtcSize / 2)
}

func ptcVotes(trueVotes, falseVotes int) [clparams.PtcSize]int8 {
	var votes [clparams.PtcSize]int8
	for i := range trueVotes {
		votes[i] = boolToVote(true)
	}
	for i := trueVotes; i < trueVotes+falseVotes; i++ {
		votes[i] = boolToVote(false)
	}
	return votes
}

func hasPayloadStatus(nodes []ForkChoiceNode, status cltypes.PayloadStatus) bool {
	for _, node := range nodes {
		if node.PayloadStatus == status {
			return true
		}
	}
	return false
}
