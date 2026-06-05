package forkchoice

import (
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
	envelopes map[common.Hash]bool
	blocks    map[common.Hash]*cltypes.SignedBeaconBlock
}

func (g ptcVoteForkGraph) HasEnvelope(root common.Hash) bool {
	return g.envelopes[root]
}

func (g ptcVoteForkGraph) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	block, ok := g.blocks[root]
	return block, ok
}

type payloadVoteForkGraph struct {
	fork_graph.ForkGraph
	hasEnvelope       bool
	dumpedEnvelope    *common.Hash
	invalidatedHeader *common.Hash
}

func (g payloadVoteForkGraph) HasEnvelope(common.Hash) bool {
	return g.hasEnvelope
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

func TestGetPTCFromWindow(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	s := state2.New(cfg)
	s.SetVersion(clparams.GloasVersion)

	slotsPerEpoch := cfg.SlotsPerEpoch
	slot := 2*slotsPerEpoch + 5
	s.SetSlot(slot)

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
	s.SetSlot(2*cfg.SlotsPerEpoch + 5)
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

	require.True(t, f.ShouldBuildOnFull(head))

	f.payloadDataAvailabilityVote.Store(root, ptcVotes(0, 0))
	require.True(t, f.ShouldBuildOnFull(head))
}

func TestPtcShouldBuildOnFullWithUnavailableMajority(t *testing.T) {
	root := common.HexToHash("0x04")
	f := newPtcVoteTestStore(root)
	f.payloadDataAvailabilityVote.Store(root, ptcVotes(0, ptcVoteThreshold()+1))

	require.False(t, f.ShouldBuildOnFull(ForkChoiceNode{
		Root:          root,
		PayloadStatus: cltypes.PayloadStatusFull,
	}))
	require.False(t, f.ShouldBuildOnFull(ForkChoiceNode{
		Root:          root,
		PayloadStatus: cltypes.PayloadStatusEmpty,
	}))
}

func TestPtcShouldBuildOnFullWithLatePayloadMajority(t *testing.T) {
	root := common.HexToHash("0x07")
	f := newPtcVoteTestStore(root)
	f.payloadTimelinessVote.Store(root, ptcVotes(0, ptcVoteThreshold()+1))

	require.False(t, f.ShouldBuildOnFull(ForkChoiceNode{
		Root:          root,
		PayloadStatus: cltypes.PayloadStatusFull,
	}))
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
	}))
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

func TestGloasForkChoiceRequiresVerifiedPayload(t *testing.T) {
	root := common.HexToHash("0x1234")

	tests := []struct {
		name          string
		hasEnvelope   bool
		verified      bool
		wantFullChild bool
	}{
		{
			name:          "envelope present but not verified means EMPTY only",
			hasEnvelope:   true,
			verified:      false,
			wantFullChild: false,
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

	t.Run("EL-verified and envelope present", func(t *testing.T) {
		f := newPayloadVoteTestStore(t, root, true, true)
		require.True(t, f.IsPayloadVerified(root))
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
	status, ok := f.GetRecentExecutionPayloadStatus(execHash)
	require.True(t, ok)
	require.Equal(t, execution_client.PayloadStatus(execution_client.PayloadStatusInvalidated), status)
	status, ok = f.GetRecentExecutionPayloadStatusByRoot(root)
	require.True(t, ok)
	require.Equal(t, execution_client.PayloadStatus(execution_client.PayloadStatusInvalidated), status)
	require.Equal(t, root, invalidatedHeader)
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
	fg := ptcVoteForkGraph{envelopes: envelopes, blocks: blocks}
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

	f := &ForkChoiceStore{
		beaconCfg:                &clparams.MainnetBeaconConfig,
		forkGraph:                payloadVoteForkGraph{hasEnvelope: hasEnvelope},
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
	for i := 0; i < trueVotes; i++ {
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
