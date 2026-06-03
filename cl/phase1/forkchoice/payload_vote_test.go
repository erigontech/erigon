package forkchoice

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

type ptcVoteForkGraph struct {
	fork_graph.ForkGraph
	envelopes map[common.Hash]bool
}

func (g ptcVoteForkGraph) HasEnvelope(root common.Hash) bool {
	return g.envelopes[root]
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

func newPtcVoteTestStore(root common.Hash) *ForkChoiceStore {
	return &ForkChoiceStore{
		beaconCfg: &clparams.MainnetBeaconConfig,
		forkGraph: ptcVoteForkGraph{
			envelopes: map[common.Hash]bool{root: true},
		},
	}
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
