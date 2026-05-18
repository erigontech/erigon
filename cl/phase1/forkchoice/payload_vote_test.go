package forkchoice

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/stretchr/testify/require"
)

func TestReadPTCFromWindow(t *testing.T) {
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

	ptc, ok := readPTCFromWindow(s, slot)
	require.True(t, ok)
	require.Equal(t, []uint64{10, 11, 12, 13}, ptc)

	ptc[0] = 99
	require.Equal(t, uint64(10), ptcWindow.Get(int(windowIndex)).Get(0))
}

func TestReadPTCFromWindowRejectsSlotOutsideWindow(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	s := state2.New(cfg)
	s.SetVersion(clparams.GloasVersion)
	s.SetSlot(2*cfg.SlotsPerEpoch + 5)
	s.SetPtcWindow(solid.NewUint64VectorOfVectors(int(3*cfg.SlotsPerEpoch), 4))

	_, ok := readPTCFromWindow(s, 0)
	require.False(t, ok)
}
