package forkchoice

import (
	"math"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/stretchr/testify/require"
)

// newTestForkChoiceStore creates a minimal ForkChoiceStore for timing tests.
func newTestForkChoiceStore(secondsPerSlot, intervalsPerSlot, slotsPerEpoch, gloasForkEpoch, genesisTime, currentTime uint64) *ForkChoiceStore {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:   secondsPerSlot,
		IntervalsPerSlot: intervalsPerSlot,
		SlotsPerEpoch:    slotsPerEpoch,
		GloasForkEpoch:   gloasForkEpoch,
		GenesisSlot:      0,
	}
	f := &ForkChoiceStore{
		genesisTime: genesisTime,
		beaconCfg:   cfg,
	}
	f.time.Store(currentTime)
	return f
}

func TestGetAttestationDueMs_PreGloas(t *testing.T) {
	// Pre-GLOAS: 12 * 1000 / 3 = 4000 ms
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 0)
	require.Equal(t, uint64(4000), f.getAttestationDueMs(0))
	require.Equal(t, uint64(4000), f.getAttestationDueMs(100))
}

func TestGetAttestationDueMs_PostGloas(t *testing.T) {
	// Post-GLOAS: 12 * 2500 / 10 = 3000 ms
	f := newTestForkChoiceStore(12, 3, 32, 10, 0, 0)
	require.Equal(t, uint64(3000), f.getAttestationDueMs(10))
	require.Equal(t, uint64(3000), f.getAttestationDueMs(100))
	// Pre-GLOAS epoch still returns legacy value
	require.Equal(t, uint64(4000), f.getAttestationDueMs(9))
}

func TestGetAggregateDueMs_PreGloas(t *testing.T) {
	// Pre-GLOAS: 12 * 1000 * 2 / 3 = 8000 ms
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 0)
	require.Equal(t, uint64(8000), f.getAggregateDueMs(0))
}

func TestGetAggregateDueMs_PostGloas(t *testing.T) {
	// Post-GLOAS: 12 * 5000 / 10 = 6000 ms
	f := newTestForkChoiceStore(12, 3, 32, 10, 0, 0)
	require.Equal(t, uint64(6000), f.getAggregateDueMs(10))
	// Pre-GLOAS epoch
	require.Equal(t, uint64(8000), f.getAggregateDueMs(9))
}

func TestGetSyncMessageDueMs(t *testing.T) {
	f := newTestForkChoiceStore(12, 3, 32, 10, 0, 0)
	// Should match attestation due
	require.Equal(t, f.getAttestationDueMs(5), f.getSyncMessageDueMs(5))
	require.Equal(t, f.getAttestationDueMs(15), f.getSyncMessageDueMs(15))
}

func TestGetContributionDueMs(t *testing.T) {
	f := newTestForkChoiceStore(12, 3, 32, 10, 0, 0)
	// Should match aggregate due
	require.Equal(t, f.getAggregateDueMs(5), f.getContributionDueMs(5))
	require.Equal(t, f.getAggregateDueMs(15), f.getContributionDueMs(15))
}

func TestGetPayloadAttestationDueMs(t *testing.T) {
	// 12 * 7500 / 10 = 9000 ms
	f := newTestForkChoiceStore(12, 3, 32, 10, 0, 0)
	require.Equal(t, uint64(9000), f.getPayloadAttestationDueMs(10))
}

func TestShouldApplyProposerBoost_PreGloas(t *testing.T) {
	// Mainnet config: 12s slots, 3 intervals, GLOAS not activated
	// Threshold: 12/3 = 4 seconds into slot

	tests := []struct {
		name        string
		genesisTime uint64
		currentTime uint64
		want        bool
	}{
		{"at slot start (0s)", 0, 0, true},
		{"1s into slot", 0, 1, true},
		{"3s into slot (just under)", 0, 3, true},
		{"4s into slot (at threshold)", 0, 4, false},
		{"6s into slot", 0, 6, false},
		{"11s into slot", 0, 11, false},
		{"next slot start (12s)", 0, 12, true},
		{"next slot 2s in (14s)", 0, 14, true},
		{"next slot 5s in (17s)", 0, 17, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, tt.genesisTime, tt.currentTime)
			require.Equal(t, tt.want, f.shouldApplyProposerBoost())
		})
	}
}

func TestShouldApplyProposerBoost_PostGloas(t *testing.T) {
	// Post-GLOAS: threshold is 3000ms = 3 seconds
	// epoch 0 with GloasForkEpoch=0, slot size 12, slots per epoch 32
	// So slot 0 is in epoch 0, which is >= GloasForkEpoch

	tests := []struct {
		name        string
		genesisTime uint64
		currentTime uint64
		want        bool
	}{
		{"at slot start (0s)", 0, 0, true},
		{"1s into slot", 0, 1, true},
		{"2s into slot", 0, 2, true},
		{"3s into slot (at threshold)", 0, 3, false}, // 3*1000 = 3000, NOT < 3000
		{"4s into slot", 0, 4, false},
		{"next slot start (12s)", 0, 12, true},
		{"next slot 2s in (14s)", 0, 14, true},
		{"next slot 3s in (15s)", 0, 15, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTestForkChoiceStore(12, 3, 32, 0, tt.genesisTime, tt.currentTime)
			require.Equal(t, tt.want, f.shouldApplyProposerBoost())
		})
	}
}

func TestShouldApplyProposerBoost_ForkTransition(t *testing.T) {
	// GLOAS activates at epoch 10, slots per epoch 32
	// Slot 319 = epoch 9 (pre-GLOAS, threshold 4s)
	// Slot 320 = epoch 10 (post-GLOAS, threshold 3s)

	f := newTestForkChoiceStore(12, 3, 32, 10, 0, 0)

	// Slot 319 (epoch 9), 3.5s into slot: pre-GLOAS threshold is 4s, so timely
	f.time.Store(319*12 + 3) // 3s < 4s threshold (integer seconds)
	require.True(t, f.shouldApplyProposerBoost())

	// Slot 320 (epoch 10), 3s into slot: post-GLOAS threshold is 3000ms, 3*1000=3000 NOT < 3000
	f.time.Store(320*12 + 3)
	require.False(t, f.shouldApplyProposerBoost())

	// Slot 320, 2s into slot: 2*1000=2000 < 3000, timely
	f.time.Store(320*12 + 2)
	require.True(t, f.shouldApplyProposerBoost())
}
