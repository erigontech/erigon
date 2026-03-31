package forkchoice

import (
	"math"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
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
	f.proposerBoostRoot.Store(common.Hash{})
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

// --- recordBlockTimeliness tests ---

func TestRecordBlockTimeliness_PreGloas_Timely(t *testing.T) {
	// Pre-GLOAS: block arrives on time (0s into slot)
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 0)

	block := &cltypes.BeaconBlock{Slot: 0}
	blockRoot := common.Hash{0x01}

	f.recordBlockTimeliness(block, blockRoot)

	timeliness, ok := f.getBlockTimeliness(blockRoot)
	require.True(t, ok)
	require.True(t, timeliness[clparams.AttestationTimelinessIndex], "block should be timely")
	require.False(t, timeliness[clparams.PtcTimelinessIndex], "pre-GLOAS PTC timeliness is always false")
}

func TestRecordBlockTimeliness_PreGloas_Late(t *testing.T) {
	// Pre-GLOAS: block arrives late (5s into slot, threshold 4s)
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 5)

	block := &cltypes.BeaconBlock{Slot: 0}
	blockRoot := common.Hash{0x02}

	f.recordBlockTimeliness(block, blockRoot)

	timeliness, ok := f.getBlockTimeliness(blockRoot)
	require.True(t, ok)
	require.False(t, timeliness[clparams.AttestationTimelinessIndex], "block should be late")
}

func TestRecordBlockTimeliness_PostGloas_TwoElementVector(t *testing.T) {
	// Post-GLOAS: block arrives at 1s into slot
	// Attestation threshold: 3000ms, PTC threshold: 9000ms
	// 1s = 1000ms < both thresholds → both timely
	f := newTestForkChoiceStore(12, 3, 32, 0, 0, 1)

	block := &cltypes.BeaconBlock{Slot: 0}
	blockRoot := common.Hash{0x03}

	f.recordBlockTimeliness(block, blockRoot)

	timeliness, ok := f.getBlockTimeliness(blockRoot)
	require.True(t, ok)
	require.True(t, timeliness[clparams.AttestationTimelinessIndex], "block should be timely (1000ms < 3000ms)")
	// PTC_TIMELINESS_INDEX is now a time-based check: did block arrive before PTC deadline?
	// 1s = 1000ms < 9000ms (PTC threshold) → true
	require.True(t, timeliness[clparams.PtcTimelinessIndex], "block arrived before PTC deadline (1000ms < 9000ms)")
}

func TestRecordBlockTimeliness_PostGloas_AfterPtcDeadline(t *testing.T) {
	// Post-GLOAS: block arrives at 10s into slot
	// Attestation threshold: 3000ms, PTC threshold: 9000ms
	// 10s = 10000ms > both thresholds → both false
	f := newTestForkChoiceStore(12, 3, 32, 0, 0, 10)

	block := &cltypes.BeaconBlock{Slot: 0}
	blockRoot := common.Hash{0x04}

	f.recordBlockTimeliness(block, blockRoot)

	timeliness, ok := f.getBlockTimeliness(blockRoot)
	require.True(t, ok)
	require.False(t, timeliness[clparams.AttestationTimelinessIndex], "block should be late (10000ms >= 3000ms)")
	require.False(t, timeliness[clparams.PtcTimelinessIndex], "block after PTC deadline (10000ms >= 9000ms)")
}

func TestRecordBlockTimeliness_PostGloas_BetweenDeadlines(t *testing.T) {
	// Post-GLOAS: block arrives at 5s into slot
	// Attestation threshold: 3000ms → late
	// PTC threshold: 9000ms → timely
	f := newTestForkChoiceStore(12, 3, 32, 0, 0, 5)

	block := &cltypes.BeaconBlock{Slot: 0}
	blockRoot := common.Hash{0x05}

	f.recordBlockTimeliness(block, blockRoot)

	timeliness, ok := f.getBlockTimeliness(blockRoot)
	require.True(t, ok)
	require.False(t, timeliness[clparams.AttestationTimelinessIndex], "block late for attestation (5000ms >= 3000ms)")
	require.True(t, timeliness[clparams.PtcTimelinessIndex], "block timely for PTC (5000ms < 9000ms)")
}

func TestRecordBlockTimeliness_WrongSlot(t *testing.T) {
	// Block slot doesn't match current slot — should not record
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 24) // time=24 → slot 2

	block := &cltypes.BeaconBlock{Slot: 0} // block for slot 0
	blockRoot := common.Hash{0x04}

	f.recordBlockTimeliness(block, blockRoot)

	_, ok := f.getBlockTimeliness(blockRoot)
	require.False(t, ok, "should not record timeliness for wrong slot")
}

func TestUpdateProposerBoostRoot_TimelyBlock(t *testing.T) {
	// Timely block (0s into slot) should get boost if no lookahead (pre-GLOAS fallback)
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 0)

	block := &cltypes.BeaconBlock{Slot: 0}
	blockRoot := common.Hash{0x10}

	f.recordBlockTimeliness(block, blockRoot)
	f.updateProposerBoostRoot(block, blockRoot)
	require.Equal(t, blockRoot, f.proposerBoostRoot.Load().(common.Hash))
}

func TestUpdateProposerBoostRoot_LateBlock(t *testing.T) {
	// Late block (5s into slot, threshold 4s) should NOT get boost
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 5)

	block := &cltypes.BeaconBlock{Slot: 0}
	blockRoot := common.Hash{0x11}

	f.recordBlockTimeliness(block, blockRoot)
	f.updateProposerBoostRoot(block, blockRoot)
	require.Equal(t, common.Hash{}, f.proposerBoostRoot.Load().(common.Hash))
}

func TestUpdateProposerBoostRoot_NotOverwritten(t *testing.T) {
	// If proposer boost root is already set, a second timely block should not overwrite it
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 0)

	firstRoot := common.Hash{0x10}
	secondRoot := common.Hash{0x20}

	block1 := &cltypes.BeaconBlock{Slot: 0}
	block2 := &cltypes.BeaconBlock{Slot: 0}

	f.recordBlockTimeliness(block1, firstRoot)
	f.updateProposerBoostRoot(block1, firstRoot)
	require.Equal(t, firstRoot, f.proposerBoostRoot.Load().(common.Hash))

	f.recordBlockTimeliness(block2, secondRoot)
	f.updateProposerBoostRoot(block2, secondRoot)
	require.Equal(t, firstRoot, f.proposerBoostRoot.Load().(common.Hash), "boost root should not be overwritten")

	// But the second block's timeliness should still be recorded
	timeliness, ok := f.getBlockTimeliness(secondRoot)
	require.True(t, ok)
	require.True(t, timeliness[clparams.AttestationTimelinessIndex])
}

// --- isHeadLate tests ---

func TestIsHeadLate_Timely(t *testing.T) {
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 0)
	root := common.Hash{0x01}
	block := &cltypes.BeaconBlock{Slot: 0}
	f.recordBlockTimeliness(block, root)
	require.False(t, f.isHeadLate(root), "timely block should not be late")
}

func TestIsHeadLate_Late(t *testing.T) {
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 5)
	root := common.Hash{0x02}
	block := &cltypes.BeaconBlock{Slot: 0}
	f.recordBlockTimeliness(block, root)
	require.True(t, f.isHeadLate(root), "late block should be late")
}

func TestIsHeadLate_Unknown(t *testing.T) {
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 0)
	require.True(t, f.isHeadLate(common.Hash{0xFF}), "unknown block should be treated as late")
}

// --- isHeadWeak tests ---

func TestIsHeadWeak_NoCheckpointState(t *testing.T) {
	// Without a checkpoint state, isHeadWeak returns false (safe default)
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 0)
	f.justifiedCheckpoint.Store(solid.Checkpoint{})
	require.False(t, f.isHeadWeak(common.Hash{0x01}))
}
