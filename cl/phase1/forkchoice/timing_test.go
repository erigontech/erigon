package forkchoice

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/caches"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestForkChoiceStore creates a minimal ForkChoiceStore for timing tests.
func newTestForkChoiceStore(secondsPerSlot, intervalsPerSlot, slotsPerEpoch, gloasForkEpoch, genesisTime, currentTimeMs uint64) *ForkChoiceStore {
	cfg := &clparams.BeaconChainConfig{
		SecondsPerSlot:           secondsPerSlot,
		IntervalsPerSlot:         intervalsPerSlot,
		SlotsPerEpoch:            slotsPerEpoch,
		GloasForkEpoch:           gloasForkEpoch,
		GenesisSlot:              0,
		AttestationDueBps:        clparams.AttestationDueBps,
		AggregateDueBps:          clparams.AggregateDueBps,
		SyncMessageDueBps:        clparams.SyncMessageDueBps,
		ContributionDueBps:       clparams.ContributionDueBps,
		AttestationDueBpsGloas:   clparams.AttestationDueBpsGloas,
		AggregateDueBpsGloas:     clparams.AggregateDueBpsGloas,
		SyncMessageDueBpsGloas:   clparams.SyncMessageDueBpsGloas,
		ContributionDueBpsGloas:  clparams.ContributionDueBpsGloas,
		PayloadAttestationDueBps: clparams.PayloadAttestationDueBps,
		ReorgHeadWeightThreshold: clparams.ReorgHeadWeightThreshold,
	}
	f := &ForkChoiceStore{
		genesisTime: genesisTime,
		beaconCfg:   cfg,
	}
	f.timeMs.Store(currentTimeMs)
	f.proposerBoostRoot.Store(common.Hash{})
	return f
}

func TestGetAttestationDueMs_PreGloas(t *testing.T) {
	// Pre-GLOAS: 12 * 3333 / 10 = 3999 ms
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 0)
	require.Equal(t, uint64(3999), f.getAttestationDueMs(0))
	require.Equal(t, uint64(3999), f.getAttestationDueMs(100))
}

func TestGetAttestationDueMs_PostGloas(t *testing.T) {
	// Post-GLOAS: 12 * 2500 / 10 = 3000 ms
	f := newTestForkChoiceStore(12, 3, 32, 10, 0, 0)
	require.Equal(t, uint64(3000), f.getAttestationDueMs(10))
	require.Equal(t, uint64(3000), f.getAttestationDueMs(100))
	// Pre-GLOAS epoch keeps the pre-GLOAS deadline
	require.Equal(t, uint64(3999), f.getAttestationDueMs(9))
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
	require.Equal(t, f.getAttestationDueMs(5), f.getSyncMessageDueMs(5))
	require.Equal(t, f.getAttestationDueMs(15), f.getSyncMessageDueMs(15))
}

func TestGetContributionDueMs(t *testing.T) {
	f := newTestForkChoiceStore(12, 3, 32, 10, 0, 0)
	require.Equal(t, f.getAggregateDueMs(5), f.getContributionDueMs(5))
	require.Equal(t, f.getAggregateDueMs(15), f.getContributionDueMs(15))
}

func TestGetPayloadAttestationDueMs(t *testing.T) {
	// 12 * 7500 / 10 = 9000 ms
	f := newTestForkChoiceStore(12, 3, 32, 10, 0, 0)
	require.Equal(t, uint64(9000), f.getPayloadAttestationDueMs())
}

func TestGetTimingDueMsUsesConfiguredBps(t *testing.T) {
	f := newTestForkChoiceStore(12, 3, 32, 10, 0, 0)
	f.beaconCfg.AttestationDueBps = 2500
	f.beaconCfg.AttestationDueBpsGloas = 1250
	f.beaconCfg.AggregateDueBps = 2000
	f.beaconCfg.AggregateDueBpsGloas = 4000
	f.beaconCfg.SyncMessageDueBps = 3000
	f.beaconCfg.SyncMessageDueBpsGloas = 1500
	f.beaconCfg.ContributionDueBps = 4000
	f.beaconCfg.ContributionDueBpsGloas = 2500
	f.beaconCfg.PayloadAttestationDueBps = 6000

	require.Equal(t, uint64(3000), f.getAttestationDueMs(0))
	require.Equal(t, uint64(1500), f.getAttestationDueMs(10))
	require.Equal(t, uint64(2400), f.getAggregateDueMs(0))
	require.Equal(t, uint64(4800), f.getAggregateDueMs(10))
	require.Equal(t, uint64(3600), f.getSyncMessageDueMs(0))
	require.Equal(t, uint64(1800), f.getSyncMessageDueMs(10))
	require.Equal(t, uint64(4800), f.getContributionDueMs(0))
	require.Equal(t, uint64(3000), f.getContributionDueMs(10))
	require.Equal(t, uint64(7200), f.getPayloadAttestationDueMs())
}

func TestComputeShufflingDependentSlotUsesGenesisInEarlyEpochs(t *testing.T) {
	require.Zero(t, computeShufflingDependentSlot(0, 1, 32))
	require.Zero(t, computeShufflingDependentSlot(1, 1, 32))
	require.Equal(t, uint64(31), computeShufflingDependentSlot(2, 1, 32))
}

// --- recordBlockTimeliness tests ---

func TestRecordBlockTimelinessUsesMilliseconds(t *testing.T) {
	t.Run("pre-Gloas", func(t *testing.T) {
		f := newTestForkChoiceStore(5, 3, 32, math.MaxUint64, 0, 1665)
		block := &cltypes.BeaconBlock{Slot: 0}

		f.recordBlockTimeliness(block, common.Hash{0x01}, f.timeMs.Load())
		timeliness, ok := f.getBlockTimeliness(common.Hash{0x01})
		require.True(t, ok)
		require.True(t, timeliness[clparams.AttestationTimelinessIndex])

		f.timeMs.Store(1700)
		f.recordBlockTimeliness(block, common.Hash{0x02}, f.timeMs.Load())
		timeliness, ok = f.getBlockTimeliness(common.Hash{0x02})
		require.True(t, ok)
		require.False(t, timeliness[clparams.AttestationTimelinessIndex])
	})

	t.Run("Gloas", func(t *testing.T) {
		f := newTestForkChoiceStore(5, 3, 32, 0, 0, 3800)
		blockRoot := common.Hash{0x03}
		f.recordBlockTimeliness(&cltypes.BeaconBlock{Slot: 0}, blockRoot, f.timeMs.Load())

		timeliness, ok := f.getBlockTimeliness(blockRoot)
		require.True(t, ok)
		require.False(t, timeliness[clparams.AttestationTimelinessIndex])
		require.False(t, timeliness[clparams.PtcTimelinessIndex])
	})
}

func TestRecordBlockTimelinessDeadlines(t *testing.T) {
	for _, test := range []struct {
		name            string
		secondsPerSlot  uint64
		gloasForkEpoch  uint64
		genesisTime     uint64
		slot            uint64
		timelinessIndex int
		timelyAtMs      uint64
		lateAtMs        uint64
	}{
		{name: "12s pre-Gloas attestation", secondsPerSlot: 12, gloasForkEpoch: math.MaxUint64, timelinessIndex: clparams.AttestationTimelinessIndex, timelyAtMs: 3998, lateAtMs: 3999},
		{name: "12s pre-Gloas attestation non-zero genesis", secondsPerSlot: 12, gloasForkEpoch: math.MaxUint64, genesisTime: 1606824023, slot: 1, timelinessIndex: clparams.AttestationTimelinessIndex, timelyAtMs: 3998, lateAtMs: 3999},
		{name: "12s Gloas attestation", secondsPerSlot: 12, gloasForkEpoch: 0, timelinessIndex: clparams.AttestationTimelinessIndex, timelyAtMs: 2999, lateAtMs: 3000},
		{name: "12s Gloas PTC", secondsPerSlot: 12, gloasForkEpoch: 0, timelinessIndex: clparams.PtcTimelinessIndex, timelyAtMs: 8999, lateAtMs: 9000},
		{name: "5s Gloas PTC", secondsPerSlot: 5, gloasForkEpoch: 0, timelinessIndex: clparams.PtcTimelinessIndex, timelyAtMs: 3749, lateAtMs: 3750},
		{name: "5s pre-Gloas attestation", secondsPerSlot: 5, gloasForkEpoch: math.MaxUint64, timelinessIndex: clparams.AttestationTimelinessIndex, timelyAtMs: 1665, lateAtMs: 1666},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, check := range []struct {
				name       string
				timeMs     uint64
				wantTimely bool
			}{
				{name: "timely", timeMs: test.timelyAtMs, wantTimely: true},
				{name: "late", timeMs: test.lateAtMs, wantTimely: false},
			} {
				t.Run(check.name, func(t *testing.T) {
					timeMs := test.genesisTime*1000 + test.slot*test.secondsPerSlot*1000 + check.timeMs
					f := newTestForkChoiceStore(test.secondsPerSlot, 3, 32, test.gloasForkEpoch, test.genesisTime, timeMs)
					blockRoot := common.Hash{byte(test.timelinessIndex), byte(check.timeMs), byte(check.timeMs >> 8)}

					f.recordBlockTimeliness(&cltypes.BeaconBlock{Slot: test.slot}, blockRoot, f.timeMs.Load())

					timeliness, ok := f.getBlockTimeliness(blockRoot)
					require.True(t, ok)
					require.Equal(t, check.wantTimely, timeliness[test.timelinessIndex])
				})
			}
		})
	}
}

func TestSlotStartTickDoesNotMakeLateLocalBlockTimely(t *testing.T) {
	f, block, _, _ := buildExAnteStoreBeforeBlocks(t, nil, nil)
	slotStartMs := (f.genesisTime + block.Block.Slot*f.beaconCfg.SecondsPerSlot) * 1000
	f.OnTick(slotStartMs + 5000)
	f.OnTick(slotStartMs)
	require.NoError(t, f.OnBlock(t.Context(), block, true, true, false))

	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	timeliness, ok := f.getBlockTimeliness(blockRoot)
	require.True(t, ok)
	assert.False(t, timeliness[clparams.AttestationTimelinessIndex])
	assert.Zero(t, f.ProposerBoostRoot())
}

func slotTimeMs(f *ForkChoiceStore, slot uint64) uint64 {
	return (f.genesisTime + slot*f.beaconCfg.SecondsPerSlot) * 1000
}

// replaySlotStartWork runs the slot-start work of a tick into the slot of timeMs that advanced the
// store time before the latest tick, but takes f.mu only now.
func replaySlotStartWork(f *ForkChoiceStore, timeMs uint64) {
	now := f.timeMs.Load()
	f.timeMs.Store(slotTimeMs(f, f.slotAtTimeMs(timeMs)) - 1)
	f.onTickPerSlot(timeMs)
	f.timeMs.Store(now)
}

// onBlockWaitingForLock calls OnBlock while f.mu is held and runs whileWaiting once OnBlock waits for it.
func onBlockWaitingForLock(t *testing.T, f *ForkChoiceStore, block *cltypes.SignedBeaconBlock, whileWaiting func()) {
	t.Helper()
	f.mu.Lock()
	onBlockDone := make(chan error, 1)
	go func() { onBlockDone <- f.OnBlock(context.Background(), block, false, true, false) }()
	require.Eventually(t, func() bool { return f.blocksProcessing.Load() == 1 }, lockScopeTimeout, time.Millisecond)
	whileWaiting()
	f.mu.Unlock()
	require.NoError(t, <-onBlockDone)
}

// importParent imports the parent of the ex-ante fixture block at the start of its slot, so the
// parent holds the proposer boost.
func importParent(t *testing.T, engine execution_client.ExecutionEngine) (*ForkChoiceStore, *cltypes.SignedBeaconBlock) {
	t.Helper()
	f, parent, _, block := buildExAnteStoreBeforeBlocks(t, engine, nil)
	f.OnTick(slotTimeMs(f, parent.Block.Slot))
	require.NoError(t, f.OnBlock(t.Context(), parent, false, true, false))
	return f, block
}

// importBlockWhileTimePasses imports a block of the slot after its parent, entering OnBlock
// enterMs into the slot, and runs duringEL while the EL validates the payload.
func importBlockWhileTimePasses(t *testing.T, enterMs uint64, duringEL func(f *ForkChoiceStore, slotStartMs uint64)) (*ForkChoiceStore, common.Hash) {
	t.Helper()
	engine, elEntered, releaseEL := blockingEngine(t, 1, execution_client.PayloadStatusValidated, nil)
	f, block := importParent(t, engine)
	slotStartMs := slotTimeMs(f, block.Block.Slot)
	f.OnTick(slotStartMs + enterMs)
	onBlockDone := make(chan error, 1)
	go func() { onBlockDone <- f.OnBlock(context.Background(), block, true, true, false) }()
	awaitSignal(t, elEntered, "NewPayload to start")
	duringEL(f, slotStartMs)
	close(releaseEL)
	require.NoError(t, <-onBlockDone)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	return f, root
}

// on_block runs atomically at the store time it is called with, so the time spent validating
// the block must not make it late.
func TestBlockTimelinessUsesTimeWhenImportStarts(t *testing.T) {
	f, root := importBlockWhileTimePasses(t, 3000, func(f *ForkChoiceStore, slotStartMs uint64) {
		f.OnTick(slotStartMs + 4500)
	})

	timeliness, ok := f.getBlockTimeliness(root)
	require.True(t, ok)
	assert.True(t, timeliness[clparams.AttestationTimelinessIndex])
	assert.Equal(t, root, f.ProposerBoostRoot())
}

// The slot tick resets the proposer boost, so a block whose slot ends while it is imported must
// not set the boost after that reset.
func TestBlockImportedAcrossSlotTickGetsNoBoost(t *testing.T) {
	f, root := importBlockWhileTimePasses(t, 3000, func(f *ForkChoiceStore, slotStartMs uint64) {
		f.OnTick(slotStartMs + 12000 + 100)
	})

	timeliness, ok := f.getBlockTimeliness(root)
	require.True(t, ok)
	assert.True(t, timeliness[clparams.AttestationTimelinessIndex])
	assert.Zero(t, f.ProposerBoostRoot())
}

// Two ticks can advance the store time to a slot and to the next one, then run their slot-start
// work in the reverse order. The earlier reset finishing last must not let a block of the earlier
// slot set the boost in the later slot.
func TestReversedSlotStartResetsKeepBoostExpired(t *testing.T) {
	f, _ := importBlockWhileTimePasses(t, 3000, func(f *ForkChoiceStore, slotStartMs uint64) {
		f.OnTick(slotStartMs + 12000 + 100)
		replaySlotStartWork(f, slotStartMs+100)
	})

	assert.Zero(t, f.ProposerBoostRoot())
}

// The slot-start work of a tick into an earlier slot can take f.mu after a timely block of the
// next slot got the boost. It must not remove that boost.
func TestStaleSlotStartKeepsBoostOfNewerSlot(t *testing.T) {
	f, block := importParent(t, nil)
	f.OnTick(slotTimeMs(f, block.Block.Slot) + 100)
	require.NoError(t, f.OnBlock(t.Context(), block, false, true, false))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), f.ProposerBoostRoot())

	replaySlotStartWork(f, slotTimeMs(f, block.Block.Slot-1)+100)

	assert.Equal(t, common.Hash(root), f.ProposerBoostRoot())
}

// A tick advances the store time into a slot before it takes f.mu for the slot-start work. A
// timely block of the slot applied in between must get the boost, and that work must keep it.
func TestBlockAppliedBeforePendingSlotStartGetsBoost(t *testing.T) {
	f, block := importParent(t, nil)
	require.NotZero(t, f.ProposerBoostRoot(), "the parent must hold the boost of its slot")
	slotStartMs := slotTimeMs(f, block.Block.Slot)
	f.timeMs.Store(slotStartMs + 100)
	require.NoError(t, f.OnBlock(t.Context(), block, false, true, false))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, common.Hash(root), f.ProposerBoostRoot())

	replaySlotStartWork(f, slotStartMs+100)

	assert.Equal(t, common.Hash(root), f.ProposerBoostRoot())
}

// A block called before its slot is admitted only once the store reaches the slot, so it is
// processed at the store time of its admission.
func TestBlockCalledBeforeItsSlotUsesAdmissionTime(t *testing.T) {
	f, block := importParent(t, nil)
	slotStartMs := slotTimeMs(f, block.Block.Slot)
	f.OnTick(slotStartMs - 50)
	onBlockWaitingForLock(t, f, block, func() { f.timeMs.Store(slotStartMs + 100) })

	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	timeliness, ok := f.getBlockTimeliness(common.Hash(root))
	require.True(t, ok)
	assert.True(t, timeliness[clparams.AttestationTimelinessIndex])
}

// A block called in its slot keeps the store time of the call while it waits for f.mu.
func TestBlockWaitingForLockUsesCallTime(t *testing.T) {
	f, block := importParent(t, nil)
	slotStartMs := slotTimeMs(f, block.Block.Slot)
	f.OnTick(slotStartMs + 3000)
	onBlockWaitingForLock(t, f, block, func() { f.timeMs.Store(slotStartMs + 4500) })

	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	timeliness, ok := f.getBlockTimeliness(common.Hash(root))
	require.True(t, ok)
	assert.True(t, timeliness[clparams.AttestationTimelinessIndex])
}

// A Gloas block called in its slot whose import takes f.mu after the next slot's tick advanced the
// time must end without the boost, as on_block followed by that tick does.
func TestGloasBlockLockedAfterNextSlotTickEndsWithoutBoost(t *testing.T) {
	importUpToLast := func(t *testing.T) (*ForkChoiceStore, *cltypes.SignedBeaconBlock) {
		fixture := tests.GetGloasFixture("attestation", false, 0)
		f := newBlockTestStore(t, fixture.PreState)
		last := len(fixture.Blocks) - 1
		for _, block := range fixture.Blocks[:last] {
			f.OnTick(slotTimeMs(f, block.Block.Slot) + 100)
			require.NoError(t, f.OnBlock(t.Context(), block, false, true, false))
		}
		block := fixture.Blocks[last]
		f.OnTick(slotTimeMs(f, block.Block.Slot) + 100)
		return f, block
	}

	serial, serialBlock := importUpToLast(t)
	require.NoError(t, serial.OnBlock(t.Context(), serialBlock, false, true, false))
	root, err := serialBlock.Block.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), serial.ProposerBoostRoot(), "the fixture block must get the boost in its slot")
	nextSlotMs := slotTimeMs(serial, serialBlock.Block.Slot+1) + 100
	serial.OnTick(nextSlotMs)
	require.Zero(t, serial.ProposerBoostRoot())

	f, block := importUpToLast(t)
	onBlockWaitingForLock(t, f, block, func() { f.timeMs.Store(nextSlotMs) })
	timeliness, ok := f.getBlockTimeliness(common.Hash(root))
	require.True(t, ok)
	assert.True(t, timeliness[clparams.AttestationTimelinessIndex])
	replaySlotStartWork(f, nextSlotMs)

	assert.Zero(t, f.ProposerBoostRoot())
}

// Spec test fixtures can move the store time back to an earlier slot, whose blocks must still
// get the proposer boost.
func TestSpecTestTimeMovedBackKeepsProposerBoost(t *testing.T) {
	f, block := importParent(t, nil)
	f.OnTick(slotTimeMs(f, block.Block.Slot+3))
	f.SetTimeForSpecTest(slotTimeMs(f, block.Block.Slot))
	require.NoError(t, f.OnBlock(t.Context(), block, false, true, false))

	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, common.Hash(root), f.ProposerBoostRoot())
}

func TestRecordBlockTimeliness_PreGloas_Timely(t *testing.T) {
	// Pre-GLOAS: block arrives on time (0s into slot)
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 0)

	block := &cltypes.BeaconBlock{Slot: 0}
	blockRoot := common.Hash{0x01}

	f.recordBlockTimeliness(block, blockRoot, f.timeMs.Load())

	timeliness, ok := f.getBlockTimeliness(blockRoot)
	require.True(t, ok)
	require.True(t, timeliness[clparams.AttestationTimelinessIndex], "block should be timely")
	require.False(t, timeliness[clparams.PtcTimelinessIndex], "pre-GLOAS PTC timeliness is always false")
}

func TestRecordBlockTimeliness_PreGloas_Late(t *testing.T) {
	// Pre-GLOAS: block arrives late (5s into slot, threshold 3999ms)
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 5*1000)

	block := &cltypes.BeaconBlock{Slot: 0}
	blockRoot := common.Hash{0x02}

	f.recordBlockTimeliness(block, blockRoot, f.timeMs.Load())

	timeliness, ok := f.getBlockTimeliness(blockRoot)
	require.True(t, ok)
	require.False(t, timeliness[clparams.AttestationTimelinessIndex], "block should be late")
}

func TestRecordBlockTimeliness_PostGloas_TwoElementVector(t *testing.T) {
	// Post-GLOAS: block arrives at 1s into slot
	// Attestation threshold: 3000ms, PTC threshold: 9000ms
	// 1s = 1000ms < both thresholds → both timely
	f := newTestForkChoiceStore(12, 3, 32, 0, 0, 1*1000)

	block := &cltypes.BeaconBlock{Slot: 0}
	blockRoot := common.Hash{0x03}

	f.recordBlockTimeliness(block, blockRoot, f.timeMs.Load())

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
	f := newTestForkChoiceStore(12, 3, 32, 0, 0, 10*1000)

	block := &cltypes.BeaconBlock{Slot: 0}
	blockRoot := common.Hash{0x04}

	f.recordBlockTimeliness(block, blockRoot, f.timeMs.Load())

	timeliness, ok := f.getBlockTimeliness(blockRoot)
	require.True(t, ok)
	require.False(t, timeliness[clparams.AttestationTimelinessIndex], "block should be late (10000ms >= 3000ms)")
	require.False(t, timeliness[clparams.PtcTimelinessIndex], "block after PTC deadline (10000ms >= 9000ms)")
}

func TestRecordBlockTimeliness_PostGloas_BetweenDeadlines(t *testing.T) {
	// Post-GLOAS: block arrives at 5s into slot
	// Attestation threshold: 3000ms → late
	// PTC threshold: 9000ms → timely
	f := newTestForkChoiceStore(12, 3, 32, 0, 0, 5*1000)

	block := &cltypes.BeaconBlock{Slot: 0}
	blockRoot := common.Hash{0x05}

	f.recordBlockTimeliness(block, blockRoot, f.timeMs.Load())

	timeliness, ok := f.getBlockTimeliness(blockRoot)
	require.True(t, ok)
	require.False(t, timeliness[clparams.AttestationTimelinessIndex], "block late for attestation (5000ms >= 3000ms)")
	require.True(t, timeliness[clparams.PtcTimelinessIndex], "block timely for PTC (5000ms < 9000ms)")
}

func TestRecordBlockTimeliness_LateKnownBlockRecordsFalseVector(t *testing.T) {
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 24*1000) // time=24 → slot 2

	block := &cltypes.BeaconBlock{Slot: 0} // block for slot 0
	blockRoot := common.Hash{0x04}

	f.recordBlockTimeliness(block, blockRoot, f.timeMs.Load())

	timeliness, ok := f.getBlockTimeliness(blockRoot)
	require.True(t, ok)
	require.False(t, timeliness[clparams.AttestationTimelinessIndex])
	require.False(t, timeliness[clparams.PtcTimelinessIndex])
}

func TestUpdateProposerBoostRoot_TimelyBlock(t *testing.T) {
	// Timely block (0s into slot) should get boost if no lookahead (pre-GLOAS fallback)
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 0)

	block := &cltypes.BeaconBlock{Slot: 0}
	blockRoot := common.Hash{0x10}

	f.recordBlockTimeliness(block, blockRoot, f.timeMs.Load())
	f.updateProposerBoostRoot(common.Hash{}, blockRoot, f.Slot())
	require.Equal(t, blockRoot, f.proposerBoostRoot.Load().(common.Hash))
}

func TestUpdateProposerBoostRoot_LateBlock(t *testing.T) {
	// Late block (5s into slot, threshold 3999ms) should NOT get boost
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 5*1000)

	block := &cltypes.BeaconBlock{Slot: 0}
	blockRoot := common.Hash{0x11}

	f.recordBlockTimeliness(block, blockRoot, f.timeMs.Load())
	f.updateProposerBoostRoot(common.Hash{}, blockRoot, f.Slot())
	require.Equal(t, common.Hash{}, f.proposerBoostRoot.Load().(common.Hash))
}

func TestUpdateProposerBoostRoot_NotOverwritten(t *testing.T) {
	// If proposer boost root is already set, a second timely block should not overwrite it
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 0)

	firstRoot := common.Hash{0x10}
	secondRoot := common.Hash{0x20}

	block1 := &cltypes.BeaconBlock{Slot: 0}
	block2 := &cltypes.BeaconBlock{Slot: 0}

	f.recordBlockTimeliness(block1, firstRoot, f.timeMs.Load())
	f.updateProposerBoostRoot(common.Hash{}, firstRoot, f.Slot())
	require.Equal(t, firstRoot, f.proposerBoostRoot.Load().(common.Hash))

	f.recordBlockTimeliness(block2, secondRoot, f.timeMs.Load())
	f.updateProposerBoostRoot(common.Hash{}, secondRoot, f.Slot())
	require.Equal(t, firstRoot, f.proposerBoostRoot.Load().(common.Hash), "boost root should not be overwritten")

	// But the second block's timeliness should still be recorded
	timeliness, ok := f.getBlockTimeliness(secondRoot)
	require.True(t, ok)
	require.True(t, timeliness[clparams.AttestationTimelinessIndex])
}

// --- isHeadWeak tests ---

func TestIsHeadWeak_NoCheckpointState(t *testing.T) {
	// Without a checkpoint state, isHeadWeak returns false (safe default)
	f := newTestForkChoiceStore(12, 3, 32, math.MaxUint64, 0, 0)
	f.justifiedCheckpoint.Store(solid.Checkpoint{})
	require.False(t, f.isHeadWeak(common.Hash{0x01}))
}

func newHeadWeakCommitteeStore(t *testing.T, cacheHeadEpochState bool) (*ForkChoiceStore, common.Hash, *checkpointState) {
	t.Helper()
	f, graph := newHeadSnapshotTestStore()
	cfg := f.beaconCfg
	genesisRoot := f.JustifiedCheckpoint().Root
	headRoot := common.Hash{0xbb}
	headBlock := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	headBlock.Block.Slot = cfg.SlotsPerEpoch + 8
	headBlock.Block.ParentRoot = genesisRoot
	graph.headers[headRoot] = headBlock.SignedBeaconBlockHeader().Header
	graph.blocks[headRoot] = headBlock

	const validators, balance = 64, 1000
	newState := func(shuffled []uint64) *checkpointState {
		cs := &checkpointState{
			beaconConfig:           cfg,
			shuffledSet:            shuffled,
			balances:               make([]uint64, validators),
			actives:                []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			validatorSetSize:       validators,
			unslashedActiveBalance: validators * balance,
		}
		for i := range cs.balances {
			cs.balances[i] = balance
		}
		return cs
	}
	justifiedSet := make([]uint64, validators)
	headEpochSet := make([]uint64, validators)
	for i := range justifiedSet {
		justifiedSet[i] = uint64(i)
		headEpochSet[i] = validators - 1 - uint64(i)
	}
	justifiedState := newState(justifiedSet)
	f.checkpointStates.Store(f.JustifiedCheckpoint(), justifiedState)
	if cacheHeadEpochState {
		f.checkpointStates.Store(solid.Checkpoint{Epoch: 1, Root: genesisRoot}, newState(headEpochSet))
	}
	return f, headRoot, justifiedState
}

var noAttestations = func(common.Hash) uint64 { return 0 }

// Slot 8 of the epoch holds validators {16, 17} in the justified shuffling
// and {47, 46} in the head epoch shuffling.
func TestIsHeadWeakAddsEquivocatorsFromHeadStateCommittee(t *testing.T) {
	f, headRoot, justifiedState := newHeadWeakCommitteeStore(t, true)
	f.setUnequivocating(47)
	require.False(t, f.isHeadWeakWith(headRoot, justifiedState, noAttestations))
}

func TestIsHeadWeakIgnoresEquivocatorsOutsideHeadStateCommittee(t *testing.T) {
	f, headRoot, justifiedState := newHeadWeakCommitteeStore(t, true)
	f.setUnequivocating(16)
	require.True(t, f.isHeadWeakWith(headRoot, justifiedState, noAttestations))
}

func TestIsHeadWeakCountsEquivocatorInactiveInJustifiedState(t *testing.T) {
	f, headRoot, justifiedState := newHeadWeakCommitteeStore(t, true)
	writeToBitset(justifiedState.actives, 47, false)
	f.setUnequivocating(47)
	require.False(t, f.isHeadWeakWith(headRoot, justifiedState, noAttestations))
}

type noAncestorWalkForkGraph struct{ *headSnapshotForkGraph }

func (noAncestorWalkForkGraph) GetHeader(common.Hash) (*cltypes.BeaconBlockHeader, bool) {
	panic("unexpected ancestor walk")
}

func TestIsHeadWeakWithoutEquivocatorsSkipsCommitteeLookup(t *testing.T) {
	f, headRoot, justifiedState := newHeadWeakCommitteeStore(t, false)
	f.forkGraph = noAncestorWalkForkGraph{f.forkGraph.(*headSnapshotForkGraph)}
	require.True(t, f.isHeadWeakWith(headRoot, justifiedState, noAttestations))
}

func TestIsHeadWeakUsesConfiguredThreshold(t *testing.T) {
	f, headRoot, justifiedState := newHeadWeakCommitteeStore(t, false)
	config := *f.beaconCfg
	config.ReorgHeadWeightThreshold = 1
	f.beaconCfg = &config
	require.False(t, f.isHeadWeakWith(headRoot, justifiedState, func(common.Hash) uint64 { return 100 }))
}

func TestIsHeadWeakUsesHeadEpochShufflingCaches(t *testing.T) {
	t.Run("decision slot filled", func(t *testing.T) {
		testIsHeadWeakUsesHeadEpochShufflingCaches(t, 1)
	})
	t.Run("decision slot empty", func(t *testing.T) {
		testIsHeadWeakUsesHeadEpochShufflingCaches(t, 2)
	})
}

func testIsHeadWeakUsesHeadEpochShufflingCaches(t *testing.T, slotsBeforeBoundary uint64) {
	f, headRoot, justifiedState := newHeadWeakCommitteeStore(t, false)
	graph := f.forkGraph.(*headSnapshotForkGraph)
	cfg := f.beaconCfg

	decisionRoot := common.Hash{0xdd, 0x01}
	decisionBlock := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	decisionBlock.Block.Slot = cfg.SlotsPerEpoch - slotsBeforeBoundary
	decisionBlock.Block.ParentRoot = f.JustifiedCheckpoint().Root
	graph.headers[decisionRoot] = decisionBlock.SignedBeaconBlockHeader().Header
	graph.blocks[decisionRoot] = decisionBlock

	laterRoot := common.Hash{0xdd, 0x02}
	laterBlock := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	laterBlock.Block.Slot = cfg.SlotsPerEpoch
	laterBlock.Block.ParentRoot = decisionRoot
	graph.headers[laterRoot] = laterBlock.SignedBeaconBlockHeader().Header
	graph.blocks[laterRoot] = laterBlock

	headBlock := graph.blocks[headRoot]
	headBlock.Block.Slot = 2*cfg.SlotsPerEpoch + 8
	headBlock.Block.ParentRoot = laterRoot
	graph.headers[headRoot] = headBlock.SignedBeaconBlockHeader().Header

	previous := caches.ShuffledIndiciesCacheGlobal
	caches.ShuffledIndiciesCacheGlobal = caches.NewShuffledIndiciesCache("shuffled_indicies", caches.MaxShuffledIndiciesCacheSize)
	t.Cleanup(func() { caches.ShuffledIndiciesCacheGlobal = previous })
	headEpochSet := make([]uint64, len(justifiedState.shuffledSet))
	for i := range headEpochSet {
		headEpochSet[i] = uint64(len(headEpochSet) - 1 - i)
	}
	caches.ShuffledIndiciesCacheGlobal.Put(2, decisionRoot, headEpochSet)

	f.setUnequivocating(47)
	require.False(t, f.isHeadWeakWith(headRoot, justifiedState, noAttestations))

	caches.ShuffledIndiciesCacheGlobal.Put(2, decisionRoot, justifiedState.shuffledSet)
	require.True(t, f.isHeadWeakWith(headRoot, justifiedState, noAttestations))

	caches.ShuffledIndiciesCacheGlobal = caches.NewShuffledIndiciesCache("shuffled_indicies", caches.MaxShuffledIndiciesCacheSize)
	f.checkpointStates.Store(solid.Checkpoint{Epoch: 2, Root: laterRoot}, &checkpointState{beaconConfig: cfg, shuffledSet: headEpochSet})
	require.False(t, f.isHeadWeakWith(headRoot, justifiedState, noAttestations))
}

func TestIsHeadWeakFallsBackToJustifiedShuffling(t *testing.T) {
	f, headRoot, justifiedState := newHeadWeakCommitteeStore(t, false)
	f.setUnequivocating(16)
	require.False(t, f.isHeadWeakWith(headRoot, justifiedState, noAttestations))
}
