package statechange_test

import (
	"math"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/caches"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/impl/eth2/statechange"
	"github.com/erigontech/erigon/common"
)

func ptcDecisionRoot(t *testing.T, s *state.CachingBeaconState) common.Hash {
	t.Helper()
	header := s.LatestBlockHeader()
	root, err := header.HashSSZ()
	require.NoError(t, err)
	return root
}

func setPtcHeaderBodyRoot(t *testing.T, s *state.CachingBeaconState, value byte) {
	t.Helper()
	header := s.LatestBlockHeader()
	header.BodyRoot[0] = value
	s.SetLatestBlockHeader(&header)
	stateRoot, err := s.HashSSZ()
	require.NoError(t, err)
	root, err := header.HashSSZ()
	require.NoError(t, err)
	index := int(s.Slot() % s.BeaconConfig().SlotsPerHistoricalRoot)
	require.NoError(t, s.SetBlockRootAt(index, root))
	require.NoError(t, s.SetStateRootAt(index, stateRoot))
}

func processPtcWindowCopy(tb testing.TB, source *state.CachingBeaconState) *state.CachingBeaconState {
	tb.Helper()
	copied, err := source.Copy()
	require.NoError(tb, err)
	require.NoError(tb, statechange.ProcessPtcWindow(copied))
	return copied
}

func TestProcessPtcWindowSeparatesDecisionRootsAndRegistries(t *testing.T) {
	resetCaches := useFreshValidatorCaches(t)
	base := ptcWindowTestState(t, 16_384)
	branch, err := base.Copy()
	require.NoError(t, err)
	targetEpoch := nextPtcEpochFirstSlot(base) / base.BeaconConfig().SlotsPerEpoch
	require.NoError(t, branch.SetExitEpochForValidatorAtIndex(branch.ValidatorLength()-1, targetEpoch))
	setPtcHeaderBodyRoot(t, branch, 2)
	wantBase := coldPtcWindow(t, base, resetCaches)
	wantBranch := coldPtcWindow(t, branch, resetCaches)
	resetCaches()

	firstBase := processPtcWindowCopy(t, base)
	firstBranch := processPtcWindowCopy(t, branch)
	secondBase := processPtcWindowCopy(t, base)
	require.Equal(t, wantBase, ptcWindowValues(firstBase))
	require.Equal(t, wantBranch, ptcWindowValues(firstBranch))
	require.Equal(t, wantBase, ptcWindowValues(secondBase))
}

func TestProcessSlotsSeparatesSameHeaderDifferentRegistries(t *testing.T) {
	resetCaches := useFreshValidatorCaches(t)
	base, boundarySlot := ptcWindowSlotTransitionState(t, 16_384)
	branch, err := base.Copy()
	require.NoError(t, err)
	targetEpoch := state.Epoch(base) + base.BeaconConfig().MinSeedLookahead + 1
	require.NoError(t, branch.SetExitEpochForValidatorAtIndex(branch.ValidatorLength()-1, targetEpoch))
	require.Equal(t, base.LatestBlockHeader(), branch.LatestBlockHeader())
	baseBlockRoots, err := base.BlockRoots().HashSSZ()
	require.NoError(t, err)
	branchBlockRoots, err := branch.BlockRoots().HashSSZ()
	require.NoError(t, err)
	require.Equal(t, baseBlockRoots, branchBlockRoots)

	sources := [2]*state.CachingBeaconState{base, branch}
	var wantWindows [2][][]uint64
	var wantActive [2][]uint64
	for i, source := range sources {
		resetCaches()
		cold, copyErr := source.Copy()
		require.NoError(t, copyErr)
		require.NoError(t, transition.DefaultMachine.ProcessSlots(cold, boundarySlot+1))
		wantWindows[i] = ptcWindowValues(cold)
		wantActive[i] = append([]uint64(nil), cold.GetActiveValidatorsIndices(targetEpoch)...)
	}

	for _, order := range [][2]int{{0, 1}, {1, 0}} {
		resetCaches()
		for _, i := range order {
			got, copyErr := sources[i].Copy()
			require.NoError(t, copyErr)
			require.NoError(t, transition.DefaultMachine.ProcessSlots(got, boundarySlot+1))
			gotActive := got.GetActiveValidatorsIndices(targetEpoch)
			require.Equal(t, len(wantActive[i]), len(gotActive))
			require.Equal(t, wantActive[i], gotActive)
			require.Equal(t, wantWindows[i], ptcWindowValues(got))
		}
	}
}

func TestProcessPtcWindowIgnoresCachesWithoutSlotTransitionEvidence(t *testing.T) {
	resetCaches := useFreshValidatorCaches(t)
	base := ptcWindowTestState(t, 2_048)
	cfg := base.BeaconConfig()
	targetEpoch := nextPtcEpochFirstSlot(base) / cfg.SlotsPerEpoch
	headerRoot := ptcDecisionRoot(t, base)
	blockRoot := common.Hash{0xb1}
	stateRoot := common.Hash{0xc1}
	index := int(base.Slot() % cfg.SlotsPerHistoricalRoot)
	require.NoError(t, base.SetBlockRootAt(index, blockRoot))
	require.NoError(t, base.SetStateRootAt(index, stateRoot))
	want := coldPtcWindow(t, base, resetCaches)

	activePoison := make([]uint64, base.ValidatorLength()-1)
	shuffledPoison := make([]uint64, base.ValidatorLength())
	for i := range activePoison {
		activePoison[i] = uint64(i)
	}
	for i := range shuffledPoison {
		shuffledPoison[i] = uint64(len(shuffledPoison) - i - 1)
	}
	resetCaches()
	for _, root := range []common.Hash{headerRoot, blockRoot, stateRoot} {
		caches.ActiveValidatorsCacheGlobal.Put(targetEpoch, root, activePoison)
		caches.LookaheadActiveValidatorsCacheGlobal.Put(targetEpoch, root, activePoison)
		caches.ShuffledIndiciesCacheGlobal.Put(targetEpoch, root, shuffledPoison)
		caches.LookaheadShuffledIndiciesCacheGlobal.Put(targetEpoch, root, shuffledPoison)
	}

	got := processPtcWindowCopy(t, base)
	require.Equal(t, want, ptcWindowValues(got))
	for _, root := range []common.Hash{headerRoot, blockRoot, stateRoot} {
		mainActive, ok := caches.ActiveValidatorsCacheGlobal.Get(targetEpoch, root)
		require.True(t, ok)
		require.Same(t, unsafe.SliceData(activePoison), unsafe.SliceData(mainActive))
		lookaheadActive, ok := caches.LookaheadActiveValidatorsCacheGlobal.Get(targetEpoch, root)
		require.True(t, ok)
		require.Same(t, unsafe.SliceData(activePoison), unsafe.SliceData(lookaheadActive))
		mainShuffled, ok := caches.ShuffledIndiciesCacheGlobal.Get(targetEpoch, root)
		require.True(t, ok)
		require.Same(t, unsafe.SliceData(shuffledPoison), unsafe.SliceData(mainShuffled))
		lookaheadShuffled, ok := caches.LookaheadShuffledIndiciesCacheGlobal.Get(targetEpoch, root)
		require.True(t, ok)
		require.Same(t, unsafe.SliceData(shuffledPoison), unsafe.SliceData(lookaheadShuffled))
	}
}

func TestUpgradeToGloasWarmCacheMatchesColdResult(t *testing.T) {
	resetCaches := useFreshValidatorCaches(t)
	boundary := ptcWindowTestState(t, 16_384)
	boundary.SetVersion(clparams.FuluVersion)
	base, err := boundary.Copy()
	require.NoError(t, err)
	require.NoError(t, base.SetSlot(base.Slot()+1))
	cold, err := base.Copy()
	require.NoError(t, err)
	resetCaches()
	require.NoError(t, cold.UpgradeToGloas())
	want := ptcWindowValues(cold)
	resetCaches()
	_, err = boundary.GetBeaconCommitee(nextPtcEpochFirstSlot(boundary), 0)
	require.NoError(t, err)

	first, err := base.Copy()
	require.NoError(t, err)
	second, err := base.Copy()
	require.NoError(t, err)
	require.NoError(t, first.UpgradeToGloas())
	require.NoError(t, second.UpgradeToGloas())
	require.Equal(t, want, ptcWindowValues(first))
	require.Equal(t, want, ptcWindowValues(second))
}

func TestProcessPtcWindowDoesNotCacheUnknownDecisionRoot(t *testing.T) {
	resetCaches := useFreshValidatorCaches(t)
	base := ptcWindowTestState(t, 2_048)
	header := base.LatestBlockHeader()
	header.Root = common.Hash{}
	base.SetLatestBlockHeader(&header)
	want := coldPtcWindow(t, base, resetCaches)
	resetCaches()

	got := processPtcWindowCopy(t, base)
	require.Equal(t, want, ptcWindowValues(got))
	epoch := nextPtcEpochFirstSlot(base) / base.BeaconConfig().SlotsPerEpoch
	headerRoot, err := header.HashSSZ()
	require.NoError(t, err)
	ringRoot := base.BlockRoots().Get(int(base.Slot() % base.BeaconConfig().SlotsPerHistoricalRoot))
	stateRoot := base.StateRoots().Get(int(base.Slot() % base.BeaconConfig().SlotsPerHistoricalRoot))
	for _, root := range []common.Hash{{}, headerRoot, ringRoot, stateRoot} {
		_, ok := caches.ShuffledIndiciesCacheGlobal.Get(epoch, root)
		require.False(t, ok)
		_, ok = caches.LookaheadShuffledIndiciesCacheGlobal.Get(epoch, root)
		require.False(t, ok)
		_, ok = caches.ActiveValidatorsCacheGlobal.Get(epoch, root)
		require.False(t, ok)
		_, ok = caches.LookaheadActiveValidatorsCacheGlobal.Get(epoch, root)
		require.False(t, ok)
	}
}

func TestProcessPtcWindowReplaysDoNotEvictMainCaches(t *testing.T) {
	useFreshValidatorCaches(t)
	base := ptcWindowTestState(t, 2_048)
	epoch := nextPtcEpochFirstSlot(base) / base.BeaconConfig().SlotsPerEpoch
	root := common.Hash{0xff}
	active := []uint64{1, 2, 3}
	shuffled := []uint64{3, 2, 1}
	caches.ActiveValidatorsCacheGlobal.Put(epoch, root, active)
	caches.ShuffledIndiciesCacheGlobal.Put(epoch, root, shuffled)

	for i := range caches.MaxShuffledIndiciesCacheSize + 1 {
		replay, err := base.Copy()
		require.NoError(t, err)
		setPtcHeaderBodyRoot(t, replay, byte(i+1))
		require.NoError(t, statechange.ProcessPtcWindow(replay))
	}

	gotActive, ok := caches.ActiveValidatorsCacheGlobal.Get(epoch, root)
	require.True(t, ok)
	require.Equal(t, active, gotActive)
	gotShuffled, ok := caches.ShuffledIndiciesCacheGlobal.Get(epoch, root)
	require.True(t, ok)
	require.Equal(t, shuffled, gotShuffled)
}

func TestUnavailableDecisionRootsStayUncached(t *testing.T) {
	resetCaches := useFreshValidatorCaches(t)
	base := ptcWindowTestState(t, 2_048)
	cfg := base.BeaconConfig()
	poison := make([]uint64, base.ValidatorLength())
	for i := range poison {
		poison[i] = math.MaxUint64
	}
	for _, epoch := range []uint64{0, 1, state.Epoch(base) + 3} {
		resetCaches()
		caches.ActiveValidatorsCacheGlobal.Put(epoch, common.Hash{}, poison)
		caches.LookaheadActiveValidatorsCacheGlobal.Put(epoch, common.Hash{}, poison)
		caches.ShuffledIndiciesCacheGlobal.Put(epoch, common.Hash{}, poison)
		caches.LookaheadShuffledIndiciesCacheGlobal.Put(epoch, common.Hash{}, poison)
		copied, err := base.Copy()
		require.NoError(t, err)

		indices := copied.GetActiveValidatorsIndices(epoch)
		require.Len(t, indices, base.ValidatorLength())
		require.Equal(t, uint64(0), indices[0])
		committee, err := copied.GetBeaconCommitee(epoch*cfg.SlotsPerEpoch, 0)
		require.NoError(t, err)
		require.NotEqual(t, uint64(math.MaxUint64), committee[0])
		mainActive, ok := caches.ActiveValidatorsCacheGlobal.Get(epoch, common.Hash{})
		require.True(t, ok)
		require.Same(t, unsafe.SliceData(poison), unsafe.SliceData(mainActive))
		lookaheadActive, ok := caches.LookaheadActiveValidatorsCacheGlobal.Get(epoch, common.Hash{})
		require.True(t, ok)
		require.Same(t, unsafe.SliceData(poison), unsafe.SliceData(lookaheadActive))
		mainShuffled, ok := caches.ShuffledIndiciesCacheGlobal.Get(epoch, common.Hash{})
		require.True(t, ok)
		require.Same(t, unsafe.SliceData(poison), unsafe.SliceData(mainShuffled))
		lookaheadShuffled, ok := caches.LookaheadShuffledIndiciesCacheGlobal.Get(epoch, common.Hash{})
		require.True(t, ok)
		require.Same(t, unsafe.SliceData(poison), unsafe.SliceData(lookaheadShuffled))
		root := ptcDecisionRoot(t, base)
		_, ok = caches.ActiveValidatorsCacheGlobal.Get(epoch, root)
		require.False(t, ok)
		_, ok = caches.LookaheadActiveValidatorsCacheGlobal.Get(epoch, root)
		require.False(t, ok)
		_, ok = caches.ShuffledIndiciesCacheGlobal.Get(epoch, root)
		require.False(t, ok)
		_, ok = caches.LookaheadShuffledIndiciesCacheGlobal.Get(epoch, root)
		require.False(t, ok)
	}
}

func TestMainCachesTakePrecedenceOverLookaheadCaches(t *testing.T) {
	useFreshValidatorCaches(t)
	base := ptcWindowTestState(t, 2_048)
	fresh, err := base.Copy()
	require.NoError(t, err)
	epoch := nextPtcEpochFirstSlot(base) / base.BeaconConfig().SlotsPerEpoch
	blockRoot := ptcDecisionRoot(t, base)
	stateRoot := base.StateRoots().Get(int(base.Slot() % base.BeaconConfig().SlotsPerHistoricalRoot))
	main := make([]uint64, base.ValidatorLength())
	lookahead := make([]uint64, base.ValidatorLength())
	for i := range main {
		main[i] = uint64(i)
		lookahead[i] = uint64(len(lookahead) - i - 1)
	}
	caches.ActiveValidatorsCacheGlobal.Put(epoch, blockRoot, main)
	caches.LookaheadActiveValidatorsCacheGlobal.Put(epoch, stateRoot, lookahead)
	caches.ShuffledIndiciesCacheGlobal.Put(epoch, blockRoot, main)
	caches.LookaheadShuffledIndiciesCacheGlobal.Put(epoch, stateRoot, lookahead)

	indices := fresh.GetActiveValidatorsIndices(epoch)
	require.Same(t, unsafe.SliceData(main), unsafe.SliceData(indices))
	committee, err := fresh.GetBeaconCommitee(epoch*base.BeaconConfig().SlotsPerEpoch, 0)
	require.NoError(t, err)
	require.Same(t, unsafe.SliceData(main), unsafe.SliceData(committee))
}

func TestProcessPtcWindowConcurrentSharedCache(t *testing.T) {
	resetCaches := useFreshValidatorCaches(t)
	base := ptcWindowTestState(t, 16_384)
	branch, err := base.Copy()
	require.NoError(t, err)
	targetEpoch := nextPtcEpochFirstSlot(base) / base.BeaconConfig().SlotsPerEpoch
	require.NoError(t, branch.SetExitEpochForValidatorAtIndex(branch.ValidatorLength()-1, targetEpoch))
	setPtcHeaderBodyRoot(t, branch, 4)
	wants := [2][][]uint64{coldPtcWindow(t, base, resetCaches), coldPtcWindow(t, branch, resetCaches)}
	resetCaches()

	var wg sync.WaitGroup
	for i := range 8 {
		source := base
		branchIndex := 0
		if i%2 != 0 {
			source = branch
			branchIndex = 1
		}
		copied, copyErr := source.Copy()
		require.NoError(t, copyErr)
		want := wants[branchIndex]
		wg.Go(func() {
			if !assert.NoError(t, statechange.ProcessPtcWindow(copied)) {
				return
			}
			assert.Equal(t, want, ptcWindowValues(copied))
		})
	}
	wg.Wait()
}
