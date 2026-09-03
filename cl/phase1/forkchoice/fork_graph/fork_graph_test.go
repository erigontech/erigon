// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package fork_graph

import (
	_ "embed"
	"errors"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

//go:embed test_data/block_0xe2a37a22d208ebe969c50e9d44bb3f1f63c5404787b9c214a5f2f28fb9835feb.ssz_snappy
var block1 []byte

//go:embed test_data/block_0xbf1a9ba2d349f6b5a5095bff40bd103ae39177e36018fb1f589953b9eeb0ca9d.ssz_snappy
var block2 []byte

//go:embed test_data/anchor_state.ssz_snappy
var anchor []byte

type blockingRemoveFs struct {
	afero.Fs
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type blockingPathRemoveFs struct {
	afero.Fs
	target  string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (fs *blockingPathRemoveFs) Remove(name string) error {
	if name == fs.target {
		fs.once.Do(func() {
			close(fs.entered)
			<-fs.release
		})
	}
	return fs.Fs.Remove(name)
}

var errPartialEnvelopeWrite = errors.New("partial envelope write")

type partialWriteFs struct {
	afero.Fs
	fail bool
}

type countingStatFs struct {
	afero.Fs
	mu    sync.Mutex
	stats int
}

func (fs *countingStatFs) Stat(name string) (os.FileInfo, error) {
	fs.mu.Lock()
	fs.stats++
	fs.mu.Unlock()
	return fs.Fs.Stat(name)
}

type blockingRenameFs struct {
	afero.Fs
	target  string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (fs *blockingRenameFs) Rename(oldname, newname string) error {
	if newname == fs.target {
		fs.once.Do(func() {
			close(fs.entered)
			<-fs.release
		})
	}
	return fs.Fs.Rename(oldname, newname)
}

func (fs *partialWriteFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	file, err := fs.Fs.OpenFile(name, flag, perm)
	if err != nil || !fs.fail {
		return file, err
	}
	return partialWriteFile{File: file}, nil
}

type partialWriteFile struct {
	afero.File
}

func (f partialWriteFile) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, errPartialEnvelopeWrite
	}
	n, _ := f.File.Write(p[:1])
	return n, errPartialEnvelopeWrite
}

func (fs *blockingRemoveFs) Remove(name string) error {
	fs.once.Do(func() {
		close(fs.entered)
		<-fs.release
	})
	return fs.Fs.Remove(name)
}

func TestForkGraphInDisk(t *testing.T) {
	blockA, blockB, blockC := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion),
		cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion),
		cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(blockA, block1, int(clparams.Phase0Version)))
	require.NoError(t, utils.DecodeSSZSnappy(blockB, block2, int(clparams.Phase0Version)))
	require.NoError(t, utils.DecodeSSZSnappy(blockC, block2, int(clparams.Phase0Version)))
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))
	graph, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	_, status, err := graph.AddChainSegment(blockA, true)
	require.NoError(t, err)
	require.Equal(t, Success, status)
	// Now make blockC a bad block
	blockC.Block.ProposerIndex = 81214459 // some invalid thing
	_, status, err = graph.AddChainSegment(blockC, true)
	require.Error(t, err)
	require.Equal(t, InvalidBlock, status)
	// Save current state hash
	_, status, err = graph.AddChainSegment(blockB, true)
	require.NoError(t, err)
	require.Equal(t, Success, status)
	// Try again with same should yield success
	_, status, err = graph.AddChainSegment(blockB, true)
	require.NoError(t, err)
	require.Equal(t, PreValidated, status)
}

// TestNewForkGraphDiskReturnsErrorOnDumpFailure pins that a failure to
// persist the anchor state to disk is returned as an error instead of
// panicking during startup.
func TestNewForkGraphDiskReturnsErrorOnDumpFailure(t *testing.T) {
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))

	// A read-only filesystem rejects the O_CREATE|O_TRUNC|O_RDWR open that
	// DumpBeaconStateOnDisk issues, simulating a disk-full or permission error.
	failingFs := afero.NewReadOnlyFs(afero.NewMemMapFs())

	require.NotPanics(t, func() {
		graph, err := NewForkGraphDisk(anchorState, nil, failingFs, beacon_router_configuration.RouterConfiguration{})
		require.Error(t, err)
		require.Nil(t, graph)
	})
}

func TestDumpEnvelopeErrorDoesNotPublishPartialFile(t *testing.T) {
	baseFs := afero.NewMemMapFs()
	fs := &partialWriteFs{Fs: baseFs, fail: true}
	cfg := clparams.MainnetBeaconConfig
	f := &forkGraphDisk{fs: fs, beaconCfg: &cfg}
	root := common.Hash{1}
	f.headers.Store(root, &cltypes.BeaconBlockHeader{Slot: 1})
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}

	err := f.DumpEnvelopeOnDisk(root, envelope)
	require.ErrorIs(t, err, errPartialEnvelopeWrite)
	require.False(t, f.HasEnvelope(root))

	fs.fail = false
	require.NoError(t, f.DumpEnvelopeOnDisk(root, envelope))
	require.True(t, f.HasEnvelope(root))
	_, err = f.ReadEnvelopeFromDisk(root)
	require.NoError(t, err)
}

func TestHasEnvelopeCachesMissAndDumpInvalidatesMiss(t *testing.T) {
	fs := &countingStatFs{Fs: afero.NewMemMapFs()}
	cfg := clparams.MainnetBeaconConfig
	f := &forkGraphDisk{fs: fs, beaconCfg: &cfg}
	root := common.Hash{1}
	f.headers.Store(root, &cltypes.BeaconBlockHeader{Slot: 1})

	require.False(t, f.HasEnvelope(root))
	require.False(t, f.HasEnvelope(root))
	fs.mu.Lock()
	require.Equal(t, 1, fs.stats)
	fs.mu.Unlock()

	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	require.NoError(t, f.DumpEnvelopeOnDisk(root, envelope))
	require.True(t, f.HasEnvelope(root))
}

func TestDumpEnvelopeBeforePruneDoesNotSurvivePrune(t *testing.T) {
	baseFs := afero.NewMemMapFs()
	oldRoot := common.Hash{1}
	newerRoot := common.Hash{2}
	fs := &blockingRenameFs{
		Fs:      baseFs,
		target:  getEnvelopeFilename(oldRoot),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig, children: make(map[common.Hash]*validatedChildren)}
	oldBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	oldBlock.Block.Slot = 64
	newerBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	newerBlock.Block.Slot = 128
	f.blocks.Store(oldRoot, oldBlock)
	f.blocks.Store(newerRoot, newerBlock)
	f.headers.Store(oldRoot, &cltypes.BeaconBlockHeader{Slot: oldBlock.Block.Slot})
	f.headers.Store(newerRoot, &cltypes.BeaconBlockHeader{Slot: newerBlock.Block.Slot})
	require.NoError(t, afero.WriteFile(baseFs, getBeaconStateFilename(oldRoot), []byte{1}, 0o644))
	require.NoError(t, afero.WriteFile(baseFs, getBeaconStateFilename(newerRoot), []byte{1}, 0o644))
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}

	dumpDone := make(chan error, 1)
	go func() { dumpDone <- f.DumpEnvelopeOnDisk(oldRoot, envelope) }()
	select {
	case <-fs.entered:
	case <-time.After(time.Second):
		t.Fatal("envelope dump did not reach rename")
	}
	pruneDone := make(chan error, 1)
	go func() { pruneDone <- f.Prune(100) }()
	select {
	case err := <-pruneDone:
		close(fs.release)
		require.NoError(t, err)
		t.Fatal("prune crossed an active envelope publication")
	case <-time.After(50 * time.Millisecond):
	}
	close(fs.release)
	require.NoError(t, <-dumpDone)
	require.NoError(t, <-pruneDone)
	require.False(t, f.HasEnvelope(oldRoot))
}

func TestNewForkGraphDiskCachesAnchorStateRoot(t *testing.T) {
	for _, tc := range []struct {
		name       string
		stateSlot  uint64
		headerSlot uint64
		headerRoot common.Hash
		cachedRoot common.Hash
	}{
		{name: "skipped slot", stateSlot: 64, headerSlot: 63, headerRoot: common.Hash{1}},
		{name: "block slot", stateSlot: 64, headerSlot: 64},
		{name: "restored block slot", stateSlot: 64, headerSlot: 64, cachedRoot: common.Hash{2}},
		{name: "legacy block slot", stateSlot: 64, headerSlot: 64, headerRoot: common.Hash{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			anchorState := state.New(&clparams.MainnetBeaconConfig)
			anchorState.SetVersion(clparams.GloasVersion)
			require.NoError(t, anchorState.SetSlot(tc.stateSlot))
			header := &cltypes.BeaconBlockHeader{Slot: tc.headerSlot, Root: tc.headerRoot}
			anchorState.SetLatestBlockHeader(header)
			expectedStateRoot, err := anchorState.HashSSZ()
			require.NoError(t, err)
			if tc.cachedRoot != (common.Hash{}) {
				expectedStateRoot = tc.cachedRoot
				anchorState.SetPreviousStateRoot(tc.cachedRoot)
			} else if tc.headerSlot == tc.stateSlot && tc.headerRoot != (common.Hash{}) {
				expectedStateRoot = tc.headerRoot
			}
			anchorRoot, err := anchorState.BlockRoot()
			require.NoError(t, err)

			forkGraph, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
			require.NoError(t, err)
			graph := forkGraph.(*forkGraphDisk)

			require.Equal(t, common.Hash(expectedStateRoot), anchorState.PeekPreviousStateRoot())
			require.Equal(t, header.Root, anchorState.LatestBlockHeader().Root)
			persistedState, err := graph.readBeaconStateFromDisk(anchorRoot)
			require.NoError(t, err)
			require.Equal(t, common.Hash(expectedStateRoot), persistedState.PeekPreviousStateRoot())
		})
	}
}

func TestNewForkGraphDiskKeepsAnchorHeaderVisibleAcrossSkippedSlots(t *testing.T) {
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	anchorState.SetVersion(clparams.GloasVersion)
	require.NoError(t, anchorState.SetSlot(64))
	anchorState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: 31})
	anchorRoot, err := anchorState.BlockRoot()
	require.NoError(t, err)

	forkGraph, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	graph := forkGraph.(*forkGraphDisk)

	_, ok := graph.GetHeader(anchorRoot)
	require.True(t, ok)
	require.Equal(t, uint64(64), graph.LowestAvailableSlot())
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	require.NoError(t, graph.DumpEnvelopeOnDisk(anchorRoot, envelope))
	require.True(t, graph.HasEnvelope(anchorRoot))
	_, err = graph.ReadEnvelopeFromDisk(anchorRoot)
	require.NoError(t, err)
}

// A prune for an already-covered slot (e.g. from a concurrent lock-free drain)
// must not move the lowest-available marker backward past deleted data.
func TestPruneKeepsLowestAvailableBlockMonotonic(t *testing.T) {
	f := &forkGraphDisk{fs: afero.NewMemMapFs(), beaconCfg: &clparams.MainnetBeaconConfig}
	addBlockWithState := func(slot uint64, root common.Hash) {
		b := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
		b.Block.Slot = slot
		f.blocks.Store(root, b)
		require.NoError(t, afero.WriteFile(f.fs, getBeaconStateFilename(root), []byte{1}, 0o644))
	}
	addBlockWithState(100, common.Hash{1})
	addBlockWithState(200, common.Hash{2})
	f.MarkHeaderAsInvalid(common.Hash{1})
	require.True(t, f.IsBlockInvalid(common.Hash{1}))
	f.MarkPayloadAccepted(common.Hash{1}, true)
	verified, accepted := f.PayloadAccepted(common.Hash{1})
	require.True(t, accepted)
	require.True(t, verified)

	require.NoError(t, f.Prune(150))
	require.Equal(t, uint64(151), f.LowestAvailableSlot())
	require.False(t, f.IsBlockInvalid(common.Hash{1}))
	_, accepted = f.PayloadAccepted(common.Hash{1})
	require.False(t, accepted)

	require.NoError(t, f.Prune(120))
	require.Equal(t, uint64(151), f.LowestAvailableSlot())
}

func TestPruneKeepsParticipationIndicesFromRetainedConcurrentAdd(t *testing.T) {
	const blockSlot = uint64(65)
	const pruneSlot = blockSlot - 1
	cfg := &clparams.MainnetBeaconConfig
	beaconState := state.New(cfg)
	beaconState.SetVersion(clparams.AltairVersion)
	require.NoError(t, beaconState.SetSlot(blockSlot))
	beaconState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: blockSlot})
	require.NoError(t, beaconState.SetCurrentSyncCommittee(solid.NewSyncCommittee()))
	require.NoError(t, beaconState.SetNextSyncCommittee(solid.NewSyncCommittee()))
	current := solid.ParticipationBitListFromBytes([]byte{1, 2}, int(cfg.ValidatorRegistryLimit))
	previous := solid.ParticipationBitListFromBytes([]byte{3, 4}, int(cfg.ValidatorRegistryLimit))
	beaconState.SetCurrentEpochParticipation(current)
	beaconState.SetPreviousEpochParticipation(previous)

	block := cltypes.NewSignedBeaconBlock(cfg, clparams.AltairVersion)
	block.Block.Slot = blockSlot
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	beaconState.SetPreviousStateRoot(common.Hash(blockRoot))
	boundaryPublished := make(chan struct{})
	continuePrune := make(chan struct{})
	f := &forkGraphDisk{
		fs:                    afero.NewMemMapFs(),
		beaconCfg:             cfg,
		rcfg:                  beacon_router_configuration.RouterConfiguration{Beacon: true},
		children:              make(map[common.Hash]*validatedChildren),
		currentState:          beaconState,
		currentStateBlockRoot: common.Hash(blockRoot),
		pruneBoundaryHook: func() {
			close(boundaryPublished)
			<-continuePrune
		},
	}
	newerRoot := common.Hash{0xff}
	newerBlock := cltypes.NewSignedBeaconBlock(cfg, clparams.AltairVersion)
	newerBlock.Block.Slot = blockSlot + cfg.SlotsPerEpoch
	f.blocks.Store(newerRoot, newerBlock)
	require.NoError(t, afero.WriteFile(f.fs, getBeaconStateFilename(newerRoot), []byte{1}, 0o644))

	pruneDone := make(chan error, 1)
	go func() { pruneDone <- f.Prune(pruneSlot) }()
	select {
	case <-boundaryPublished:
	case <-time.After(time.Second):
		t.Fatal("prune did not publish its boundary")
	}
	require.Equal(t, blockSlot, f.LowestAvailableSlot())
	_, result, err := f.AddChainSegment(block, false)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	close(continuePrune)
	require.NoError(t, <-pruneDone)

	epoch := blockSlot / cfg.SlotsPerEpoch
	gotCurrent, err := f.GetCurrentParticipationIndicies(epoch)
	require.NoError(t, err)
	require.NotNil(t, gotCurrent)
	require.Equal(t, current.Bytes(), gotCurrent.Bytes())
	gotPrevious, err := f.GetPreviousParticipationIndicies(epoch)
	require.NoError(t, err)
	require.NotNil(t, gotPrevious)
	require.Equal(t, previous.Bytes(), gotPrevious.Bytes())
}

func TestPruneKeepsParticipationIndicesFromRetainedPriorAdd(t *testing.T) {
	const blockSlot = uint64(65)
	const pruneSlot = uint64(64)
	cfg := &clparams.MainnetBeaconConfig
	beaconState := state.New(cfg)
	beaconState.SetVersion(clparams.AltairVersion)
	require.NoError(t, beaconState.SetSlot(blockSlot))
	beaconState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: blockSlot})
	require.NoError(t, beaconState.SetCurrentSyncCommittee(solid.NewSyncCommittee()))
	require.NoError(t, beaconState.SetNextSyncCommittee(solid.NewSyncCommittee()))
	current := solid.ParticipationBitListFromBytes([]byte{5, 6}, int(cfg.ValidatorRegistryLimit))
	previous := solid.ParticipationBitListFromBytes([]byte{7, 8}, int(cfg.ValidatorRegistryLimit))
	beaconState.SetCurrentEpochParticipation(current)
	beaconState.SetPreviousEpochParticipation(previous)

	block := cltypes.NewSignedBeaconBlock(cfg, clparams.AltairVersion)
	block.Block.Slot = blockSlot
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	beaconState.SetPreviousStateRoot(common.Hash(blockRoot))
	f := &forkGraphDisk{
		fs:                    afero.NewMemMapFs(),
		beaconCfg:             cfg,
		rcfg:                  beacon_router_configuration.RouterConfiguration{Beacon: true},
		children:              make(map[common.Hash]*validatedChildren),
		currentState:          beaconState,
		currentStateBlockRoot: common.Hash(blockRoot),
	}
	_, result, err := f.AddChainSegment(block, false)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	newerRoot := common.Hash{0xfe}
	newerBlock := cltypes.NewSignedBeaconBlock(cfg, clparams.AltairVersion)
	newerBlock.Block.Slot = blockSlot + cfg.SlotsPerEpoch
	f.blocks.Store(newerRoot, newerBlock)
	require.NoError(t, afero.WriteFile(f.fs, getBeaconStateFilename(newerRoot), []byte{1}, 0o644))

	require.NoError(t, f.Prune(pruneSlot))
	require.Equal(t, blockSlot, f.LowestAvailableSlot())
	_, retained := f.GetHeader(common.Hash(blockRoot))
	require.True(t, retained)
	epoch := blockSlot / cfg.SlotsPerEpoch
	gotCurrent, err := f.GetCurrentParticipationIndicies(epoch)
	require.NoError(t, err)
	require.NotNil(t, gotCurrent)
	require.Equal(t, current.Bytes(), gotCurrent.Bytes())
	gotPrevious, err := f.GetPreviousParticipationIndicies(epoch)
	require.NoError(t, err)
	require.NotNil(t, gotPrevious)
	require.Equal(t, previous.Bytes(), gotPrevious.Bytes())
}

func TestLastFullyPrunedEpoch(t *testing.T) {
	for _, tc := range []struct {
		pruneSlot uint64
		epoch     uint64
		ok        bool
	}{
		{pruneSlot: 0},
		{pruneSlot: 31},
		{pruneSlot: 32, epoch: 0, ok: true},
		{pruneSlot: 63, epoch: 0, ok: true},
		{pruneSlot: 64, epoch: 1, ok: true},
		{pruneSlot: 65, epoch: 1, ok: true},
	} {
		epoch, ok := lastFullyPrunedEpoch(tc.pruneSlot, 32)
		require.Equal(t, tc.ok, ok, "prune slot %d", tc.pruneSlot)
		require.Equal(t, tc.epoch, epoch, "prune slot %d", tc.pruneSlot)
	}
}

func TestOrphanEnvelopeIsNotRediscoveredAfterRootRemoval(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.Hash{1}
	require.NoError(t, afero.WriteFile(fs, getEnvelopeFilename(root), []byte{1}, 0o644))

	require.False(t, f.HasEnvelope(root))
	_, err := f.ReadEnvelopeFromDisk(root)
	require.ErrorIs(t, err, ErrStateNotFound)
	_, err = fs.Stat(getEnvelopeFilename(root))
	require.NoError(t, err)
}

func TestHasBlockChildAtOrAfterUsesValidatedChildren(t *testing.T) {
	f := &forkGraphDisk{}
	parentRoot := common.Hash{1}
	child := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	child.Block.ParentRoot = parentRoot
	child.Block.Slot = 64
	f.blocks.Store(common.Hash{2}, child)

	require.False(t, f.HasBlockChildAtOrAfter(parentRoot, 64))
	f.addValidatedChild(parentRoot, common.Hash{2}, 64)
	require.True(t, f.HasBlockChildAtOrAfter(parentRoot, 64))
	require.False(t, f.HasBlockChildAtOrAfter(parentRoot, 65))
	require.False(t, f.HasBlockChildAtOrAfter(common.Hash{3}, 64))
	f.removeValidatedChildren(map[common.Hash][]common.Hash{parentRoot: {{2}}})
	require.False(t, f.HasBlockChildAtOrAfter(parentRoot, 64))
}

func TestHasBlockEquivocationUsesValidatedHeaders(t *testing.T) {
	f := &forkGraphDisk{}
	root := common.Hash{1}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	block.Block.Slot = 64
	block.Block.ProposerIndex = 9
	f.blocks.Store(root, block)

	require.False(t, f.HasBlockEquivocation(64, 9, common.Hash{2}))
	f.headers.Store(root, &cltypes.BeaconBlockHeader{Slot: 64, ProposerIndex: 9})
	require.True(t, f.HasBlockEquivocation(64, 9, common.Hash{2}))
	require.False(t, f.HasBlockEquivocation(64, 9, root))
	require.False(t, f.HasBlockEquivocation(64, 8, common.Hash{2}))
	require.False(t, f.HasBlockEquivocation(65, 9, common.Hash{2}))
}

func TestHasBlockEquivocationUsesPruneBoundary(t *testing.T) {
	f := &forkGraphDisk{}
	f.headers.Store(common.Hash{1}, &cltypes.BeaconBlockHeader{Slot: 64, ProposerIndex: 9})
	f.headers.Store(common.Hash{2}, &cltypes.BeaconBlockHeader{Slot: 63, ProposerIndex: 9})
	f.lowestAvailableBlock.Store(65)

	require.True(t, f.HasBlockEquivocation(64, 9, common.Hash{3}))
	require.False(t, f.HasBlockEquivocation(63, 9, common.Hash{3}))
}

func TestRemoveValidatedChildrenBulkKeepsSameSlotSurvivor(t *testing.T) {
	f := &forkGraphDisk{children: make(map[common.Hash]*validatedChildren)}
	parentRoot := common.Hash{1}
	removed := make([]common.Hash, 1024)
	for i := range removed {
		removed[i][0] = byte(i)
		removed[i][1] = byte(i >> 8)
		f.addValidatedChild(parentRoot, removed[i], 128)
	}
	survivor := common.Hash{0xff, 0xff}
	f.addValidatedChild(parentRoot, survivor, 127)

	f.removeValidatedChildren(map[common.Hash][]common.Hash{parentRoot: removed})

	require.True(t, f.HasBlockChildAtOrAfter(parentRoot, 127))
	require.False(t, f.HasBlockChildAtOrAfter(parentRoot, 128))
	require.Equal(t, map[common.Hash]uint64{survivor: 127}, f.children[parentRoot].slots)
}

func TestValidatedChildQueryProgressesDuringPruneLifecycle(t *testing.T) {
	baseFs := afero.NewMemMapFs()
	fs := &blockingRemoveFs{Fs: baseFs, entered: make(chan struct{}), release: make(chan struct{})}
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig, children: make(map[common.Hash]*validatedChildren)}
	parentRoot := common.Hash{1}
	childRoot := common.Hash{2}
	newerRoot := common.Hash{3}
	child := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	child.Block.ParentRoot = parentRoot
	child.Block.Slot = 64
	newer := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	newer.Block.Slot = 128
	f.blocks.Store(childRoot, child)
	f.blocks.Store(newerRoot, newer)
	f.headers.Store(childRoot, &cltypes.BeaconBlockHeader{ParentRoot: parentRoot, Slot: child.Block.Slot})
	f.headers.Store(newerRoot, &cltypes.BeaconBlockHeader{Slot: newer.Block.Slot})
	require.NoError(t, afero.WriteFile(baseFs, getBeaconStateFilename(childRoot), []byte{1}, 0o644))
	require.NoError(t, afero.WriteFile(baseFs, getBeaconStateFilename(newerRoot), []byte{1}, 0o644))
	f.addValidatedChild(parentRoot, childRoot, 64)

	pruneDone := make(chan error, 1)
	go func() { pruneDone <- f.Prune(100) }()
	select {
	case <-fs.entered:
	case <-time.After(time.Second):
		t.Fatal("prune did not reach filesystem removal")
	}

	result := make(chan bool, 1)
	go func() { result <- f.HasBlockChildAtOrAfter(parentRoot, 64) }()
	select {
	case found := <-result:
		require.False(t, found)
	case <-time.After(time.Second):
		close(fs.release)
		t.Fatal("validated-child query blocked on the prune lifecycle")
	}

	oldBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	oldBlock.Block.ParentRoot = parentRoot
	oldBlock.Block.Slot = 63
	addDone := make(chan ChainSegmentInsertionResult, 1)
	go func() {
		_, result, _ := f.AddChainSegment(oldBlock, true)
		addDone <- result
	}()
	select {
	case result := <-addDone:
		require.Equal(t, BelowAnchor, result)
	case <-time.After(time.Second):
		close(fs.release)
		t.Fatal("below-boundary add blocked on filesystem cleanup")
	}
	aboveBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	aboveBlock.Block.ParentRoot = newerRoot
	aboveBlock.Block.Slot = 129
	aboveAddDone := make(chan ChainSegmentInsertionResult, 1)
	go func() {
		_, result, _ := f.AddChainSegment(aboveBlock, true)
		aboveAddDone <- result
	}()
	select {
	case result := <-aboveAddDone:
		require.NotEqual(t, BelowAnchor, result)
	case <-time.After(time.Second):
		close(fs.release)
		t.Fatal("above-boundary add blocked on filesystem cleanup")
	}
	statusDone := make(chan bool, 1)
	go func() {
		statusDone <- f.WithRetainedBlock(newerRoot, func() { f.MarkPayloadAccepted(newerRoot, false) })
	}()
	select {
	case retained := <-statusDone:
		require.True(t, retained)
	case <-time.After(time.Second):
		close(fs.release)
		t.Fatal("retained status update blocked on filesystem cleanup")
	}
	hasDone := make(chan bool, 1)
	go func() { hasDone <- f.HasEnvelope(newerRoot) }()
	select {
	case found := <-hasDone:
		require.False(t, found)
	case <-time.After(time.Second):
		close(fs.release)
		t.Fatal("retained envelope query blocked on filesystem cleanup")
	}
	readDone := make(chan error, 1)
	go func() { _, err := f.ReadEnvelopeFromDisk(newerRoot); readDone <- err }()
	select {
	case err := <-readDone:
		require.Error(t, err)
	case <-time.After(time.Second):
		close(fs.release)
		t.Fatal("retained envelope read blocked on filesystem cleanup")
	}
	close(fs.release)
	require.NoError(t, <-pruneDone)

	require.False(t, f.HasBlockChildAtOrAfter(parentRoot, 64))
	oldRoot, err := oldBlock.Block.HashSSZ()
	require.NoError(t, err)
	_, found := f.GetHeader(oldRoot)
	require.False(t, found)
}

func TestPruneYieldsLifecycleBetweenBatches(t *testing.T) {
	fs := afero.NewMemMapFs()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	cleanupCalls := 0
	f := &forkGraphDisk{
		fs:        fs,
		beaconCfg: &clparams.MainnetBeaconConfig,
		children:  make(map[common.Hash]*validatedChildren),
		pruneBatchHook: func() {
			once.Do(func() { close(entered); <-release })
		},
		pruneChildrenHook: func() { cleanupCalls++ },
	}
	parentRoot := common.Hash{0xaa}
	staleParentRoot := common.Hash{0xbb}
	oldRoots := make([]common.Hash, pruneBatchSize+1)
	for i := range oldRoots {
		oldRoots[i][0] = byte(i)
		oldRoots[i][1] = byte(i >> 8)
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
		block.Block.Slot = uint64(i + 1)
		f.blocks.Store(oldRoots[i], block)
		f.headers.Store(oldRoots[i], &cltypes.BeaconBlockHeader{ParentRoot: parentRoot, Slot: block.Block.Slot})
		f.addValidatedChild(parentRoot, oldRoots[i], block.Block.Slot)
	}
	f.addValidatedChild(staleParentRoot, oldRoots[len(oldRoots)-1], uint64(len(oldRoots)))
	newRoot := common.Hash{0xff, 0xff, 0xff}
	newBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	newBlock.Block.Slot = 512
	f.blocks.Store(newRoot, newBlock)
	f.headers.Store(newRoot, &cltypes.BeaconBlockHeader{Slot: newBlock.Block.Slot})
	f.addValidatedChild(parentRoot, newRoot, newBlock.Block.Slot)
	require.NoError(t, afero.WriteFile(fs, getBeaconStateFilename(newRoot), []byte{1}, 0o644))

	done := make(chan error, 1)
	go func() { done <- f.Prune(300) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("prune did not yield between batches")
	}
	require.Equal(t, uint64(301), f.LowestAvailableSlot())
	_, found := f.GetBlock(oldRoots[len(oldRoots)-1])
	require.False(t, found)
	require.False(t, f.HasBlockChildAtOrAfter(staleParentRoot, 1))
	progress := make(chan bool, 1)
	go func() { progress <- f.WithRetainedBlock(newRoot, func() { f.MarkPayloadAccepted(newRoot, false) }) }()
	select {
	case retained := <-progress:
		require.True(t, retained)
	case <-time.After(time.Second):
		close(release)
		t.Fatal("retained operation blocked between prune batches")
	}
	hasDone := make(chan bool, 1)
	go func() { hasDone <- f.HasEnvelope(newRoot) }()
	select {
	case found := <-hasDone:
		require.False(t, found)
	case <-time.After(time.Second):
		close(release)
		t.Fatal("envelope query blocked between prune batches")
	}
	readDone := make(chan error, 1)
	go func() { _, err := f.ReadEnvelopeFromDisk(newRoot); readDone <- err }()
	select {
	case err := <-readDone:
		require.Error(t, err)
	case <-time.After(time.Second):
		close(release)
		t.Fatal("envelope read blocked between prune batches")
	}
	close(release)
	require.NoError(t, <-done)
	for _, root := range oldRoots {
		_, found := f.blocks.Load(root)
		require.False(t, found)
	}
	require.Equal(t, 1, cleanupCalls)
	require.True(t, f.HasBlockChildAtOrAfter(parentRoot, newBlock.Block.Slot))
	require.False(t, f.HasBlockChildAtOrAfter(parentRoot, newBlock.Block.Slot+1))
	require.False(t, f.HasBlockChildAtOrAfter(staleParentRoot, 1))
}

func TestHasEnvelopeDoesNotRepopulateCacheDuringPrune(t *testing.T) {
	baseFs := afero.NewMemMapFs()
	oldRoot := common.Hash{1}
	newerRoot := common.Hash{2}
	fs := &blockingPathRemoveFs{
		Fs:      baseFs,
		target:  getEnvelopeFilename(oldRoot),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig, children: make(map[common.Hash]*validatedChildren)}
	oldBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	oldBlock.Block.Slot = 64
	newerBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	newerBlock.Block.Slot = 128
	f.blocks.Store(oldRoot, oldBlock)
	f.blocks.Store(newerRoot, newerBlock)
	f.headers.Store(oldRoot, &cltypes.BeaconBlockHeader{Slot: oldBlock.Block.Slot})
	f.headers.Store(newerRoot, &cltypes.BeaconBlockHeader{Slot: newerBlock.Block.Slot})
	require.NoError(t, afero.WriteFile(baseFs, getBeaconStateFilename(oldRoot), []byte{1}, 0o644))
	require.NoError(t, afero.WriteFile(baseFs, getBeaconStateFilename(newerRoot), []byte{1}, 0o644))
	require.NoError(t, afero.WriteFile(baseFs, getEnvelopeFilename(oldRoot), []byte{1}, 0o644))
	f.MarkHeaderAsInvalid(oldRoot)
	f.MarkPayloadUnavailable(oldRoot)

	pruneDone := make(chan error, 1)
	go func() { pruneDone <- f.Prune(100) }()
	select {
	case <-fs.entered:
	case <-time.After(time.Second):
		t.Fatal("prune did not reach envelope removal")
	}
	require.False(t, f.IsBlockInvalid(oldRoot))
	require.False(t, f.IsPayloadUnavailable(oldRoot))

	queryDone := make(chan bool, 1)
	go func() { queryDone <- f.HasEnvelope(oldRoot) }()
	lateDumpDone := make(chan error, 1)
	lateEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
	go func() { lateDumpDone <- f.DumpEnvelopeOnDisk(oldRoot, lateEnvelope) }()
	select {
	case found := <-queryDone:
		close(fs.release)
		require.NoError(t, <-pruneDone)
		require.False(t, found)
	case <-time.After(50 * time.Millisecond):
		close(fs.release)
		require.NoError(t, <-pruneDone)
		select {
		case found := <-queryDone:
			require.False(t, found)
		case <-time.After(time.Second):
			t.Fatal("envelope query did not progress after prune completed")
		}
	}
	select {
	case err := <-lateDumpDone:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("late envelope dump did not progress after prune completed")
	}
	require.False(t, f.HasEnvelope(oldRoot))
	require.False(t, f.IsBlockInvalid(oldRoot))
	require.False(t, f.IsPayloadUnavailable(oldRoot))
}

func TestAddChainSegmentRejectsSlotBelowPrunedBoundary(t *testing.T) {
	f := &forkGraphDisk{
		anchorSlot: 0,
		children:   make(map[common.Hash]*validatedChildren),
	}
	f.lowestAvailableBlock.Store(65)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	block.Block.Slot = 63

	_, status, err := f.AddChainSegment(block, true)
	require.NoError(t, err)
	require.Equal(t, BelowAnchor, status)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	_, headerFound := f.GetHeader(root)
	require.False(t, headerFound)
	require.False(t, f.HasBlockChildAtOrAfter(block.Block.ParentRoot, block.Block.Slot))
	require.False(t, isBelowPrunedBoundary(64, 65))
	require.False(t, isBelowPrunedBoundary(^uint64(0), ^uint64(0)))
}

func TestAddChainSegmentDoesNotExcludeLifecycleReaders(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(block, block1, int(clparams.Phase0Version)))
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))
	graph, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	f := graph.(*forkGraphDisk)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	f.lifecycleMu.RLock()
	lifecycleReaderHeld := true
	t.Cleanup(func() {
		if lifecycleReaderHeld {
			f.lifecycleMu.RUnlock()
		}
	})

	done := make(chan struct {
		result ChainSegmentInsertionResult
		err    error
	}, 1)
	go func() {
		_, result, err := f.AddChainSegment(block, true)
		done <- struct {
			result ChainSegmentInsertionResult
			err    error
		}{result: result, err: err}
	}()

	statePublished := make(chan struct{})
	go func() {
		for {
			f.currentStateMu.RLock()
			published := f.currentStateBlockRoot == common.Hash(blockRoot)
			f.currentStateMu.RUnlock()
			if published {
				close(statePublished)
				return
			}
			runtime.Gosched()
		}
	}()
	select {
	case <-statePublished:
	case <-time.After(time.Second):
		t.Fatal("block insertion did not reach final lifecycle publication")
	}
	select {
	case result := <-done:
		t.Fatalf("block insertion completed before lifecycle reader released: %v", result)
	default:
	}

	f.lifecycleMu.RUnlock()
	lifecycleReaderHeld = false
	result := <-done
	require.NoError(t, result.err)
	require.Equal(t, Success, result.result)
	_, headerFound := f.GetHeader(common.Hash(blockRoot))
	require.True(t, headerFound)
}
