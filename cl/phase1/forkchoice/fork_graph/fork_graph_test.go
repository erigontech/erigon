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
	"bytes"
	_ "embed"
	"encoding/binary"
	"errors"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/golang/snappy"
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

var errTestEnvelopeIO = errors.New("test envelope I/O error")

type envelopeCloseErrorFile struct{ afero.File }

func (f envelopeCloseErrorFile) Close() error {
	_ = f.File.Close()
	return errTestEnvelopeIO
}

type envelopeCloseErrorFs struct{ afero.Fs }

func (f envelopeCloseErrorFs) Open(name string) (afero.File, error) {
	file, err := f.Fs.Open(name)
	if err != nil {
		return nil, err
	}
	return envelopeCloseErrorFile{File: file}, nil
}

type envelopeReadErrorFile struct{ afero.File }

func (envelopeReadErrorFile) Read([]byte) (int, error) { return 0, errTestEnvelopeIO }

type envelopeReadErrorFs struct{ afero.Fs }

func (f envelopeReadErrorFs) Open(name string) (afero.File, error) {
	file, err := f.Fs.Open(name)
	if err != nil {
		return nil, err
	}
	return envelopeReadErrorFile{File: file}, nil
}

type envelopeOpenErrorFs struct{ afero.Fs }

func (f envelopeOpenErrorFs) Open(name string) (afero.File, error) {
	if strings.HasSuffix(name, ".envelope.snappy_ssz") {
		return nil, errTestEnvelopeIO
	}
	return f.Fs.Open(name)
}

type envelopeOpenCountingFs struct {
	afero.Fs
	opens atomic.Int32
}

func (f *envelopeOpenCountingFs) Open(name string) (afero.File, error) {
	if strings.HasSuffix(name, ".envelope.snappy_ssz") {
		f.opens.Add(1)
	}
	return f.Fs.Open(name)
}

type envelopeWriteFailureFs struct {
	afero.Fs
	stage  string
	closes *atomic.Int32
}

func (f envelopeWriteFailureFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	if f.stage == "open" && strings.HasSuffix(name, ".tmp") {
		return nil, errTestEnvelopeIO
	}
	file, err := f.Fs.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return envelopeWriteFailureFile{File: file, stage: f.stage, closes: f.closes}, nil
}

func (f envelopeWriteFailureFs) Rename(oldname, newname string) error {
	if f.stage == "rename" {
		return errTestEnvelopeIO
	}
	return f.Fs.Rename(oldname, newname)
}

type envelopeWriteFailureFile struct {
	afero.File
	stage  string
	closes *atomic.Int32
}

type envelopeBlockingRenameFs struct {
	afero.Fs
	reached      chan struct{}
	release      chan struct{}
	pruneReached chan struct{}
	renameOnce   sync.Once
	statOnce     sync.Once
}

type envelopeRemoveFailureFs struct {
	afero.Fs
	suffix string
}

type envelopeBlockingPruneFs struct {
	afero.Fs
	firstReached  chan struct{}
	releaseFirst  chan struct{}
	secondReached chan struct{}
	releaseSecond chan struct{}
	removes       atomic.Int32
}

func (f envelopeRemoveFailureFs) Remove(name string) error {
	if strings.HasSuffix(name, f.suffix) {
		return errTestEnvelopeIO
	}
	return f.Fs.Remove(name)
}

func (f *envelopeBlockingPruneFs) Remove(name string) error {
	switch f.removes.Add(1) {
	case 1:
		close(f.firstReached)
		<-f.releaseFirst
	case 2:
		close(f.secondReached)
		<-f.releaseSecond
	}
	return f.Fs.Remove(name)
}

func (f *envelopeBlockingRenameFs) Rename(oldname, newname string) error {
	if strings.HasSuffix(oldname, ".tmp") {
		f.renameOnce.Do(func() { close(f.reached) })
		<-f.release
	}
	return f.Fs.Rename(oldname, newname)
}

func (f *envelopeBlockingRenameFs) Stat(name string) (os.FileInfo, error) {
	if f.pruneReached != nil && strings.HasSuffix(name, ".snappy_ssz") && !strings.HasSuffix(name, ".envelope.snappy_ssz") {
		f.statOnce.Do(func() { close(f.pruneReached) })
	}
	return f.Fs.Stat(name)
}

func (f envelopeWriteFailureFile) Write(p []byte) (int, error) {
	if f.stage == "write" {
		return 0, errTestEnvelopeIO
	}
	return f.File.Write(p)
}

func (f envelopeWriteFailureFile) Sync() error {
	if f.stage == "sync" {
		return errTestEnvelopeIO
	}
	return f.File.Sync()
}

func (f envelopeWriteFailureFile) Close() error {
	if f.closes != nil {
		f.closes.Add(1)
	}
	if f.stage == "close" {
		_ = f.File.Close()
		return errTestEnvelopeIO
	}
	return f.File.Close()
}

func waitEnvelopeTestSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func waitEnvelopeTestResult(t *testing.T, result <-chan error, name string) {
	t.Helper()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
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

func TestReadEnvelopeMarksCorruptFileInvalidWithoutDeletingIt(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	require.NoError(t, afero.WriteFile(fs, getEnvelopeFilename(root), []byte("truncated"), 0o644))

	_, err := f.ReadEnvelopeFromDisk(root)
	require.Error(t, err)
	require.False(t, f.HasEnvelope(root))
	exists, existsErr := afero.Exists(fs, getEnvelopeFilename(root))
	require.NoError(t, existsErr)
	require.True(t, exists)
}

func TestHasEnvelopeDoesNotTrustUnvalidatedDiskFile(t *testing.T) {
	baseFs := afero.NewMemMapFs()
	root := common.HexToHash("0x1234")
	writer := &forkGraphDisk{fs: baseFs, beaconCfg: &clparams.MainnetBeaconConfig}
	addEnvelopeTestBlock(writer, root, 1)
	require.NoError(t, writer.DumpEnvelopeOnDisk(root, testEnvelopeWithTransaction(root, []byte{1})))

	fs := &envelopeOpenCountingFs{Fs: baseFs}
	restarted := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	addEnvelopeTestBlock(restarted, root, 1)
	restarted.stateDumpLock.Lock()
	result := make(chan bool, 1)
	go func() { result <- restarted.HasEnvelope(root) }()
	select {
	case hasEnvelope := <-result:
		require.False(t, hasEnvelope)
	case <-time.After(time.Second):
		restarted.stateDumpLock.Unlock()
		t.Fatal("HasEnvelope waited for state disk I/O")
	}
	restarted.stateDumpLock.Unlock()
	require.Zero(t, fs.opens.Load())

	_, err := restarted.ReadEnvelopeFromDisk(root)
	require.NoError(t, err)
	require.Equal(t, int32(1), fs.opens.Load())
	require.True(t, restarted.HasEnvelope(root))
}

func TestReadEnvelopeRemovesUnsupportedSnappyFrames(t *testing.T) {
	streamIdentifier := []byte{0xff, 0x06, 0x00, 0x00, 's', 'N', 'a', 'P', 'p', 'Y'}
	frame := append(append([]byte{}, streamIdentifier...), 0x02, 0x00, 0x00, 0x00)
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	require.NoError(t, afero.WriteFile(fs, getEnvelopeFilename(root), frame, 0o644))

	_, err := f.ReadEnvelopeFromDisk(root)
	require.ErrorIs(t, err, snappy.ErrUnsupported)
	require.False(t, f.HasEnvelope(root))
}

func TestEnvelopeReadClassifiesSnappyStructuralErrors(t *testing.T) {
	require.True(t, isCorruptEnvelopeReadError(snappy.ErrCorrupt, nil))
	require.True(t, isCorruptEnvelopeReadError(snappy.ErrUnsupported, nil))
	require.True(t, isCorruptEnvelopeReadError(snappy.ErrTooLarge, nil))
	require.False(t, isCorruptEnvelopeReadError(errTestEnvelopeIO, errTestEnvelopeIO))
}

func TestDumpEnvelopeAtomicallyPersistsReadableFile(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)

	require.NoError(t, f.DumpEnvelopeOnDisk(root, testEnvelopeWithTransaction(root, []byte{1, 2, 3})))
	tempExists, err := afero.Exists(fs, getEnvelopeFilename(root)+".tmp")
	require.NoError(t, err)
	require.False(t, tempExists)
	persisted, err := f.ReadEnvelopeFromDisk(root)
	require.NoError(t, err)
	require.Equal(t, root, persisted.Message.BeaconBlockRoot)
}

func TestDumpEnvelopeRejectsUnsupportedVersion(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	version := clparams.GloasVersion + 1
	envelope := testEnvelopeWithVersion(root, []byte{1, 2, 3}, version)

	require.ErrorContains(t, f.DumpEnvelopeOnDisk(root, envelope), "unsupported execution payload envelope consensus version")
}

func TestReadEnvelopeRejectsUnsupportedFramingVersion(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	encoded, err := testEnvelopeWithTransaction(root, []byte{1}).EncodeSSZ(nil)
	require.NoError(t, err)
	writeEnvelopeTestFile(t, fs, root, clparams.GloasVersion+1, encoded)

	_, err = f.ReadEnvelopeFromDisk(root)
	require.ErrorContains(t, err, "unsupported execution payload envelope consensus version")
	require.False(t, f.HasEnvelope(root))
}

func TestDumpEnvelopeRejectsMismatchedNestedVersions(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	envelope := testEnvelopeWithVersion(root, []byte{1}, clparams.GloasVersion+1)
	envelope.Message.ExecutionRequests = cltypes.NewExecutionRequestsWithVersion(&clparams.MainnetBeaconConfig, clparams.GloasVersion)

	require.ErrorContains(t, f.DumpEnvelopeOnDisk(root, envelope), "versions differ")
	require.False(t, f.HasEnvelope(root))
}

func TestDumpEnvelopeRejectsMismatchedRoot(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	rootA := common.HexToHash("0xa")
	rootB := common.HexToHash("0xb")
	addEnvelopeTestBlock(f, rootA, 1)

	require.Error(t, f.DumpEnvelopeOnDisk(rootA, testEnvelopeWithTransaction(rootB, []byte{1})))
	require.False(t, f.HasEnvelope(rootA))
}

func TestReadEnvelopeRejectsMismatchedRoot(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	rootA := common.HexToHash("0xa")
	rootB := common.HexToHash("0xb")
	addEnvelopeTestBlock(f, rootA, 1)
	addEnvelopeTestBlock(f, rootB, 2)
	require.NoError(t, f.DumpEnvelopeOnDisk(rootA, testEnvelopeWithTransaction(rootA, []byte{1})))
	require.NoError(t, fs.Rename(getEnvelopeFilename(rootA), getEnvelopeFilename(rootB)))

	_, err := f.ReadEnvelopeFromDisk(rootB)
	require.Error(t, err)
	require.False(t, f.HasEnvelope(rootB))
	exists, existsErr := afero.Exists(fs, getEnvelopeFilename(rootB))
	require.NoError(t, existsErr)
	require.True(t, exists)
}

func TestReadEnvelopeRejectsLengthAboveGossipLimit(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	var compressed bytes.Buffer
	writer := snappy.NewBufferedWriter(&compressed)
	length := make([]byte, 8)
	binary.BigEndian.PutUint64(length, clparams.MaxChunkSize+1)
	_, err := writer.Write([]byte{byte(clparams.GloasVersion)})
	require.NoError(t, err)
	_, err = writer.Write(length)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, afero.WriteFile(fs, getEnvelopeFilename(root), compressed.Bytes(), 0o644))

	_, err = f.ReadEnvelopeFromDisk(root)
	require.ErrorContains(t, err, "exceeds max")
	require.False(t, f.HasEnvelope(root))
}

func TestReadEnvelopeRejectsNonCanonicalSSZ(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	encoded, err := testEnvelopeWithTransaction(root, []byte{1}).EncodeSSZ(nil)
	require.NoError(t, err)
	messageOffset := binary.LittleEndian.Uint32(encoded)
	binary.LittleEndian.PutUint32(encoded, messageOffset+1)
	encoded = append(encoded[:messageOffset], append([]byte{0}, encoded[messageOffset:]...)...)
	writeEnvelopeTestFile(t, fs, root, clparams.GloasVersion, encoded)

	_, err = f.ReadEnvelopeFromDisk(root)
	require.Error(t, err)
	require.False(t, f.HasEnvelope(root))
}

func TestReadEnvelopeRejectsTrailingFileData(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	envelope := testEnvelopeWithTransaction(root, []byte{1})
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)

	var compressed bytes.Buffer
	writer := snappy.NewBufferedWriter(&compressed)
	_, err = writer.Write([]byte{byte(clparams.GloasVersion)})
	require.NoError(t, err)
	length := make([]byte, 8)
	binary.BigEndian.PutUint64(length, uint64(len(encoded)))
	_, err = writer.Write(length)
	require.NoError(t, err)
	_, err = writer.Write(encoded)
	require.NoError(t, err)
	_, err = writer.Write([]byte{1})
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, afero.WriteFile(fs, getEnvelopeFilename(root), compressed.Bytes(), 0o644))

	_, err = f.ReadEnvelopeFromDisk(root)
	require.ErrorContains(t, err, "trailing data")
	require.False(t, f.HasEnvelope(root))
	_, invalid := f.invalidEnvelopes.Load(root)
	require.True(t, invalid)
}

func TestReadEnvelopeValidatesDecodedEnvelopeAgainstConfig(t *testing.T) {
	fs := afero.NewMemMapFs()
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxWithdrawalRequestsPerPayload = 1
	f := &forkGraphDisk{fs: fs, beaconCfg: &cfg}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	envelope := testEnvelopeWithTransaction(root, []byte{1})
	for range 17 {
		envelope.Message.ExecutionRequests.Withdrawals.Append(&solid.WithdrawalRequest{})
	}
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	writeEnvelopeTestFile(t, fs, root, clparams.GloasVersion, encoded)

	_, err = f.ReadEnvelopeFromDisk(root)
	require.ErrorContains(t, err, "withdrawals: list has 17 elements, max 1")
	require.False(t, f.HasEnvelope(root))
}

func TestReadEnvelopeRejectsRequestsPastConsensusLimitWithinDecoderGuard(t *testing.T) {
	fs := afero.NewMemMapFs()
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxWithdrawalRequestsPerPayload = 1
	f := &forkGraphDisk{fs: fs, beaconCfg: &cfg}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	envelope := testEnvelopeWithTransaction(root, []byte{1})
	envelope.Message.ExecutionRequests.Withdrawals.Append(&solid.WithdrawalRequest{})
	envelope.Message.ExecutionRequests.Withdrawals.Append(&solid.WithdrawalRequest{})
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	writeEnvelopeTestFile(t, fs, root, clparams.GloasVersion, encoded)

	_, err = f.ReadEnvelopeFromDisk(root)
	require.ErrorContains(t, err, "withdrawals")
	require.False(t, f.HasEnvelope(root))
	_, invalid := f.invalidEnvelopes.Load(root)
	require.True(t, invalid)
}

func TestReadEnvelopeRejectsKnownInvalidFileWithoutOpening(t *testing.T) {
	baseFs := afero.NewMemMapFs()
	fs := &envelopeWriteFailureFs{Fs: baseFs, stage: "open"}
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	f.invalidEnvelopes.Store(root, struct{}{})

	_, err := f.ReadEnvelopeFromDisk(root)
	require.ErrorContains(t, err, "known invalid")
	require.NotErrorIs(t, err, errTestEnvelopeIO)
}

func TestDumpEnvelopeRejectsLengthAboveGossipLimit(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)

	err := f.DumpEnvelopeOnDisk(root, testEnvelopeWithTransaction(root, make([]byte, clparams.MaxChunkSize)))
	require.ErrorContains(t, err, "exceeds max")
	require.False(t, f.HasEnvelope(root))
}

func TestDumpEnvelopeRejectsIncompleteInput(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	validMessage := testEnvelopeWithTransaction(root, nil).Message
	wrongPayloadVersion := testEnvelopeWithTransaction(root, nil).Message
	wrongPayloadVersion.Payload = testEnvelopeWithVersion(root, nil, clparams.DenebVersion).Message.Payload
	wrongPayloadVersion.Payload.BlockAccessList = solid.NewByteListSSZ(clparams.MainnetBeaconConfig.MaxBytesPerTransaction)
	wrongRequestsVersion := testEnvelopeWithTransaction(root, nil).Message
	wrongRequestsVersion.ExecutionRequests = cltypes.NewExecutionRequestsWithVersion(&clparams.MainnetBeaconConfig, clparams.ElectraVersion)
	zeroRequests := testEnvelopeWithTransaction(root, nil).Message
	zeroRequests.ExecutionRequests = &cltypes.ExecutionRequests{}

	for _, tt := range []struct {
		name      string
		envelope  *cltypes.SignedExecutionPayloadEnvelope
		wantError string
	}{
		{name: "nil envelope", wantError: "nil execution payload envelope"},
		{name: "nil message", envelope: &cltypes.SignedExecutionPayloadEnvelope{}, wantError: "nil execution payload envelope message"},
		{name: "nil payload", envelope: &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{ExecutionRequests: validMessage.ExecutionRequests}}, wantError: "nil payload"},
		{name: "nil execution requests", envelope: &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{Payload: validMessage.Payload}}, wantError: "nil execution requests"},
		{name: "wrong payload version", envelope: &cltypes.SignedExecutionPayloadEnvelope{Message: wrongPayloadVersion}, wantError: "execution payload version 4 predates Gloas"},
		{name: "wrong requests version", envelope: &cltypes.SignedExecutionPayloadEnvelope{Message: wrongRequestsVersion}, wantError: "execution requests version 5 predates Gloas"},
		{name: "uninitialized requests", envelope: &cltypes.SignedExecutionPayloadEnvelope{Message: zeroRequests}, wantError: "execution requests version 5 predates Gloas"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.ErrorContains(t, f.DumpEnvelopeOnDisk(root, tt.envelope), tt.wantError)
		})
	}
}

func TestDumpEnvelopeRejectsNilNestedInput(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*cltypes.ExecutionPayloadEnvelope)
		wantError string
	}{
		{name: "payload extra data", mutate: func(e *cltypes.ExecutionPayloadEnvelope) { e.Payload.Extra = nil }, wantError: "nil extra data"},
		{name: "payload transactions", mutate: func(e *cltypes.ExecutionPayloadEnvelope) { e.Payload.Transactions = nil }, wantError: "nil transactions"},
		{name: "payload withdrawals", mutate: func(e *cltypes.ExecutionPayloadEnvelope) { e.Payload.Withdrawals = nil }, wantError: "nil withdrawals"},
		{name: "payload block access list", mutate: func(e *cltypes.ExecutionPayloadEnvelope) { e.Payload.BlockAccessList = nil }, wantError: "nil block access list"},
		{name: "deposit requests", mutate: func(e *cltypes.ExecutionPayloadEnvelope) { e.ExecutionRequests.Deposits = nil }, wantError: "nil deposit requests"},
		{name: "withdrawal requests", mutate: func(e *cltypes.ExecutionPayloadEnvelope) { e.ExecutionRequests.Withdrawals = nil }, wantError: "nil withdrawal requests"},
		{name: "consolidation requests", mutate: func(e *cltypes.ExecutionPayloadEnvelope) { e.ExecutionRequests.Consolidations = nil }, wantError: "nil consolidation requests"},
		{name: "builder deposit requests", mutate: func(e *cltypes.ExecutionPayloadEnvelope) { e.ExecutionRequests.BuilderDeposits = nil }, wantError: "nil builder deposit requests"},
		{name: "builder exit requests", mutate: func(e *cltypes.ExecutionPayloadEnvelope) { e.ExecutionRequests.BuilderExits = nil }, wantError: "nil builder exit requests"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
			root := common.HexToHash("0x1234")
			addEnvelopeTestBlock(f, root, 1)
			envelope := testEnvelopeWithTransaction(root, []byte{1})
			tt.mutate(envelope.Message)

			require.ErrorContains(t, f.DumpEnvelopeOnDisk(root, envelope), tt.wantError)
			require.False(t, f.HasEnvelope(root))
		})
	}
}

func TestDumpEnvelopeAcceptsInitializedEmptyNestedCollections(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	envelope := testEnvelopeWithTransaction(root, nil)
	envelope.Message.Payload.Transactions = &solid.TransactionsSSZ{}

	require.NoError(t, f.DumpEnvelopeOnDisk(root, envelope))
	require.True(t, f.HasEnvelope(root))
}

func TestDumpEnvelopeRejectsNilNestedListMembers(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*cltypes.ExecutionPayloadEnvelope)
		wantError string
	}{
		{name: "payload withdrawals", mutate: func(e *cltypes.ExecutionPayloadEnvelope) { e.Payload.Withdrawals.Append(nil) }, wantError: "nil withdrawal at index 0"},
		{name: "deposit requests", mutate: func(e *cltypes.ExecutionPayloadEnvelope) { e.ExecutionRequests.Deposits.Append(nil) }, wantError: "nil deposit request at index 0"},
		{name: "withdrawal requests", mutate: func(e *cltypes.ExecutionPayloadEnvelope) { e.ExecutionRequests.Withdrawals.Append(nil) }, wantError: "nil withdrawal request at index 0"},
		{name: "consolidation requests", mutate: func(e *cltypes.ExecutionPayloadEnvelope) { e.ExecutionRequests.Consolidations.Append(nil) }, wantError: "nil consolidation request at index 0"},
		{name: "builder deposit requests", mutate: func(e *cltypes.ExecutionPayloadEnvelope) { e.ExecutionRequests.BuilderDeposits.Append(nil) }, wantError: "nil builder deposit request at index 0"},
		{name: "builder exit requests", mutate: func(e *cltypes.ExecutionPayloadEnvelope) { e.ExecutionRequests.BuilderExits.Append(nil) }, wantError: "nil builder exit request at index 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
			root := common.HexToHash("0x1234")
			addEnvelopeTestBlock(f, root, 1)
			envelope := testEnvelopeWithTransaction(root, []byte{1})
			tt.mutate(envelope.Message)

			require.ErrorContains(t, f.DumpEnvelopeOnDisk(root, envelope), tt.wantError)
			require.False(t, f.HasEnvelope(root))
			exists, err := afero.Exists(fs, getEnvelopeFilename(root))
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
}

func TestDumpEnvelopeAllowsAnchorRoot(t *testing.T) {
	fs := afero.NewMemMapFs()
	root := common.HexToHash("0x1234")
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig, anchorRoot: root}

	require.NoError(t, f.DumpEnvelopeOnDisk(root, testEnvelopeWithTransaction(root, []byte{1})))
}

func TestDumpEnvelopeFailurePreservesExistingFinal(t *testing.T) {
	for _, stage := range []string{"open", "write", "sync", "close", "rename"} {
		t.Run(stage, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
			root := common.HexToHash("0x1234")
			addEnvelopeTestBlock(f, root, 1)
			require.NoError(t, f.DumpEnvelopeOnDisk(root, testEnvelopeWithTransaction(root, []byte{1, 2, 3})))

			var closes atomic.Int32
			f.fs = envelopeWriteFailureFs{Fs: fs, stage: stage, closes: &closes}
			require.ErrorIs(t, f.DumpEnvelopeOnDisk(root, testEnvelopeWithTransaction(root, []byte{9, 8, 7})), errTestEnvelopeIO)
			if stage == "close" {
				require.Equal(t, int32(1), closes.Load())
			}
			f.fs = fs

			tempExists, err := afero.Exists(fs, getEnvelopeFilename(root)+".tmp")
			require.NoError(t, err)
			require.False(t, tempExists)
			persisted, err := f.ReadEnvelopeFromDisk(root)
			require.NoError(t, err)
			require.Equal(t, [][]byte{{1, 2, 3}}, persisted.Message.Payload.Transactions.UnderlyngReference())
		})
	}
}

func TestPruneDoesNotRaceEnvelopeReplacement(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	oldRoot := common.HexToHash("0x1")
	newRoot := common.HexToHash("0x2")
	for root, slot := range map[common.Hash]uint64{oldRoot: 1, newRoot: 3} {
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
		block.Block.Slot = slot
		f.blocks.Store(root, block)
		require.NoError(t, afero.WriteFile(fs, getBeaconStateFilename(root), []byte{1}, 0o644))
	}
	require.NoError(t, f.DumpEnvelopeOnDisk(oldRoot, testEnvelopeWithTransaction(oldRoot, []byte{1})))
	require.NoError(t, afero.WriteFile(fs, getEnvelopeFilename(oldRoot)+".tmp", []byte("stale"), 0o644))

	blockingFs := &envelopeBlockingRenameFs{
		Fs:           fs,
		reached:      make(chan struct{}),
		release:      make(chan struct{}),
		pruneReached: make(chan struct{}),
	}
	f.fs = blockingFs
	dumpDone := make(chan error, 1)
	go func() {
		dumpDone <- f.DumpEnvelopeOnDisk(oldRoot, testEnvelopeWithTransaction(oldRoot, []byte{2}))
	}()
	waitEnvelopeTestSignal(t, blockingFs.reached, "envelope rename")
	pruneDone := make(chan error, 1)
	go func() { pruneDone <- f.Prune(2) }()
	waitEnvelopeTestSignal(t, blockingFs.pruneReached, "prune state scan")
	close(blockingFs.release)
	waitEnvelopeTestResult(t, dumpDone, "envelope replacement")
	waitEnvelopeTestResult(t, pruneDone, "prune completion")

	finalExists, err := afero.Exists(fs, getEnvelopeFilename(oldRoot))
	require.NoError(t, err)
	require.False(t, finalExists)
	tempExists, err := afero.Exists(fs, getEnvelopeFilename(oldRoot)+".tmp")
	require.NoError(t, err)
	require.False(t, tempExists)
	require.False(t, f.HasEnvelope(oldRoot))
}

func TestDumpEnvelopeRejectsPrunedRoot(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	oldRoot := common.HexToHash("0x1")
	newRoot := common.HexToHash("0x2")
	addEnvelopeTestBlock(f, oldRoot, 1)
	addEnvelopeTestBlock(f, newRoot, 3)
	require.NoError(t, afero.WriteFile(fs, getBeaconStateFilename(oldRoot), []byte{1}, 0o644))
	require.NoError(t, afero.WriteFile(fs, getBeaconStateFilename(newRoot), []byte{1}, 0o644))
	require.NoError(t, f.Prune(2))

	require.Error(t, f.DumpEnvelopeOnDisk(oldRoot, testEnvelopeWithTransaction(oldRoot, []byte{1})))
	exists, err := afero.Exists(fs, getEnvelopeFilename(oldRoot))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestDumpEnvelopeDoesNotApplyLegacyTransactionLimit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxTransactionsPerPayload = 1
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &cfg}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	envelope := cltypes.NewExecutionPayloadEnvelope(&cfg)
	envelope.BeaconBlockRoot = root
	envelope.Payload.Extra = solid.NewExtraData()
	envelope.Payload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{{1}, {2}})
	envelope.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	envelope.Payload.BlockAccessList = solid.NewByteListSSZ(cfg.MaxBytesPerTransaction)

	require.NoError(t, f.DumpEnvelopeOnDisk(root, &cltypes.SignedExecutionPayloadEnvelope{Message: envelope}))
	decoded, err := f.ReadEnvelopeFromDisk(root)
	require.NoError(t, err)
	require.Len(t, decoded.Message.Payload.Transactions.UnderlyngReference(), 2)
}

func TestDumpEnvelopeAcceptsProtocolValidProgressiveDepositCount(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	envelope := testEnvelopeWithTransaction(root, nil)
	envelope.Message.ExecutionRequests.Deposits = solid.NewStaticProgressiveListSSZ[*solid.DepositRequest](8193, solid.SizeDepositRequest)
	for range 16_385 {
		envelope.Message.ExecutionRequests.Deposits.Append(&solid.DepositRequest{})
	}

	err := f.DumpEnvelopeOnDisk(root, envelope)
	require.NoError(t, err)
	exists, existsErr := afero.Exists(fs, getEnvelopeFilename(root))
	require.NoError(t, existsErr)
	require.True(t, exists)
	decoded, err := f.ReadEnvelopeFromDisk(root)
	require.NoError(t, err)
	require.Equal(t, 16_385, decoded.Message.ExecutionRequests.Deposits.Len())
}

func TestDumpEnvelopeRejectsProgressiveTransactionResourceCount(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	envelope := testEnvelopeWithTransaction(root, nil)
	transactionCount := int(clparams.MainnetBeaconConfig.MaxTransactionsPerPayload) + 1
	envelope.Message.Payload.Transactions = solid.NewTransactionsSSZFromTransactions(make([][]byte, transactionCount))
	require.LessOrEqual(t, uint64(envelope.EncodingSizeSSZ()), clparams.MaxChunkSize)
	require.NoError(t, envelope.ValidateForConfig(&clparams.MainnetBeaconConfig))
	require.ErrorContains(t, envelope.ValidateForPersistence(&clparams.MainnetBeaconConfig), "too many transactions")
	require.ErrorContains(t, f.DumpEnvelopeOnDisk(root, envelope), "too many transactions")
}

func TestDumpEnvelopeRejectsRequestsPastConsensusLimitWithinDecoderGuard(t *testing.T) {
	fs := afero.NewMemMapFs()
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxWithdrawalRequestsPerPayload = 1
	f := &forkGraphDisk{fs: fs, beaconCfg: &cfg}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	envelope := testEnvelopeWithTransaction(root, []byte{1})
	envelope.Message.ExecutionRequests.Withdrawals.Append(&solid.WithdrawalRequest{})
	envelope.Message.ExecutionRequests.Withdrawals.Append(&solid.WithdrawalRequest{})

	err := f.DumpEnvelopeOnDisk(root, envelope)
	require.ErrorContains(t, err, "withdrawals")
	require.False(t, f.HasEnvelope(root))
}

func TestPruneReportsEnvelopeRemovalFailureWithoutRecachingRoot(t *testing.T) {
	baseFs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: baseFs, beaconCfg: &clparams.MainnetBeaconConfig}
	oldRoot := common.HexToHash("0x1")
	newRoot := common.HexToHash("0x2")
	addEnvelopeTestBlock(f, oldRoot, 1)
	addEnvelopeTestBlock(f, newRoot, 3)
	require.NoError(t, afero.WriteFile(baseFs, getBeaconStateFilename(oldRoot), []byte{1}, 0o644))
	require.NoError(t, afero.WriteFile(baseFs, getBeaconStateFilename(newRoot), []byte{1}, 0o644))
	require.NoError(t, f.DumpEnvelopeOnDisk(oldRoot, testEnvelopeWithTransaction(oldRoot, []byte{1})))
	f.fs = envelopeRemoveFailureFs{Fs: baseFs, suffix: ".envelope.snappy_ssz"}

	require.ErrorIs(t, f.Prune(2), errTestEnvelopeIO)
	require.False(t, f.HasEnvelope(oldRoot))
	_, err := f.ReadEnvelopeFromDisk(oldRoot)
	require.Error(t, err)
	_, exists := f.blocks.Load(oldRoot)
	require.False(t, exists)
}

func TestPruneAllowsUnrelatedEnvelopeIOBetweenRoots(t *testing.T) {
	baseFs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: baseFs, beaconCfg: &clparams.MainnetBeaconConfig}
	oldRootA := common.HexToHash("0x1")
	oldRootB := common.HexToHash("0x2")
	retainedRoot := common.HexToHash("0x3")
	for root, slot := range map[common.Hash]uint64{oldRootA: 1, oldRootB: 2, retainedRoot: 4} {
		addEnvelopeTestBlock(f, root, slot)
		require.NoError(t, afero.WriteFile(baseFs, getBeaconStateFilename(root), []byte{1}, 0o644))
	}
	blockingFs := &envelopeBlockingPruneFs{
		Fs:            baseFs,
		firstReached:  make(chan struct{}),
		releaseFirst:  make(chan struct{}),
		secondReached: make(chan struct{}),
		releaseSecond: make(chan struct{}),
	}
	f.fs = blockingFs
	pruneDone := make(chan error, 1)
	go func() { pruneDone <- f.Prune(3) }()
	waitEnvelopeTestSignal(t, blockingFs.firstReached, "first envelope removal")

	dumpDone := make(chan error, 1)
	go func() {
		dumpDone <- f.DumpEnvelopeOnDisk(retainedRoot, testEnvelopeWithTransaction(retainedRoot, []byte{1}))
	}()
	close(blockingFs.releaseFirst)
	waitEnvelopeTestSignal(t, blockingFs.secondReached, "second envelope removal")
	waitEnvelopeTestResult(t, dumpDone, "unrelated envelope dump")
	close(blockingFs.releaseSecond)
	waitEnvelopeTestResult(t, pruneDone, "prune completion")
}

func TestReadEnvelopeOwnsDecodedTransactions(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	rootA := common.HexToHash("0xa")
	rootB := common.HexToHash("0xb")
	addEnvelopeTestBlock(f, rootA, 1)
	addEnvelopeTestBlock(f, rootB, 2)
	require.NoError(t, f.DumpEnvelopeOnDisk(rootA, testEnvelopeWithTransaction(rootA, []byte{1, 2, 3})))

	persistedA, err := f.ReadEnvelopeFromDisk(rootA)
	require.NoError(t, err)
	require.NoError(t, f.DumpEnvelopeOnDisk(rootB, testEnvelopeWithTransaction(rootB, []byte{9, 8, 7})))
	_, err = f.ReadEnvelopeFromDisk(rootB)
	require.NoError(t, err)
	require.Equal(t, [][]byte{{1, 2, 3}}, persistedA.Message.Payload.Transactions.UnderlyngReference())
}

func TestReadEnvelopeReusesSharedBufferWithoutAliasingTransactions(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	rootA := common.HexToHash("0xa")
	rootB := common.HexToHash("0xb")
	addEnvelopeTestBlock(f, rootA, 1)
	addEnvelopeTestBlock(f, rootB, 2)
	envelopeA := testEnvelopeWithTransaction(rootA, []byte{1, 2, 3})
	require.NoError(t, f.DumpEnvelopeOnDisk(rootA, envelopeA))
	require.NoError(t, f.DumpEnvelopeOnDisk(rootB, testEnvelopeWithTransaction(rootB, []byte{9, 8, 7})))
	encodedA, err := envelopeA.EncodeSSZ(nil)
	require.NoError(t, err)
	f.sszBuffer = bytes.Repeat([]byte{0xff}, len(encodedA))
	sharedBuffer := &f.sszBuffer[0]

	persistedA, err := f.ReadEnvelopeFromDisk(rootA)
	require.NoError(t, err)
	require.Same(t, sharedBuffer, &f.sszBuffer[0])
	require.Equal(t, encodedA, f.sszBuffer)
	_, err = f.ReadEnvelopeFromDisk(rootB)
	require.NoError(t, err)
	require.Equal(t, [][]byte{{1, 2, 3}}, persistedA.Message.Payload.Transactions.UnderlyngReference())
}

func TestReadEnvelopeTransactionsPreserveProgressiveDecodeLimits(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxTransactionsPerPayload = 1
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &cfg}
	root := common.HexToHash("0xa")
	addEnvelopeTestBlock(f, root, 1)
	require.NoError(t, f.DumpEnvelopeOnDisk(root, testEnvelopeWithTransaction(root, []byte{1})))

	persisted, err := f.ReadEnvelopeFromDisk(root)
	require.NoError(t, err)
	overLimit, err := solid.NewTransactionsSSZFromTransactions([][]byte{{1}, {2}}).EncodeSSZ(nil)
	require.NoError(t, err)
	require.NoError(t, persisted.Message.Payload.Transactions.DecodeSSZ(overLimit, 0))
	require.Len(t, persisted.Message.Payload.Transactions.UnderlyngReference(), 2)
}

func TestReadEnvelopeTransactionsDoNotRaceWithDump(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	rootA := common.HexToHash("0xa")
	rootB := common.HexToHash("0xb")
	addEnvelopeTestBlock(f, rootA, 1)
	addEnvelopeTestBlock(f, rootB, 2)
	require.NoError(t, f.DumpEnvelopeOnDisk(rootA, testEnvelopeWithTransaction(rootA, []byte{1, 2, 3})))
	persistedA, err := f.ReadEnvelopeFromDisk(rootA)
	require.NoError(t, err)
	envelopeB := testEnvelopeWithTransaction(rootB, []byte{9, 8, 7})

	start := make(chan struct{})
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		<-start
		for range 100 {
			if err := f.DumpEnvelopeOnDisk(rootB, envelopeB); err != nil {
				errCh <- err
				return
			}
		}
	})
	close(start)
	var observed uint64
	for range 100 {
		observed += uint64(persistedA.Message.Payload.Transactions.UnderlyngReference()[0][0])
	}
	wg.Wait()
	require.Equal(t, uint64(100), observed)
	require.Equal(t, [][]byte{{1, 2, 3}}, persistedA.Message.Payload.Transactions.UnderlyngReference())
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
}

func TestReadEnvelopeCloseErrorKeepsDecodedFile(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	require.NoError(t, f.DumpEnvelopeOnDisk(root, testEnvelopeWithTransaction(root, []byte{1})))
	f.fs = envelopeCloseErrorFs{Fs: fs}

	envelope, err := f.ReadEnvelopeFromDisk(root)
	require.NoError(t, err)
	require.NotNil(t, envelope)
	require.True(t, f.HasEnvelope(root))
}

func TestReadEnvelopeTransientReadErrorKeepsFile(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	require.NoError(t, f.DumpEnvelopeOnDisk(root, testEnvelopeWithTransaction(root, []byte{1})))
	f.fs = envelopeReadErrorFs{Fs: fs}

	_, err := f.ReadEnvelopeFromDisk(root)
	require.ErrorIs(t, err, errTestEnvelopeIO)
	require.True(t, f.HasEnvelope(root))
	exists, existsErr := afero.Exists(fs, getEnvelopeFilename(root))
	require.NoError(t, existsErr)
	require.True(t, exists)
}

func TestReadEnvelopeTransientOpenErrorKeepsTrustedCache(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	require.NoError(t, f.DumpEnvelopeOnDisk(root, testEnvelopeWithTransaction(root, []byte{1})))
	f.fs = envelopeOpenErrorFs{Fs: fs}

	_, err := f.ReadEnvelopeFromDisk(root)
	require.ErrorIs(t, err, errTestEnvelopeIO)
	require.True(t, f.HasEnvelope(root))
}

func TestReadEnvelopeStructuralCorruptionEvictsTrustedCache(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	require.NoError(t, f.DumpEnvelopeOnDisk(root, testEnvelopeWithTransaction(root, []byte{1})))
	require.NoError(t, afero.WriteFile(fs, getEnvelopeFilename(root), []byte("corrupt"), 0o644))

	_, err := f.ReadEnvelopeFromDisk(root)
	require.Error(t, err)
	require.False(t, f.HasEnvelope(root))
	_, invalid := f.invalidEnvelopes.Load(root)
	require.True(t, invalid)
}

func testEnvelopeWithTransaction(root common.Hash, transaction []byte) *cltypes.SignedExecutionPayloadEnvelope {
	return testEnvelopeWithVersion(root, transaction, clparams.GloasVersion)
}

func testEnvelopeWithVersion(root common.Hash, transaction []byte, version clparams.StateVersion) *cltypes.SignedExecutionPayloadEnvelope {
	envelope := cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)
	envelope.BeaconBlockRoot = root
	envelope.Payload = cltypes.NewEth1Block(version, &clparams.MainnetBeaconConfig)
	envelope.Payload.Extra = solid.NewExtraData()
	envelope.Payload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{transaction})
	envelope.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)
	envelope.ExecutionRequests = cltypes.NewExecutionRequestsWithVersion(&clparams.MainnetBeaconConfig, version)
	return &cltypes.SignedExecutionPayloadEnvelope{Message: envelope}
}

func addEnvelopeTestBlock(f *forkGraphDisk, root common.Hash, slot uint64) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = slot
	f.blocks.Store(root, block)
}

func writeEnvelopeTestFile(t *testing.T, fs afero.Fs, root common.Hash, version clparams.StateVersion, encoded []byte) {
	t.Helper()
	var compressed bytes.Buffer
	writer := snappy.NewBufferedWriter(&compressed)
	_, err := writer.Write([]byte{byte(version)})
	require.NoError(t, err)
	length := make([]byte, 8)
	binary.BigEndian.PutUint64(length, uint64(len(encoded)))
	_, err = writer.Write(length)
	require.NoError(t, err)
	_, err = writer.Write(encoded)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, afero.WriteFile(fs, getEnvelopeFilename(root), compressed.Bytes(), 0o644))
}
