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
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/golang/snappy"
	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

type renameFailFS struct {
	afero.Fs
}

func (renameFailFS) Rename(string, string) error {
	return errors.New("injected rename failure")
}

type blockingStatFS struct {
	afero.Fs
	target  string
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type directorySyncTrackingFS struct {
	afero.Fs
	mu       sync.Mutex
	events   []string
	failNext bool
}

type directorySyncTrackingFile struct {
	afero.File
	fs *directorySyncTrackingFS
}

func (f *directorySyncTrackingFS) Open(name string) (afero.File, error) {
	file, err := f.Fs.Open(name)
	if err != nil || name != "." {
		return file, err
	}
	return &directorySyncTrackingFile{File: file, fs: f}, nil
}

func (f *directorySyncTrackingFS) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	if strings.HasSuffix(name, ".envelope.indices-pending") {
		f.record("marker")
	}
	return f.Fs.OpenFile(name, flag, perm)
}

func (f *directorySyncTrackingFS) Rename(oldname, newname string) error {
	f.record("rename")
	return f.Fs.Rename(oldname, newname)
}

func (f *directorySyncTrackingFS) Remove(name string) error {
	if strings.HasSuffix(name, ".envelope.indices-pending") {
		f.record("remove marker")
	}
	return f.Fs.Remove(name)
}

func (f *directorySyncTrackingFS) record(event string) {
	f.mu.Lock()
	f.events = append(f.events, event)
	f.mu.Unlock()
}

func (f *directorySyncTrackingFS) takeEvents() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	events := append([]string(nil), f.events...)
	f.events = nil
	return events
}

func (f *directorySyncTrackingFS) failNextSync() {
	f.mu.Lock()
	f.failNext = true
	f.mu.Unlock()
}

func (f *directorySyncTrackingFile) Sync() error {
	f.fs.mu.Lock()
	f.fs.events = append(f.fs.events, "sync directory")
	fail := f.fs.failNext
	f.fs.failNext = false
	f.fs.mu.Unlock()
	if fail {
		return errors.New("injected directory sync failure")
	}
	return f.File.Sync()
}

func (f *blockingStatFS) Stat(name string) (os.FileInfo, error) {
	if name == f.target {
		f.once.Do(func() { close(f.started) })
		<-f.release
	}
	return f.Fs.Stat(name)
}

//go:embed test_data/block_0xe2a37a22d208ebe969c50e9d44bb3f1f63c5404787b9c214a5f2f28fb9835feb.ssz_snappy
var block1 []byte

//go:embed test_data/block_0xbf1a9ba2d349f6b5a5095bff40bd103ae39177e36018fb1f589953b9eeb0ca9d.ssz_snappy
var block2 []byte

//go:embed test_data/anchor_state.ssz_snappy
var anchor []byte

func TestForkGraphInDisk(t *testing.T) {
	blockA, blockB, blockC := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion),
		cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion),
		cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(blockA, block1, int(clparams.Phase0Version)))
	require.NoError(t, utils.DecodeSSZSnappy(blockB, block2, int(clparams.Phase0Version)))
	require.NoError(t, utils.DecodeSSZSnappy(blockC, block2, int(clparams.Phase0Version)))
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))
	graph := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
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

func TestNewForkGraphDiskCachesAnchorStateRoot(t *testing.T) {
	for _, tc := range []struct {
		name         string
		stateSlot    uint64
		headerSlot   uint64
		headerRoot   common.Hash
		cachedRoot   common.Hash
		expectedRoot common.Hash
	}{
		{name: "skipped slot", stateSlot: 64, headerSlot: 63, headerRoot: common.Hash{1}},
		{name: "block slot", stateSlot: 64, headerSlot: 64},
		{name: "legacy block slot", stateSlot: 64, headerSlot: 64, headerRoot: common.Hash{1}, cachedRoot: common.Hash{1}},
		{name: "legacy block slot without cached root", stateSlot: 64, headerSlot: 64, headerRoot: common.Hash{2}, expectedRoot: common.Hash{2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			anchorState := state.New(&clparams.MainnetBeaconConfig)
			anchorState.SetVersion(clparams.GloasVersion)
			anchorState.SetSlot(tc.stateSlot)
			header := &cltypes.BeaconBlockHeader{Slot: tc.headerSlot, Root: tc.headerRoot}
			anchorState.SetLatestBlockHeader(header)
			expectedStateRoot, err := anchorState.HashSSZ()
			require.NoError(t, err)
			if tc.cachedRoot != (common.Hash{}) {
				expectedStateRoot = tc.cachedRoot
				anchorState.SetPreviousStateRoot(tc.cachedRoot)
			} else if tc.expectedRoot != (common.Hash{}) {
				expectedStateRoot = tc.expectedRoot
			}
			anchorRoot, err := anchorState.BlockRoot()
			require.NoError(t, err)

			graph := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{}).(*forkGraphDisk)

			require.Equal(t, common.Hash(expectedStateRoot), anchorState.PeekPreviousStateRoot())
			require.Equal(t, header.Root, anchorState.LatestBlockHeader().Root)
			persistedState, err := graph.readBeaconStateFromDisk(anchorRoot)
			require.NoError(t, err)
			require.Equal(t, common.Hash(expectedStateRoot), persistedState.PeekPreviousStateRoot())
		})
	}
}

func TestHasEnvelopeRejectsTruncatedFile(t *testing.T) {
	fs := afero.NewMemMapFs()
	root := common.Hash{1}
	require.NoError(t, afero.WriteFile(fs, getEnvelopeFilename(root), []byte{1, 2, 3}, 0o644))
	graph := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}

	require.False(t, graph.HasEnvelope(root))
	_, cached := graph.envelopeExists.Load(root)
	require.False(t, cached)
}

func TestDumpEnvelopeOnDiskKeepsPreviousFileWhenRenameFails(t *testing.T) {
	fs := afero.NewMemMapFs()
	root := common.Hash{1}
	graph := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	original := testExecutionPayloadEnvelope(root, common.Hash{2})
	require.NoError(t, graph.DumpEnvelopeOnDisk(root, original))

	graph.fs = renameFailFS{Fs: fs}
	replacement := testExecutionPayloadEnvelope(root, common.Hash{3})
	require.ErrorContains(t, graph.DumpEnvelopeOnDisk(root, replacement), "injected rename failure")

	stored, err := graph.ReadEnvelopeFromDisk(root)
	require.NoError(t, err)
	require.Equal(t, original.Message.Payload.BlockHash, stored.Message.Payload.BlockHash)
	exists, err := afero.Exists(fs, getEnvelopeFilename(root)+".tmp")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestDumpEnvelopeOnDiskWithBasePathFs(t *testing.T) {
	baseDir := t.TempDir()
	fs := afero.NewBasePathFs(afero.NewOsFs(), baseDir)
	root := common.Hash{1}
	graph := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}

	require.NoError(t, graph.DumpEnvelopeOnDisk(root, testExecutionPayloadEnvelope(root, common.Hash{2})))
	require.FileExists(t, filepath.Join(baseDir, getEnvelopeFilename(root)))
	require.FileExists(t, filepath.Join(baseDir, getEnvelopeIndexMarkerFilename(root)))
	matches, err := filepath.Glob(filepath.Join(baseDir, getEnvelopeFilename(root)+".tmp-*"))
	require.NoError(t, err)
	require.Empty(t, matches)

	require.NoError(t, graph.MarkEnvelopeIndicesCommitted(root))
	require.NoFileExists(t, filepath.Join(baseDir, getEnvelopeIndexMarkerFilename(root)))
}

func TestEnvelopeJournalSyncsDirectoryAtDurabilityBoundaries(t *testing.T) {
	fs := &directorySyncTrackingFS{Fs: afero.NewMemMapFs()}
	root := common.Hash{1}
	graph := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}

	publish, err := graph.PrepareEnvelopeOnDisk(root, testExecutionPayloadEnvelope(root, common.Hash{2}), false)
	require.NoError(t, err)
	require.Equal(t, []string{"marker", "sync directory"}, fs.takeEvents())

	require.NoError(t, publish())
	require.Equal(t, []string{"rename", "sync directory"}, fs.takeEvents())

	require.NoError(t, graph.MarkEnvelopeIndicesCommitted(root))
	require.Equal(t, []string{"remove marker", "sync directory"}, fs.takeEvents())
}

func TestEnvelopeJournalPropagatesDirectorySyncFailures(t *testing.T) {
	t.Run("prepare marker", func(t *testing.T) {
		fs := &directorySyncTrackingFS{Fs: afero.NewMemMapFs()}
		root := common.Hash{1}
		graph := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
		fs.failNextSync()

		_, err := graph.PrepareEnvelopeOnDisk(root, testExecutionPayloadEnvelope(root, common.Hash{2}), false)
		require.ErrorContains(t, err, "injected directory sync failure")
	})

	t.Run("publish rename", func(t *testing.T) {
		fs := &directorySyncTrackingFS{Fs: afero.NewMemMapFs()}
		root := common.Hash{1}
		graph := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
		publish, err := graph.PrepareEnvelopeOnDisk(root, testExecutionPayloadEnvelope(root, common.Hash{2}), false)
		require.NoError(t, err)
		fs.takeEvents()
		fs.failNextSync()

		err = publish()
		require.ErrorContains(t, err, "injected directory sync failure")
		_, cached := graph.envelopeExists.Load(root)
		require.False(t, cached)
	})

	t.Run("commit marker removal", func(t *testing.T) {
		fs := &directorySyncTrackingFS{Fs: afero.NewMemMapFs()}
		root := common.Hash{1}
		graph := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
		require.NoError(t, graph.DumpEnvelopeOnDisk(root, testExecutionPayloadEnvelope(root, common.Hash{2})))
		fs.takeEvents()
		fs.failNextSync()

		err := graph.MarkEnvelopeIndicesCommitted(root)
		require.ErrorContains(t, err, "injected directory sync failure")
	})
}

func TestEnvelopeJournalSyncsAbortedPublishCleanup(t *testing.T) {
	fs := &directorySyncTrackingFS{Fs: afero.NewMemMapFs()}
	root := common.Hash{1}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	graph := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	graph.blocks.Store(root, block)
	publish, err := graph.PrepareEnvelopeOnDisk(root, testExecutionPayloadEnvelope(root, common.Hash{2}), true)
	require.NoError(t, err)
	fs.takeEvents()
	graph.blocks.Delete(root)

	err = publish()
	require.ErrorContains(t, err, "pruned block")
	require.Equal(t, []string{"remove marker", "sync directory"}, fs.takeEvents())
	pendingRoots, pendingErr := graph.PendingEnvelopeIndexRoots()
	require.NoError(t, pendingErr)
	require.NotContains(t, pendingRoots, root)
}

func TestReplacementPreservesPreexistingRecoveryMarker(t *testing.T) {
	newGraph := func(t *testing.T) (*forkGraphDisk, *directorySyncTrackingFS, common.Hash) {
		t.Helper()
		fs := &directorySyncTrackingFS{Fs: afero.NewMemMapFs()}
		root := common.Hash{1}
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
		graph := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
		graph.blocks.Store(root, block)
		require.NoError(t, graph.DumpEnvelopeOnDisk(root, testExecutionPayloadEnvelope(root, common.Hash{2})))
		fs.takeEvents()
		return graph, fs, root
	}
	assertMarker := func(t *testing.T, graph *forkGraphDisk, root common.Hash) {
		t.Helper()
		pendingRoots, err := graph.PendingEnvelopeIndexRoots()
		require.NoError(t, err)
		require.Contains(t, pendingRoots, root)
	}

	t.Run("pruned before replacement publish", func(t *testing.T) {
		graph, fs, root := newGraph(t)
		publish, err := graph.PrepareEnvelopeOnDisk(root, testExecutionPayloadEnvelope(root, common.Hash{3}), true)
		require.NoError(t, err)
		graph.blocks.Delete(root)
		fs.takeEvents()

		err = publish()
		require.ErrorContains(t, err, "pruned block")
		assertMarker(t, graph, root)
		require.Equal(t, []string{"sync directory"}, fs.takeEvents())
	})

	t.Run("replacement publish sync failure", func(t *testing.T) {
		graph, fs, root := newGraph(t)
		publish, err := graph.PrepareEnvelopeOnDisk(root, testExecutionPayloadEnvelope(root, common.Hash{3}), true)
		require.NoError(t, err)
		fs.takeEvents()
		fs.failNextSync()

		err = publish()
		require.ErrorContains(t, err, "injected directory sync failure")
		assertMarker(t, graph, root)
	})
}

func TestPrunePropagatesDirectorySyncFailureBeforeDroppingBlock(t *testing.T) {
	fs := &directorySyncTrackingFS{Fs: afero.NewMemMapFs()}
	oldRoot := common.Hash{1}
	newRoot := common.Hash{2}
	oldBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	oldBlock.Block.Slot = 100
	newBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	newBlock.Block.Slot = 200
	graph := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	graph.blocks.Store(oldRoot, oldBlock)
	graph.blocks.Store(newRoot, newBlock)
	require.NoError(t, afero.WriteFile(fs, getBeaconStateFilename(oldRoot), []byte{1}, 0o644))
	require.NoError(t, afero.WriteFile(fs, getBeaconStateFilename(newRoot), []byte{1}, 0o644))
	fs.failNextSync()

	err := graph.Prune(150)
	require.ErrorContains(t, err, "injected directory sync failure")
	_, blockStillTracked := graph.blocks.Load(oldRoot)
	require.True(t, blockStillTracked)
	require.NoError(t, graph.Prune(150))
	_, blockStillTracked = graph.blocks.Load(oldRoot)
	require.False(t, blockStillTracked)
}

func testExecutionPayloadEnvelope(root, executionHash common.Hash) *cltypes.SignedExecutionPayloadEnvelope {
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig),
	}
	envelope.Message.BeaconBlockRoot = root
	envelope.Message.Payload.BlockHash = executionHash
	return envelope
}

func writeEncodedEnvelope(t *testing.T, fs afero.Fs, root common.Hash, encoded []byte) {
	t.Helper()
	file, err := fs.Create(getEnvelopeFilename(root))
	require.NoError(t, err)
	writer := snappy.NewBufferedWriter(file)
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(encoded)))
	_, err = writer.Write(length[:])
	require.NoError(t, err)
	_, err = writer.Write(encoded)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())
}

func TestReadEnvelopeFromDiskRejectsTrailingSSZBytes(t *testing.T) {
	fs := afero.NewMemMapFs()
	root := common.Hash{1}
	graph := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	encoded, err := testExecutionPayloadEnvelope(root, common.Hash{2}).EncodeSSZ(nil)
	require.NoError(t, err)
	writeEncodedEnvelope(t, fs, root, append(encoded, 0))

	_, err = graph.ReadEnvelopeFromDisk(root)
	require.Error(t, err)
}

func TestReadEnvelopeFromDiskRejectsDynamicOffsetGap(t *testing.T) {
	fs := afero.NewMemMapFs()
	root := common.Hash{1}
	graph := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	encoded, err := testExecutionPayloadEnvelope(root, common.Hash{2}).EncodeSSZ(nil)
	require.NoError(t, err)
	messageOffset := binary.LittleEndian.Uint32(encoded[:4])
	mutated := make([]byte, 0, len(encoded)+4)
	mutated = append(mutated, encoded[:messageOffset]...)
	mutated = append(mutated, 0, 0, 0, 0)
	mutated = append(mutated, encoded[messageOffset:]...)
	binary.LittleEndian.PutUint32(mutated[:4], messageOffset+4)
	writeEncodedEnvelope(t, fs, root, mutated)

	_, err = graph.ReadEnvelopeFromDisk(root)
	require.ErrorContains(t, err, "non-canonical")
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

	require.NoError(t, f.Prune(150))
	require.Equal(t, uint64(151), f.LowestAvailableSlot())

	require.NoError(t, f.Prune(120))
	require.Equal(t, uint64(151), f.LowestAvailableSlot())
}

func TestPruneScanDoesNotBlockUnrelatedEnvelopePersistence(t *testing.T) {
	baseFS := afero.NewMemMapFs()
	oldRoot := common.Hash{1}
	newRoot := common.Hash{2}
	oldBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	oldBlock.Block.Slot = 100
	newBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	newBlock.Block.Slot = 200
	require.NoError(t, afero.WriteFile(baseFS, getBeaconStateFilename(oldRoot), []byte{1}, 0o644))
	require.NoError(t, afero.WriteFile(baseFS, getBeaconStateFilename(newRoot), []byte{1}, 0o644))

	fs := &blockingStatFS{
		Fs:      baseFS,
		target:  getBeaconStateFilename(oldRoot),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	graph := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	graph.blocks.Store(oldRoot, oldBlock)
	graph.blocks.Store(newRoot, newBlock)

	pruneDone := make(chan error, 1)
	go func() { pruneDone <- graph.Prune(150) }()
	<-fs.started

	envelopeRoot := common.Hash{3}
	dumpDone := make(chan error, 1)
	go func() {
		dumpDone <- graph.DumpEnvelopeOnDisk(envelopeRoot, testExecutionPayloadEnvelope(envelopeRoot, common.Hash{4}))
	}()

	var dumpErr error
	blocked := false
	select {
	case dumpErr = <-dumpDone:
	case <-time.After(100 * time.Millisecond):
		blocked = true
	}
	close(fs.release)
	require.NoError(t, <-pruneDone)
	if blocked {
		require.NoError(t, <-dumpDone)
		t.Fatal("prune held the global persistence lock while scanning the filesystem")
	}
	require.NoError(t, dumpErr)
}

func TestPreparedEnvelopeCannotPublishAfterItsBlockIsPruned(t *testing.T) {
	fs := afero.NewMemMapFs()
	oldRoot := common.Hash{1}
	newRoot := common.Hash{2}
	oldBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	oldBlock.Block.Slot = 100
	newBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	newBlock.Block.Slot = 200
	graph := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	graph.blocks.Store(oldRoot, oldBlock)
	graph.blocks.Store(newRoot, newBlock)
	require.NoError(t, afero.WriteFile(fs, getBeaconStateFilename(oldRoot), []byte{1}, 0o644))
	require.NoError(t, afero.WriteFile(fs, getBeaconStateFilename(newRoot), []byte{1}, 0o644))

	publish, err := graph.PrepareEnvelopeOnDisk(oldRoot, testExecutionPayloadEnvelope(oldRoot, common.Hash{3}), true)
	require.NoError(t, err)
	require.NoError(t, graph.Prune(150))
	require.ErrorContains(t, publish(), "pruned block")
	require.False(t, graph.HasEnvelope(oldRoot))
	pendingRoots, err := graph.PendingEnvelopeIndexRoots()
	require.NoError(t, err)
	require.NotContains(t, pendingRoots, oldRoot)
}

func TestEnvelopeCannotPrepareAfterItsBlockIsPruned(t *testing.T) {
	fs := afero.NewMemMapFs()
	oldRoot := common.Hash{1}
	newRoot := common.Hash{2}
	oldBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	oldBlock.Block.Slot = 100
	newBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	newBlock.Block.Slot = 200
	graph := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	graph.blocks.Store(oldRoot, oldBlock)
	graph.blocks.Store(newRoot, newBlock)
	require.NoError(t, afero.WriteFile(fs, getBeaconStateFilename(oldRoot), []byte{1}, 0o644))
	require.NoError(t, afero.WriteFile(fs, getBeaconStateFilename(newRoot), []byte{1}, 0o644))

	require.NoError(t, graph.Prune(150))
	_, err := graph.PrepareEnvelopeOnDisk(oldRoot, testExecutionPayloadEnvelope(oldRoot, common.Hash{3}), true)
	require.ErrorContains(t, err, "missing block")
	require.False(t, graph.HasEnvelope(oldRoot))
}
