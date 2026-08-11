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

type envelopeWriteFailureFs struct {
	afero.Fs
	stage string
}

func (f envelopeWriteFailureFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	if f.stage == "open" && strings.HasSuffix(name, ".tmp") {
		return nil, errTestEnvelopeIO
	}
	file, err := f.Fs.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return envelopeWriteFailureFile{File: file, stage: f.stage}, nil
}

func (f envelopeWriteFailureFs) Rename(oldname, newname string) error {
	if f.stage == "rename" {
		return errTestEnvelopeIO
	}
	return f.Fs.Rename(oldname, newname)
}

type envelopeWriteFailureFile struct {
	afero.File
	stage string
}

type envelopeBlockingRenameFs struct {
	afero.Fs
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

type envelopeRemoveFailureFs struct {
	afero.Fs
	suffix     string
	failRename bool
}

func (f envelopeRemoveFailureFs) Rename(oldname, newname string) error {
	if f.failRename && strings.HasSuffix(oldname, f.suffix) {
		return errTestEnvelopeIO
	}
	return f.Fs.Rename(oldname, newname)
}

func (f envelopeRemoveFailureFs) Remove(name string) error {
	if strings.HasSuffix(name, f.suffix) {
		return errTestEnvelopeIO
	}
	return f.Fs.Remove(name)
}

type envelopeDirSyncFs struct {
	afero.Fs
	fail  bool
	syncs int
}

func (f *envelopeDirSyncFs) Open(name string) (afero.File, error) {
	file, err := f.Fs.Open(name)
	if err != nil || name != "." {
		return file, err
	}
	return &envelopeDirSyncFile{File: file, fs: f}, nil
}

type envelopeDirSyncFile struct {
	afero.File
	fs *envelopeDirSyncFs
}

func (f *envelopeDirSyncFile) Sync() error {
	f.fs.syncs++
	if f.fs.fail {
		return errTestEnvelopeIO
	}
	return f.File.Sync()
}

func (f *envelopeBlockingRenameFs) Rename(oldname, newname string) error {
	if strings.HasSuffix(oldname, ".tmp") {
		f.once.Do(func() { close(f.reached) })
		<-f.release
	}
	return f.Fs.Rename(oldname, newname)
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
	if f.stage == "close" {
		_ = f.File.Close()
		return errTestEnvelopeIO
	}
	return f.File.Close()
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

func TestReadEnvelopeRemovesCorruptPersistenceMarker(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	require.NoError(t, afero.WriteFile(fs, getEnvelopeFilename(root), []byte("truncated"), 0o644))
	require.True(t, f.HasEnvelope(root))

	_, err := f.ReadEnvelopeFromDisk(root)
	require.Error(t, err)
	require.False(t, f.HasEnvelope(root))
}

func TestReadEnvelopeQuarantinesCorruptFileWhenRemoveFails(t *testing.T) {
	baseFs := afero.NewMemMapFs()
	fs := envelopeRemoveFailureFs{Fs: baseFs, suffix: ".envelope.snappy_ssz"}
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	filename := getEnvelopeFilename(root)
	require.NoError(t, afero.WriteFile(baseFs, filename, []byte("truncated"), 0o644))

	_, err := f.ReadEnvelopeFromDisk(root)
	require.Error(t, err)
	finalExists, existsErr := afero.Exists(baseFs, filename)
	require.NoError(t, existsErr)
	require.False(t, finalExists)
	quarantineExists, existsErr := afero.Exists(baseFs, filename+".corrupt")
	require.NoError(t, existsErr)
	require.True(t, quarantineExists)
	require.False(t, f.HasEnvelope(root))
}

func TestReadEnvelopeRemovesUnsupportedSnappyFrames(t *testing.T) {
	streamIdentifier := []byte{0xff, 0x06, 0x00, 0x00, 's', 'N', 'a', 'P', 'p', 'Y'}
	frame := append(append([]byte{}, streamIdentifier...), 0x02, 0x00, 0x00, 0x00)
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	require.NoError(t, afero.WriteFile(fs, getEnvelopeFilename(root), frame, 0o644))
	require.True(t, f.HasEnvelope(root))

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
	_, err := writer.Write(length)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, afero.WriteFile(fs, getEnvelopeFilename(root), compressed.Bytes(), 0o644))

	_, err = f.ReadEnvelopeFromDisk(root)
	require.ErrorContains(t, err, "exceeds max")
	require.False(t, f.HasEnvelope(root))
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

func TestDumpEnvelopeSyncsDirectoryAfterRename(t *testing.T) {
	baseFs := afero.NewMemMapFs()
	fs := &envelopeDirSyncFs{Fs: baseFs}
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)

	require.NoError(t, f.DumpEnvelopeOnDisk(root, testEnvelopeWithTransaction(root, []byte{1})))
	require.Equal(t, 1, fs.syncs)
}

func TestSyncEnvelopeDirectorySkipsUnsupportedWindowsDirectoryFlush(t *testing.T) {
	fs := &envelopeDirSyncFs{Fs: afero.NewMemMapFs(), fail: true}

	require.NoError(t, syncEnvelopeDirectoryForOS(fs, "windows"))
	require.Zero(t, fs.syncs)
}

func TestRemoveOrQuarantineEnvelopeTreatsSuccessfulRenameAsSuccess(t *testing.T) {
	baseFs := afero.NewMemMapFs()
	fs := envelopeRemoveFailureFs{Fs: baseFs, suffix: ".envelope.snappy_ssz"}
	filename := getEnvelopeFilename(common.HexToHash("0x1234"))
	require.NoError(t, afero.WriteFile(baseFs, filename, []byte("corrupt"), 0o644))

	require.NoError(t, removeOrQuarantineEnvelope(fs, filename, ".corrupt"))
	exists, err := afero.Exists(baseFs, filename)
	require.NoError(t, err)
	require.False(t, exists)
	quarantined, err := afero.Exists(baseFs, filename+".corrupt")
	require.NoError(t, err)
	require.True(t, quarantined)
}

func TestDumpEnvelopeReturnsDirectorySyncErrorAfterCompleteRename(t *testing.T) {
	baseFs := afero.NewMemMapFs()
	fs := &envelopeDirSyncFs{Fs: baseFs, fail: true}
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	f.invalidEnvelopes.Store(root, struct{}{})

	require.ErrorIs(t, f.DumpEnvelopeOnDisk(root, testEnvelopeWithTransaction(root, []byte{1})), errTestEnvelopeIO)
	exists, err := afero.Exists(baseFs, getEnvelopeFilename(root))
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, f.HasEnvelope(root))
	_, err = f.ReadEnvelopeFromDisk(root)
	require.NoError(t, err)
}

func TestDumpEnvelopeRejectsIncompleteInput(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	validMessage := cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)
	wrongPayloadVersion := cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)
	wrongPayloadVersion.BeaconBlockRoot = root
	wrongPayloadVersion.Payload = cltypes.NewEth1Block(clparams.DenebVersion, &clparams.MainnetBeaconConfig)
	wrongRequestsVersion := cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)
	wrongRequestsVersion.BeaconBlockRoot = root
	wrongRequestsVersion.ExecutionRequests = cltypes.NewExecutionRequestsWithVersion(&clparams.MainnetBeaconConfig, clparams.ElectraVersion)
	zeroRequests := cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)
	zeroRequests.BeaconBlockRoot = root
	zeroRequests.ExecutionRequests = &cltypes.ExecutionRequests{}

	for _, tt := range []struct {
		name     string
		envelope *cltypes.SignedExecutionPayloadEnvelope
	}{
		{name: "nil envelope"},
		{name: "nil message", envelope: &cltypes.SignedExecutionPayloadEnvelope{}},
		{name: "nil payload", envelope: &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{ExecutionRequests: validMessage.ExecutionRequests}}},
		{name: "nil execution requests", envelope: &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{Payload: validMessage.Payload}}},
		{name: "wrong payload version", envelope: &cltypes.SignedExecutionPayloadEnvelope{Message: wrongPayloadVersion}},
		{name: "wrong requests version", envelope: &cltypes.SignedExecutionPayloadEnvelope{Message: wrongRequestsVersion}},
		{name: "uninitialized requests", envelope: &cltypes.SignedExecutionPayloadEnvelope{Message: zeroRequests}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, f.DumpEnvelopeOnDisk(root, tt.envelope))
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

			f.fs = envelopeWriteFailureFs{Fs: fs, stage: stage}
			require.ErrorIs(t, f.DumpEnvelopeOnDisk(root, testEnvelopeWithTransaction(root, []byte{9, 8, 7})), errTestEnvelopeIO)
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

	blockingFs := &envelopeBlockingRenameFs{Fs: fs, reached: make(chan struct{}), release: make(chan struct{})}
	f.fs = blockingFs
	dumpDone := make(chan error, 1)
	go func() {
		dumpDone <- f.DumpEnvelopeOnDisk(oldRoot, testEnvelopeWithTransaction(oldRoot, []byte{2}))
	}()
	<-blockingFs.reached
	pruneDone := make(chan error, 1)
	go func() { pruneDone <- f.Prune(2) }()

	pruneCompleted := false
	select {
	case err := <-pruneDone:
		require.NoError(t, err)
		pruneCompleted = true
	case <-time.After(time.Second):
	}
	close(blockingFs.release)
	require.NoError(t, <-dumpDone)
	if !pruneCompleted {
		require.NoError(t, <-pruneDone)
	}

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
	f.fs = envelopeRemoveFailureFs{Fs: baseFs, suffix: ".envelope.snappy_ssz", failRename: true}

	require.ErrorIs(t, f.Prune(2), errTestEnvelopeIO)
	require.False(t, f.HasEnvelope(oldRoot))
	_, err := f.ReadEnvelopeFromDisk(oldRoot)
	require.Error(t, err)
	_, exists := f.blocks.Load(oldRoot)
	require.False(t, exists)
}

func TestNewForkGraphDiskRemovesOrphanEnvelopeTemps(t *testing.T) {
	fs := afero.NewMemMapFs()
	require.NoError(t, afero.WriteFile(fs, getEnvelopeTempFilename(common.HexToHash("0x1")), []byte("orphan"), 0o644))
	corruptFilename := getEnvelopeFilename(common.HexToHash("0x2")) + ".corrupt"
	require.NoError(t, afero.WriteFile(fs, corruptFilename, []byte("orphan"), 0o644))
	require.NoError(t, afero.WriteFile(fs, "unrelated.tmp", []byte("keep"), 0o644))
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))

	NewForkGraphDisk(anchorState, nil, fs, beacon_router_configuration.RouterConfiguration{})
	envelopeTempExists, err := afero.Exists(fs, getEnvelopeTempFilename(common.HexToHash("0x1")))
	require.NoError(t, err)
	require.False(t, envelopeTempExists)
	corruptExists, err := afero.Exists(fs, corruptFilename)
	require.NoError(t, err)
	require.False(t, corruptExists)
	unrelatedExists, err := afero.Exists(fs, "unrelated.tmp")
	require.NoError(t, err)
	require.True(t, unrelatedExists)
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
}

func testEnvelopeWithTransaction(root common.Hash, transaction []byte) *cltypes.SignedExecutionPayloadEnvelope {
	envelope := cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)
	envelope.BeaconBlockRoot = root
	envelope.Payload.Extra = solid.NewExtraData()
	envelope.Payload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{transaction})
	return &cltypes.SignedExecutionPayloadEnvelope{Message: envelope}
}

func addEnvelopeTestBlock(f *forkGraphDisk, root common.Hash, slot uint64) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = slot
	f.blocks.Store(root, block)
}
