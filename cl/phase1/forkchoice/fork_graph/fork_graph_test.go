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

func TestDumpEnvelopePreservesPostGloasVersion(t *testing.T) {
	fs := afero.NewMemMapFs()
	f := &forkGraphDisk{fs: fs, beaconCfg: &clparams.MainnetBeaconConfig}
	root := common.HexToHash("0x1234")
	addEnvelopeTestBlock(f, root, 1)
	version := clparams.GloasVersion + 1
	envelope := testEnvelopeWithVersion(root, []byte{1, 2, 3}, version)

	require.NoError(t, f.DumpEnvelopeOnDisk(root, envelope))
	persisted, err := f.ReadEnvelopeFromDisk(root)
	require.NoError(t, err)
	require.Equal(t, version, persisted.Message.Payload.Version())
	require.Equal(t, version, persisted.Message.ExecutionRequests.Version())
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
	require.ErrorContains(t, err, "list too big")
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

func TestDumpEnvelopeRejectsTooManyTransactionsBeforeWriting(t *testing.T) {
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

	err := f.DumpEnvelopeOnDisk(root, &cltypes.SignedExecutionPayloadEnvelope{Message: envelope})
	require.ErrorContains(t, err, "too many transactions")
	exists, existsErr := afero.Exists(fs, getEnvelopeFilename(root))
	require.NoError(t, existsErr)
	require.False(t, exists)
}

func TestDumpEnvelopeRejectsDepositRepresentationUnreadableByConfiguredDecoder(t *testing.T) {
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
	require.ErrorContains(t, err, "decoder resource limit")
	exists, existsErr := afero.Exists(fs, getEnvelopeFilename(root))
	require.NoError(t, existsErr)
	require.False(t, exists)
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

func TestReadEnvelopeTransactionsPreserveDecodeLimits(t *testing.T) {
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
	require.ErrorContains(t, persisted.Message.Payload.Transactions.DecodeSSZ(overLimit, 0), "expected at most 1 transactions")
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
