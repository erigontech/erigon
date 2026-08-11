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
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/golang/snappy"
	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// maxSSZObjectSize is a generous upper bound for any single SSZ object
// (beacon state, envelope, etc.). Mainnet states with ~1.5M validators are
// ~327 MB after decompression; 1 GiB leaves ample room for validator-set
// growth while still catching clearly corrupt length fields before OOM.
const maxSSZObjectSize = 1 << 30 // 1 GiB

func getBeaconStateFilename(blockRoot common.Hash) string {
	return fmt.Sprintf("%x.snappy_ssz", blockRoot)
}

// getEnvelopeFilename returns the filename for execution payload envelopes.
// [New in Gloas:EIP7732]
func getEnvelopeFilename(blockRoot common.Hash) string {
	return fmt.Sprintf("%x.envelope.snappy_ssz", blockRoot)
}

func getEnvelopeIndexMarkerFilename(blockRoot common.Hash) string {
	return fmt.Sprintf("%x.envelope.indices-pending", blockRoot)
}

func (f *forkGraphDisk) readBeaconStateFromDisk(blockRoot common.Hash) (bs *state.CachingBeaconState, err error) {
	var file afero.File
	f.stateDumpLock.Lock()
	defer f.stateDumpLock.Unlock()

	file, err = f.fs.Open(getBeaconStateFilename(blockRoot))
	if err != nil {
		return
	}
	defer file.Close()

	if f.sszSnappyReader == nil {
		f.sszSnappyReader = snappy.NewReader(file)
	} else {
		f.sszSnappyReader.Reset(file)
	}
	// Read the version
	v := []byte{0}
	if _, err := f.sszSnappyReader.Read(v); err != nil {
		return nil, fmt.Errorf("failed to read hard fork version: %w, root: %x", err, blockRoot)
	}
	// Read the length
	lengthBytes := make([]byte, 8)
	var n int
	n, err = io.ReadFull(f.sszSnappyReader, lengthBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read length: %w, root: %x", err, blockRoot)
	}
	if n != 8 {
		return nil, fmt.Errorf("failed to read length: %d, want 8, root: %x", n, blockRoot)
	}

	length := binary.BigEndian.Uint64(lengthBytes)
	if length > maxSSZObjectSize {
		return nil, fmt.Errorf("corrupt beacon state file: length %d exceeds max %d, root: %x", length, maxSSZObjectSize, blockRoot)
	}
	if length > uint64(cap(f.sszBuffer)) {
		f.sszBuffer = make([]byte, length)
	} else {
		f.sszBuffer = f.sszBuffer[:length]
	}
	n, err = io.ReadFull(f.sszSnappyReader, f.sszBuffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read snappy buffer: %w, root: %x", err, blockRoot)
	}
	f.sszBuffer = f.sszBuffer[:n]
	bs = state.New(f.beaconCfg)

	if err = bs.DecodeSSZ(f.sszBuffer, int(v[0])); err != nil {
		return nil, fmt.Errorf("failed to decode beacon state: %w, root: %x, len: %d, decLen: %d, bs: %+v", err, blockRoot, n, len(f.sszBuffer), bs)
	}

	// Try to read the persisted previousStateRoot (appended after SSZ data).
	// This is needed for GLOAS where the execution payload envelope modifies
	// the state after TransitionState, making HashSSZ() diverge from
	// the block's state_root. Older state files won't have this field;
	// in that case we leave previousStateRoot as zero (HashSSZ fallback).
	var prevRoot [32]byte
	if _, readErr := io.ReadFull(f.sszSnappyReader, prevRoot[:]); readErr == nil {
		bs.SetPreviousStateRoot(common.Hash(prevRoot))
	}

	return
}

// dumpBeaconStateOnDisk dumps a beacon state on disk in ssz snappy format
func (f *forkGraphDisk) DumpBeaconStateOnDisk(blockRoot common.Hash, bs *state.CachingBeaconState, forced bool) (err error) {
	if !forced && bs.Slot()%dumpSlotFrequency != 0 {
		return
	}
	f.stateDumpLock.Lock()
	defer f.stateDumpLock.Unlock()
	// Truncate and then grow the buffer to the size of the state.
	f.sszBuffer, err = bs.EncodeSSZ(f.sszBuffer[:0])
	if err != nil {
		return
	}
	version := bs.Version()

	dumpedFile, err := f.fs.OpenFile(getBeaconStateFilename(blockRoot), os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		return err
	}
	defer dumpedFile.Close()

	if f.sszSnappyWriter == nil {
		f.sszSnappyWriter = snappy.NewBufferedWriter(dumpedFile)
	} else {
		f.sszSnappyWriter.Reset(dumpedFile)
	}

	// First write the hard fork version
	if _, err := f.sszSnappyWriter.Write([]byte{byte(version)}); err != nil {
		log.Error("failed to write hard fork version", "err", err)
		return err
	}
	// Second write the length
	length := make([]byte, 8)
	binary.BigEndian.PutUint64(length, uint64(len(f.sszBuffer)))
	if _, err := f.sszSnappyWriter.Write(length); err != nil {
		log.Error("failed to write length", "err", err)
		return err
	}
	// Lastly dump the state
	if _, err := f.sszSnappyWriter.Write(f.sszBuffer); err != nil {
		log.Error("failed to write ssz buffer", "err", err)
		return err
	}
	// A skipped-slot state root differs from the latest block header's state root.
	var stateRootToWrite common.Hash
	if bs.Version() >= clparams.GloasVersion && bs.LatestBlockHeader().Slot < bs.Slot() {
		stateRootToWrite = bs.PeekPreviousStateRoot()
	} else if hdr, ok := f.GetHeader(blockRoot); ok {
		stateRootToWrite = hdr.Root
	} else {
		// Fallback for anchor state or cases where header isn't stored yet
		stateRootToWrite = bs.PeekPreviousStateRoot()
	}
	if _, err := f.sszSnappyWriter.Write(stateRootToWrite[:]); err != nil {
		log.Error("failed to write previousStateRoot", "err", err)
		return err
	}
	if err = f.sszSnappyWriter.Flush(); err != nil {
		log.Error("failed to flush snappy writer", "err", err)
		return err
	}

	if err = dumpedFile.Sync(); err != nil {
		log.Error("failed to sync dumped file", "err", err)
		return
	}

	return
}

// HasEnvelope checks if an envelope exists for the given block root.
// Uses an in-memory cache populated by DumpEnvelopeOnDisk to avoid repeated disk stats.
// [New in Gloas:EIP7732]
func (f *forkGraphDisk) HasEnvelope(blockRoot common.Hash) bool {
	// Fast path: check in-memory cache
	if _, ok := f.envelopeExists.Load(blockRoot); ok {
		return true
	}
	// Slow path: fall back to disk and populate cache on hit
	exists, err := afero.Exists(f.fs, getEnvelopeFilename(blockRoot))
	if err == nil && exists {
		envelope, readErr := f.ReadEnvelopeFromDisk(blockRoot)
		if readErr != nil || envelope == nil || envelope.Message == nil || envelope.Message.BeaconBlockRoot != blockRoot {
			return false
		}
		f.envelopeExists.Store(blockRoot, struct{}{})
		return true
	}
	return false
}

// ReadEnvelopeFromDisk reads an execution payload envelope from disk.
// [New in Gloas:EIP7732]
func (f *forkGraphDisk) ReadEnvelopeFromDisk(blockRoot common.Hash) (envelope *cltypes.SignedExecutionPayloadEnvelope, err error) {
	var file afero.File
	f.stateDumpLock.Lock()
	defer f.stateDumpLock.Unlock()

	file, err = f.fs.Open(getEnvelopeFilename(blockRoot))
	if err != nil {
		return
	}
	defer file.Close()

	if f.sszSnappyReader == nil {
		f.sszSnappyReader = snappy.NewReader(file)
	} else {
		f.sszSnappyReader.Reset(file)
	}

	// Read the length
	lengthBytes := make([]byte, 8)
	var n int
	n, err = io.ReadFull(f.sszSnappyReader, lengthBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read length: %w, root: %x", err, blockRoot)
	}
	if n != 8 {
		return nil, fmt.Errorf("failed to read length: %d, want 8, root: %x", n, blockRoot)
	}

	envelopeLength := binary.BigEndian.Uint64(lengthBytes)
	if envelopeLength > maxSSZObjectSize {
		return nil, fmt.Errorf("corrupt envelope file: length %d exceeds max %d, root: %x", envelopeLength, maxSSZObjectSize, blockRoot)
	}
	if envelopeLength > uint64(cap(f.sszBuffer)) {
		f.sszBuffer = make([]byte, envelopeLength)
	} else {
		f.sszBuffer = f.sszBuffer[:envelopeLength]
	}
	n, err = io.ReadFull(f.sszSnappyReader, f.sszBuffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read snappy buffer: %w, root: %x", err, blockRoot)
	}
	f.sszBuffer = f.sszBuffer[:n]
	var trailing [1]byte
	trailingN, trailingErr := f.sszSnappyReader.Read(trailing[:])
	if trailingN != 0 || trailingErr != io.EOF {
		return nil, fmt.Errorf("trailing envelope data, root: %x", blockRoot)
	}
	encoded := append([]byte(nil), f.sszBuffer...)

	envelope = &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(f.beaconCfg),
	}
	if err = envelope.DecodeSSZ(f.sszBuffer, int(clparams.GloasVersion)); err != nil {
		return nil, fmt.Errorf("failed to decode envelope: %w, root: %x, len: %d", err, blockRoot, n)
	}
	canonical, err := envelope.EncodeSSZ(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to re-encode envelope: %w, root: %x", err, blockRoot)
	}
	if !bytes.Equal(encoded, canonical) {
		return nil, fmt.Errorf("non-canonical envelope encoding, root: %x", blockRoot)
	}

	return
}

// DumpEnvelopeOnDisk dumps an execution payload envelope to disk.
// [New in Gloas:EIP7732]
func (f *forkGraphDisk) DumpEnvelopeOnDisk(blockRoot common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) (err error) {
	publish, err := f.PrepareEnvelopeOnDisk(blockRoot, envelope, false)
	if err != nil {
		return err
	}
	return publish()
}

func (f *forkGraphDisk) PrepareEnvelopeOnDisk(blockRoot common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope, requireBlock bool) (publish func() error, err error) {
	f.stateDumpLock.Lock()
	defer f.stateDumpLock.Unlock()
	_, blockWasPresent := f.blocks.Load(blockRoot)
	if requireBlock && !blockWasPresent {
		return nil, fmt.Errorf("cannot prepare envelope for missing block %x", blockRoot)
	}

	// Encode the envelope
	f.sszBuffer, err = envelope.EncodeSSZ(f.sszBuffer[:0])
	if err != nil {
		return
	}

	filename := getEnvelopeFilename(blockRoot)
	dumpedFile, err := afero.TempFile(f.fs, ".", filename+".tmp-")
	if err != nil {
		return nil, err
	}
	temporaryFilename := dumpedFile.Name()
	keepTemporary := false
	defer func() {
		_ = dumpedFile.Close()
		if !keepTemporary {
			_ = f.fs.Remove(temporaryFilename)
		}
	}()

	if f.sszSnappyWriter == nil {
		f.sszSnappyWriter = snappy.NewBufferedWriter(dumpedFile)
	} else {
		f.sszSnappyWriter.Reset(dumpedFile)
	}

	// Write the length
	length := make([]byte, 8)
	binary.BigEndian.PutUint64(length, uint64(len(f.sszBuffer)))
	if _, err := f.sszSnappyWriter.Write(length); err != nil {
		log.Error("failed to write length", "err", err)
		return nil, err
	}
	// Write the envelope
	if _, err := f.sszSnappyWriter.Write(f.sszBuffer); err != nil {
		log.Error("failed to write ssz buffer", "err", err)
		return nil, err
	}
	if err = f.sszSnappyWriter.Flush(); err != nil {
		log.Error("failed to flush snappy writer", "err", err)
		return nil, err
	}

	if err = dumpedFile.Sync(); err != nil {
		log.Error("failed to sync dumped file", "err", err)
		return
	}
	if err := dumpedFile.Close(); err != nil {
		return nil, err
	}
	markerFilename := getEnvelopeIndexMarkerFilename(blockRoot)
	markerExisted, err := afero.Exists(f.fs, markerFilename)
	if err != nil {
		return nil, err
	}
	if !markerExisted {
		marker, err := f.fs.OpenFile(markerFilename, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
		if err != nil {
			return nil, err
		}
		if err = marker.Sync(); err != nil {
			_ = marker.Close()
			_ = f.fs.Remove(markerFilename)
			return nil, err
		}
		if err = marker.Close(); err != nil {
			_ = f.fs.Remove(markerFilename)
			return nil, err
		}
	}
	if err := syncRootDirectory(f.fs); err != nil {
		return nil, err
	}
	keepTemporary = true
	cleanupFiles := []string{temporaryFilename}
	if !markerExisted {
		cleanupFiles = append(cleanupFiles, markerFilename)
	}

	return func() error {
		f.stateDumpLock.Lock()
		defer f.stateDumpLock.Unlock()
		if _, blockPresent := f.blocks.Load(blockRoot); requireBlock && !blockPresent {
			if cleanupErr := removeFilesAndSyncDirectory(f.fs, cleanupFiles...); cleanupErr != nil {
				return fmt.Errorf("cannot publish envelope for pruned block %x: %w", blockRoot, cleanupErr)
			}
			return fmt.Errorf("cannot publish envelope for pruned block %x", blockRoot)
		}
		if err := f.fs.Rename(temporaryFilename, filename); err != nil {
			if cleanupErr := removeFilesAndSyncDirectory(f.fs, cleanupFiles...); cleanupErr != nil {
				return errors.Join(err, cleanupErr)
			}
			return err
		}
		if err := syncRootDirectory(f.fs); err != nil {
			return err
		}
		f.envelopeExists.Store(blockRoot, struct{}{})
		return nil
	}, nil
}

func (f *forkGraphDisk) PendingEnvelopeIndexRoots() ([]common.Hash, error) {
	f.stateDumpLock.Lock()
	defer f.stateDumpLock.Unlock()
	entries, err := afero.ReadDir(f.fs, ".")
	if err != nil {
		return nil, err
	}
	const suffix = ".envelope.indices-pending"
	roots := make([]common.Hash, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, suffix) {
			continue
		}
		rootBytes, err := hex.DecodeString(strings.TrimSuffix(name, suffix))
		if err != nil || len(rootBytes) != len(common.Hash{}) {
			continue
		}
		roots = append(roots, common.BytesToHash(rootBytes))
	}
	return roots, nil
}

func (f *forkGraphDisk) MarkEnvelopeIndicesCommitted(blockRoot common.Hash) error {
	f.stateDumpLock.Lock()
	defer f.stateDumpLock.Unlock()
	matches, err := afero.Glob(f.fs, getEnvelopeFilename(blockRoot)+".tmp-*")
	if err != nil {
		return err
	}
	for _, match := range matches {
		if err := f.fs.Remove(match); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	err = f.fs.Remove(getEnvelopeIndexMarkerFilename(blockRoot))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncRootDirectory(f.fs)
}

func removeFilesAndSyncDirectory(fs afero.Fs, filenames ...string) error {
	for _, filename := range filenames {
		if err := fs.Remove(filename); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return syncRootDirectory(fs)
}

func syncRootDirectory(fs afero.Fs) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := fs.Open(".")
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return err
	}
	return directory.Close()
}
