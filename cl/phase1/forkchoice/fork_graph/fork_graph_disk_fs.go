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
	"encoding/binary"
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

func getEnvelopeTempFilename(blockRoot common.Hash) string {
	return getEnvelopeFilename(blockRoot) + ".tmp"
}

func cleanupEnvelopeArtifacts(fs afero.Fs) error {
	entries, err := afero.ReadDir(fs, ".")
	if err != nil {
		return err
	}
	var cleanupErr error
	removed := false
	for _, entry := range entries {
		if entry.IsDir() || !(strings.HasSuffix(entry.Name(), ".envelope.snappy_ssz.tmp") ||
			strings.HasSuffix(entry.Name(), ".envelope.snappy_ssz.corrupt") ||
			strings.HasSuffix(entry.Name(), ".envelope.snappy_ssz.pruned")) {
			continue
		}
		if err := fs.Remove(entry.Name()); err != nil && !os.IsNotExist(err) {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove envelope artifact %s: %w", entry.Name(), err))
		} else if err == nil {
			removed = true
		}
	}
	if removed {
		cleanupErr = errors.Join(cleanupErr, syncEnvelopeDirectory(fs))
	}
	return cleanupErr
}

func syncEnvelopeDirectory(fs afero.Fs) error {
	return syncEnvelopeDirectoryForOS(fs, runtime.GOOS)
}

func syncEnvelopeDirectoryForOS(fs afero.Fs, goos string) error {
	if goos == "windows" {
		return nil
	}
	dir, err := fs.Open(".")
	if err != nil {
		return err
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}

func removeOrQuarantineEnvelope(fs afero.Fs, filename, suffix string) error {
	err := fs.Remove(filename)
	if err == nil || os.IsNotExist(err) {
		return nil
	}
	renameErr := fs.Rename(filename, filename+suffix)
	if renameErr == nil || os.IsNotExist(renameErr) {
		return nil
	}
	return errors.Join(err, fmt.Errorf("quarantine envelope: %w", renameErr))
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
	// Write the authoritative state root so it can be restored on load.
	// Use the stored block header's Root (set from block.StateRoot in AddChainSegment)
	// rather than the state's PreviousStateRoot cache field, which can be stale if
	// a concurrent block arrival modified f.currentState between GetStateAtBlockRoot
	// and the copy in OnHeadStateWithBlockRoot.
	var stateRootToWrite common.Hash
	if hdr, ok := f.GetHeader(blockRoot); ok {
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
	if _, invalid := f.invalidEnvelopes.Load(blockRoot); invalid {
		return false
	}
	if blockRoot != f.anchorRoot {
		if _, ok := f.blocks.Load(blockRoot); !ok {
			f.envelopeExists.Delete(blockRoot)
			return false
		}
	}
	if _, ok := f.envelopeExists.Load(blockRoot); ok {
		return true
	}
	f.stateDumpLock.Lock()
	defer f.stateDumpLock.Unlock()
	if _, invalid := f.invalidEnvelopes.Load(blockRoot); invalid {
		return false
	}
	if blockRoot != f.anchorRoot {
		if _, ok := f.blocks.Load(blockRoot); !ok {
			f.envelopeExists.Delete(blockRoot)
			return false
		}
	}
	if _, ok := f.envelopeExists.Load(blockRoot); ok {
		return true
	}
	exists, err := afero.Exists(f.fs, getEnvelopeFilename(blockRoot))
	if err == nil && exists {
		f.envelopeExists.Store(blockRoot, struct{}{})
		return true
	}
	return false
}

// ReadEnvelopeFromDisk reads an execution payload envelope from disk.
// [New in Gloas:EIP7732]
func (f *forkGraphDisk) ReadEnvelopeFromDisk(blockRoot common.Hash) (envelope *cltypes.SignedExecutionPayloadEnvelope, err error) {
	var file afero.File
	var corrupt bool
	f.stateDumpLock.Lock()
	defer f.stateDumpLock.Unlock()
	if _, invalid := f.invalidEnvelopes.Load(blockRoot); invalid {
		return nil, fmt.Errorf("cannot read known invalid envelope for root %x", blockRoot)
	}
	if blockRoot != f.anchorRoot {
		if _, ok := f.blocks.Load(blockRoot); !ok {
			f.envelopeExists.Delete(blockRoot)
			return nil, fmt.Errorf("cannot read envelope for unknown block root %x", blockRoot)
		}
	}

	filename := getEnvelopeFilename(blockRoot)
	file, err = f.fs.Open(filename)
	if err != nil {
		f.envelopeExists.Delete(blockRoot)
		return
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Warn("failed to close envelope after read", "root", blockRoot, "err", closeErr)
		}
		if corrupt {
			f.invalidEnvelopes.Store(blockRoot, struct{}{})
			if removeErr := removeOrQuarantineEnvelope(f.fs, filename, ".corrupt"); removeErr != nil {
				log.Warn("failed to remove corrupt envelope", "root", blockRoot, "err", removeErr)
			}
			if syncErr := syncEnvelopeDirectory(f.fs); syncErr != nil {
				log.Warn("failed to sync envelope directory after corrupt cleanup", "root", blockRoot, "err", syncErr)
			}
		}
		if err != nil {
			f.envelopeExists.Delete(blockRoot)
		}
	}()

	readTracker := &envelopeReadTracker{Reader: file}
	if f.sszSnappyReader == nil {
		f.sszSnappyReader = snappy.NewReader(readTracker)
	} else {
		f.sszSnappyReader.Reset(readTracker)
	}

	// Read the length
	lengthBytes := make([]byte, 8)
	var n int
	n, err = io.ReadFull(f.sszSnappyReader, lengthBytes)
	if err != nil {
		corrupt = isCorruptEnvelopeReadError(err, readTracker.err)
		return nil, fmt.Errorf("failed to read length: %w, root: %x", err, blockRoot)
	}
	if n != 8 {
		corrupt = true
		return nil, fmt.Errorf("failed to read length: %d, want 8, root: %x", n, blockRoot)
	}

	envelopeLength := binary.BigEndian.Uint64(lengthBytes)
	if envelopeLength > clparams.MaxChunkSize {
		corrupt = true
		return nil, fmt.Errorf("corrupt envelope file: length %d exceeds max %d, root: %x", envelopeLength, clparams.MaxChunkSize, blockRoot)
	}
	ownedBuffer := make([]byte, envelopeLength)
	n, err = io.ReadFull(f.sszSnappyReader, ownedBuffer)
	if err != nil {
		corrupt = isCorruptEnvelopeReadError(err, readTracker.err)
		return nil, fmt.Errorf("failed to read snappy buffer: %w, root: %x", err, blockRoot)
	}
	ownedBuffer = ownedBuffer[:n]

	envelope = &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(f.beaconCfg),
	}
	if err = envelope.DecodeSSZ(ownedBuffer, int(clparams.GloasVersion)); err != nil {
		corrupt = true
		return nil, fmt.Errorf("failed to decode envelope: %w, root: %x, len: %d", err, blockRoot, n)
	}
	if envelope.Message.BeaconBlockRoot != blockRoot {
		corrupt = true
		return nil, fmt.Errorf("corrupt envelope file: embedded root %x does not match filename root %x", envelope.Message.BeaconBlockRoot, blockRoot)
	}
	f.invalidEnvelopes.Delete(blockRoot)

	return
}

type envelopeReadTracker struct {
	io.Reader
	err error
}

func (r *envelopeReadTracker) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		r.err = err
	}
	return n, err
}

func isCorruptEnvelopeReadError(err, sourceErr error) bool {
	return sourceErr == nil || !errors.Is(err, sourceErr)
}

// DumpEnvelopeOnDisk dumps an execution payload envelope to disk.
// [New in Gloas:EIP7732]
func (f *forkGraphDisk) DumpEnvelopeOnDisk(blockRoot common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) (err error) {
	if envelope == nil {
		return errors.New("cannot persist nil envelope")
	}
	if envelope.Message == nil {
		return errors.New("cannot persist envelope with nil message")
	}
	if envelope.Message.Payload == nil {
		return errors.New("cannot persist envelope with nil payload")
	}
	if envelope.Message.ExecutionRequests == nil {
		return errors.New("cannot persist envelope with nil execution requests")
	}
	if envelope.Message.Payload.Version() != clparams.GloasVersion {
		return fmt.Errorf("cannot persist envelope payload version %d", envelope.Message.Payload.Version())
	}
	if envelope.Message.ExecutionRequests.Version() != clparams.GloasVersion {
		return fmt.Errorf("cannot persist envelope execution requests version %d", envelope.Message.ExecutionRequests.Version())
	}
	if envelope.Message.BeaconBlockRoot != blockRoot {
		return fmt.Errorf("cannot persist envelope for root %x with embedded root %x", blockRoot, envelope.Message.BeaconBlockRoot)
	}
	f.stateDumpLock.Lock()
	defer f.stateDumpLock.Unlock()
	if blockRoot != f.anchorRoot {
		if _, ok := f.blocks.Load(blockRoot); !ok {
			return fmt.Errorf("cannot persist envelope for unknown block root %x", blockRoot)
		}
	}

	// Populate in-memory cache on successful write
	defer func() {
		if err == nil || errors.Is(err, ErrEnvelopeCommitted) {
			f.envelopeExists.Store(blockRoot, struct{}{})
			f.invalidEnvelopes.Delete(blockRoot)
		}
	}()

	// Encode the envelope
	f.sszBuffer, err = envelope.EncodeSSZ(f.sszBuffer[:0])
	if err != nil {
		return
	}
	if uint64(len(f.sszBuffer)) > clparams.MaxChunkSize {
		return fmt.Errorf("cannot persist envelope: length %d exceeds max %d", len(f.sszBuffer), clparams.MaxChunkSize)
	}

	filename := getEnvelopeFilename(blockRoot)
	tempFilename := getEnvelopeTempFilename(blockRoot)
	dumpedFile, err := f.fs.OpenFile(tempFilename, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = dumpedFile.Close()
		}
		if err != nil {
			_ = f.fs.Remove(tempFilename)
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
		return err
	}
	// Write the envelope
	if _, err := f.sszSnappyWriter.Write(f.sszBuffer); err != nil {
		log.Error("failed to write ssz buffer", "err", err)
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
	if err = dumpedFile.Close(); err != nil {
		return
	}
	closed = true
	if err = f.fs.Rename(tempFilename, filename); err != nil {
		return
	}
	if err = syncEnvelopeDirectory(f.fs); err != nil {
		return fmt.Errorf("%w: sync envelope directory: %w", ErrEnvelopeCommitted, err)
	}

	return
}
