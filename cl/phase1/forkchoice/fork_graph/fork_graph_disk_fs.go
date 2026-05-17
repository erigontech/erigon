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
	"fmt"
	"io"
	"os"

	"github.com/golang/snappy"
	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

const maxSSZBufferSize = 128 << 20 // 128 MB

func getBeaconStateFilename(blockRoot common.Hash) string {
	return fmt.Sprintf("%x.snappy_ssz", blockRoot)
}

// getEnvelopeFilename returns the filename for execution payload envelopes.
// [New in Gloas:EIP7732]
func getEnvelopeFilename(blockRoot common.Hash) string {
	return fmt.Sprintf("%x.envelope.snappy_ssz", blockRoot)
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
	if length > maxSSZBufferSize {
		return nil, fmt.Errorf("corrupt beacon state file: length %d exceeds max %d, root: %x", length, maxSSZBufferSize, blockRoot)
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

	// Re-initialize caches after SSZ decode. state.New() called InitBeaconState()
	// on an empty state; after DecodeSSZ populates real data, caches like
	// publicKeyIndicies are stale (empty). Reinitialize them from the decoded data.
	if err = bs.InitBeaconState(); err != nil {
		return nil, fmt.Errorf("failed to reinitialize caches after SSZ decode: %w, root: %x", err, blockRoot)
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
	// Fast path: check in-memory cache
	if _, ok := f.envelopeExists.Load(blockRoot); ok {
		return true
	}
	// Slow path: fall back to disk and populate cache on hit
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
	if envelopeLength > maxSSZBufferSize {
		return nil, fmt.Errorf("corrupt envelope file: length %d exceeds max %d, root: %x", envelopeLength, maxSSZBufferSize, blockRoot)
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

	envelope = &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(f.beaconCfg),
	}
	if err = envelope.DecodeSSZ(f.sszBuffer, int(clparams.GloasVersion)); err != nil {
		return nil, fmt.Errorf("failed to decode envelope: %w, root: %x, len: %d", err, blockRoot, n)
	}

	return
}

// DumpEnvelopeOnDisk dumps an execution payload envelope to disk.
// [New in Gloas:EIP7732]
func (f *forkGraphDisk) DumpEnvelopeOnDisk(blockRoot common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) (err error) {
	f.stateDumpLock.Lock()
	defer f.stateDumpLock.Unlock()

	// Populate in-memory cache on successful write
	defer func() {
		if err == nil {
			f.envelopeExists.Store(blockRoot, struct{}{})
		}
	}()

	// Encode the envelope
	f.sszBuffer, err = envelope.EncodeSSZ(f.sszBuffer[:0])
	if err != nil {
		return
	}

	dumpedFile, err := f.fs.OpenFile(getEnvelopeFilename(blockRoot), os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		return err
	}
	defer dumpedFile.Close()

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

	return
}
