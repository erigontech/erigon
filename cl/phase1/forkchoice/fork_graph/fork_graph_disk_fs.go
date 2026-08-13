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
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/golang/snappy"
	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
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
// Only envelopes successfully persisted or validated by this process are reported.
// [New in Gloas:EIP7732]
func (f *forkGraphDisk) HasEnvelope(blockRoot common.Hash) bool {
	if _, invalid := f.invalidEnvelopes.Load(blockRoot); invalid {
		return false
	}
	if !f.knowsBlockRoot(blockRoot) {
		return false
	}
	_, ok := f.envelopeExists.Load(blockRoot)
	return ok
}

func (f *forkGraphDisk) knowsBlockRoot(blockRoot common.Hash) bool {
	if blockRoot == f.anchorRoot {
		return true
	}
	_, ok := f.blocks.Load(blockRoot)
	return ok
}

// ReadEnvelopeFromDisk reads an execution payload envelope from disk.
// [New in Gloas:EIP7732]
func (f *forkGraphDisk) ReadEnvelopeFromDisk(blockRoot common.Hash) (envelope *cltypes.SignedExecutionPayloadEnvelope, err error) {
	f.stateDumpLock.Lock()
	defer f.stateDumpLock.Unlock()
	return f.readEnvelopeFromDiskLocked(blockRoot)
}

func (f *forkGraphDisk) readEnvelopeFromDiskLocked(blockRoot common.Hash) (envelope *cltypes.SignedExecutionPayloadEnvelope, err error) {
	var file afero.File
	var corrupt bool
	if _, invalid := f.invalidEnvelopes.Load(blockRoot); invalid {
		return nil, fmt.Errorf("cannot read known invalid envelope for root %x", blockRoot)
	}
	if !f.knowsBlockRoot(blockRoot) {
		return nil, fmt.Errorf("cannot read envelope for unknown block root %x", blockRoot)
	}

	filename := getEnvelopeFilename(blockRoot)
	file, err = f.fs.Open(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			f.envelopeExists.Delete(blockRoot)
		}
		return
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Warn("failed to close envelope after read", "root", blockRoot, "err", closeErr)
		}
		if corrupt {
			f.invalidEnvelopes.Store(blockRoot, struct{}{})
			f.envelopeExists.Delete(blockRoot)
		}
	}()

	readTracker := &envelopeReadTracker{Reader: file}
	if f.envelopeSnappyReader == nil {
		f.envelopeSnappyReader = snappy.NewReader(readTracker)
	} else {
		f.envelopeSnappyReader.Reset(readTracker)
	}
	versionBytes := []byte{0}
	if _, err = io.ReadFull(f.envelopeSnappyReader, versionBytes); err != nil {
		corrupt = isCorruptEnvelopeReadError(err, readTracker.err)
		return nil, fmt.Errorf("failed to read envelope version: %w, root: %x", err, blockRoot)
	}
	version := clparams.StateVersion(versionBytes[0])
	if versionErr := cltypes.ValidateExecutionPayloadEnvelopeVersion(version); versionErr != nil {
		corrupt = true
		return nil, fmt.Errorf("corrupt envelope file: %w, root: %x", versionErr, blockRoot)
	}

	// Read the length
	lengthBytes := make([]byte, 8)
	_, err = io.ReadFull(f.envelopeSnappyReader, lengthBytes)
	if err != nil {
		corrupt = isCorruptEnvelopeReadError(err, readTracker.err)
		return nil, fmt.Errorf("failed to read length: %w, root: %x", err, blockRoot)
	}
	envelopeLength := binary.BigEndian.Uint64(lengthBytes)
	if envelopeLength > clparams.MaxChunkSize {
		corrupt = true
		return nil, fmt.Errorf("corrupt envelope file: length %d exceeds max %d, root: %x", envelopeLength, clparams.MaxChunkSize, blockRoot)
	}
	if envelopeLength > uint64(cap(f.sszBuffer)) {
		f.sszBuffer = make([]byte, envelopeLength)
	} else {
		f.sszBuffer = f.sszBuffer[:envelopeLength]
	}
	n, err := io.ReadFull(f.envelopeSnappyReader, f.sszBuffer)
	if err != nil {
		corrupt = isCorruptEnvelopeReadError(err, readTracker.err)
		return nil, fmt.Errorf("failed to read snappy buffer: %w, root: %x", err, blockRoot)
	}
	var trailing [1]byte
	if _, readErr := io.ReadFull(f.envelopeSnappyReader, trailing[:]); readErr == nil {
		corrupt = true
		return nil, fmt.Errorf("corrupt envelope file: trailing data after declared payload, root: %x", blockRoot)
	} else if !errors.Is(readErr, io.EOF) {
		corrupt = isCorruptEnvelopeReadError(readErr, readTracker.err)
		return nil, fmt.Errorf("failed to verify envelope end: %w, root: %x", readErr, blockRoot)
	}
	envelope = &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelopeWithVersion(f.beaconCfg, version),
	}
	if err = envelope.DecodeSSZStrict(f.sszBuffer, int(version)); err != nil {
		corrupt = true
		return nil, fmt.Errorf("failed to decode envelope: %w, root: %x, len: %d", err, blockRoot, n)
	}
	if err = envelope.ValidateForConfig(f.beaconCfg); err != nil {
		corrupt = true
		return nil, fmt.Errorf("invalid persisted envelope: %w, root: %x", err, blockRoot)
	}
	if err = envelope.ValidateForPersistence(f.beaconCfg); err != nil {
		corrupt = true
		return nil, fmt.Errorf("invalid persisted envelope: %w, root: %x", err, blockRoot)
	}
	if envelope.Message.BeaconBlockRoot != blockRoot {
		corrupt = true
		return nil, fmt.Errorf("corrupt envelope file: embedded root %x does not match filename root %x", envelope.Message.BeaconBlockRoot, blockRoot)
	}
	transactions := envelope.Message.Payload.Transactions.UnderlyngReference()
	ownedTransactions := make([][]byte, len(transactions))
	for i, transaction := range transactions {
		ownedTransactions[i] = bytes.Clone(transaction)
	}
	// TransactionsSSZ decode aliases its input, so detach it before reusing the shared buffer.
	envelope.Message.Payload.Transactions = solid.NewTransactionsSSZFromTransactionsWithLimits(
		ownedTransactions,
		f.beaconCfg.MaxTransactionsPerPayload,
		f.beaconCfg.MaxBytesPerTransaction,
	)
	f.envelopeExists.Store(blockRoot, struct{}{})
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
	if validateErr := envelope.ValidateForConfig(f.beaconCfg); validateErr != nil {
		return fmt.Errorf("cannot persist invalid envelope: %w", validateErr)
	}
	if validateErr := envelope.ValidateForPersistence(f.beaconCfg); validateErr != nil {
		return fmt.Errorf("cannot persist invalid envelope: %w", validateErr)
	}
	if envelope.Message.BeaconBlockRoot != blockRoot {
		return fmt.Errorf("cannot persist envelope for root %x with embedded root %x", blockRoot, envelope.Message.BeaconBlockRoot)
	}
	f.stateDumpLock.Lock()
	defer f.stateDumpLock.Unlock()
	if !f.knowsBlockRoot(blockRoot) {
		return fmt.Errorf("cannot persist envelope for unknown block root %x", blockRoot)
	}

	// Populate in-memory cache on successful write
	defer func() {
		if err == nil {
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
	if _, err := f.sszSnappyWriter.Write([]byte{byte(envelope.Message.Payload.Version())}); err != nil {
		log.Error("failed to write envelope version", "err", err)
		return err
	}
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
	err = dumpedFile.Close()
	closed = true
	if err != nil {
		return
	}
	if err = f.fs.Rename(tempFilename, filename); err != nil {
		return
	}

	return
}
