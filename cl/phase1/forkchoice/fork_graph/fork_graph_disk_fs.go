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

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/snappypool"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

// maxSSZObjectSize leaves room for beacon-state growth while rejecting corrupt lengths before allocation.
const maxSSZObjectSize = 1 << 30 // 1 GiB

func getBeaconStateFilename(blockRoot common.Hash) string {
	return fmt.Sprintf("%x.snappy_ssz", blockRoot)
}

func getBeaconStateTempFilename(blockRoot common.Hash) string {
	return getBeaconStateFilename(blockRoot) + ".tmp"
}

// getEnvelopeFilename returns the filename for execution payload envelopes.
// [New in Gloas:EIP7732]
func getEnvelopeFilename(blockRoot common.Hash) string {
	return fmt.Sprintf("%x.envelope.snappy_ssz", blockRoot)
}

func getEnvelopeTempFilename(blockRoot common.Hash) string {
	return getEnvelopeFilename(blockRoot) + ".tmp"
}

func (f *forkGraphDisk) readBeaconStateFromDisk(blockRoot common.Hash) (*state.CachingBeaconState, error) {
	bs, prevRoot, err := f.decodeBeaconStateFile(blockRoot)
	if err != nil {
		return nil, err
	}
	// Take the head view only after releasing stateDumpLock: states are dumped while the head view is held.
	shared, err := bs.InitBeaconStateSharing(f.viewHeadState)
	if err != nil {
		return nil, fmt.Errorf("failed to init beacon state: %w, root: %x", err, blockRoot)
	}
	sharedValidatorChunksCounter.AddInt(shared)
	if prevRoot != nil {
		bs.SetPreviousStateRoot(*prevRoot)
	}
	return bs, nil
}

func (f *forkGraphDisk) viewHeadState(use func(head *state.CachingBeaconState)) {
	if f.syncedData == nil {
		return
	}
	_ = f.syncedData.ViewHeadState(func(head *state.CachingBeaconState) error {
		use(head)
		return nil
	})
}

func (f *forkGraphDisk) decodeBeaconStateFile(blockRoot common.Hash) (bs *state.CachingBeaconState, prevRoot *common.Hash, err error) {
	var file afero.File
	f.stateDumpLock.Lock()
	defer f.stateDumpLock.Unlock()

	file, err = f.fs.Open(getBeaconStateFilename(blockRoot))
	if err != nil {
		return
	}
	defer file.Close()

	sr := snappypool.Reader(file)
	defer snappypool.PutReader(sr)
	var version [1]byte
	if _, err := io.ReadFull(sr, version[:]); err != nil {
		return nil, nil, fmt.Errorf("failed to read hard fork version: %w, root: %x", err, blockRoot)
	}
	var lengthBytes [8]byte
	if _, err := io.ReadFull(sr, lengthBytes[:]); err != nil {
		return nil, nil, fmt.Errorf("failed to read length: %w, root: %x", err, blockRoot)
	}
	length := binary.BigEndian.Uint64(lengthBytes[:])
	if length > maxSSZObjectSize {
		return nil, nil, fmt.Errorf("corrupt beacon state file: length %d exceeds max %d, root: %x", length, maxSSZObjectSize, blockRoot)
	}
	bs = state.New(f.beaconCfg)
	if err = bs.BeaconState.DecodeCanonicalSSZFrom(sr, int(length), int(version[0])); err != nil {
		return nil, nil, fmt.Errorf("failed to decode beacon state: %w, root: %x, len: %d", err, blockRoot, length)
	}
	decodedStateSizeHistogram.Observe(float64(length))

	// Try to read the persisted previousStateRoot (appended after SSZ data).
	// This is needed for GLOAS where the execution payload envelope modifies
	// the state after TransitionState, making HashSSZ() diverge from
	// the block's state_root. Older state files won't have this field;
	// in that case we leave previousStateRoot as zero (HashSSZ fallback).
	var root common.Hash
	if _, readErr := io.ReadFull(sr, root[:]); readErr == nil {
		prevRoot = &root
	}

	return
}

// fullWriter turns a short write without an error into io.ErrShortWrite, which snappy.Writer does not check.
type fullWriter struct{ w io.Writer }

func (w fullWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if err == nil && n < len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

// DumpBeaconStateOnDisk streams a beacon state to disk in ssz snappy format.
// The file is written under a temporary name and renamed, so a failed dump keeps the previous file.
func (f *forkGraphDisk) DumpBeaconStateOnDisk(blockRoot common.Hash, bs *state.CachingBeaconState, forced bool) (err error) {
	if !forced && bs.Slot()%dumpSlotFrequency != 0 {
		return
	}
	f.stateDumpLock.Lock()
	defer f.stateDumpLock.Unlock()
	slot := bs.Slot()
	if blockRoot == f.lastDumpedRoot && slot == f.lastDumpedSlot {
		return nil
	}
	f.lastDumpedRoot, f.lastDumpedSlot = common.Hash{}, 0

	filename := getBeaconStateFilename(blockRoot)
	tempFilename := getBeaconStateTempFilename(blockRoot)
	dumpedFile, err := f.fs.OpenFile(tempFilename, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0o755)
	if err != nil {
		return err
	}
	closed, renamed := false, false
	defer func() {
		if !closed {
			err = errors.Join(err, dumpedFile.Close())
		}
		if !renamed {
			_ = f.fs.Remove(tempFilename)
		}
	}()

	if err := f.writeBeaconState(dumpedFile, blockRoot, bs); err != nil {
		return err
	}
	closed = true
	if err := dumpedFile.Close(); err != nil {
		return err
	}
	if err := f.fs.Rename(tempFilename, filename); err != nil {
		return err
	}
	renamed = true
	f.lastDumpedRoot, f.lastDumpedSlot = blockRoot, slot
	return nil
}

func (f *forkGraphDisk) writeBeaconState(file io.Writer, blockRoot common.Hash, bs *state.CachingBeaconState) error {
	sw := snappypool.Writer(fullWriter{file})
	defer snappypool.PutWriter(sw)

	length := bs.EncodingSizeSSZ()
	var header [9]byte
	header[0] = byte(bs.Version())
	binary.BigEndian.PutUint64(header[1:], uint64(length))
	if _, err := sw.Write(header[:]); err != nil {
		log.Error("failed to write state header", "err", err)
		return err
	}
	if err := bs.EncodeSSZTo(sw); err != nil {
		log.Error("failed to write ssz state", "err", err)
		return err
	}
	metrics.NewHistTimer("encode_ssz_beacon_state_size").Observe(float64(length))
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
	if _, err := sw.Write(stateRootToWrite[:]); err != nil {
		log.Error("failed to write previousStateRoot", "err", err)
		return err
	}
	if err := sw.Flush(); err != nil {
		log.Error("failed to flush snappy writer", "err", err)
		return err
	}
	return nil
}

// HasEnvelope reports cached envelope availability for a retained root.
// [New in Gloas:EIP7732]
func (f *forkGraphDisk) HasEnvelope(blockRoot common.Hash) bool {
	f.lifecycleMu.RLock()
	defer f.lifecycleMu.RUnlock()
	if !f.retainedBlock(blockRoot) || f.IsEnvelopeRejected(blockRoot) {
		return false
	}
	if _, invalid := f.invalidEnvelopes.Load(blockRoot); invalid {
		return false
	}
	_, ok := f.envelopeExists.Load(blockRoot)
	return ok
}

func (f *forkGraphDisk) RejectEnvelope(blockRoot common.Hash) bool {
	f.lifecycleMu.RLock()
	defer f.lifecycleMu.RUnlock()
	if !f.retainedBlock(blockRoot) {
		return false
	}
	f.envelopeMu.Lock()
	defer f.envelopeMu.Unlock()
	f.rejectedEnvelopes.Store(blockRoot, struct{}{})
	f.envelopeExists.Delete(blockRoot)
	f.ClearPayloadAccepted(blockRoot)
	f.MarkPayloadUnavailable(blockRoot)
	return true
}

func (f *forkGraphDisk) IsEnvelopeRejected(blockRoot common.Hash) bool {
	_, rejected := f.rejectedEnvelopes.Load(blockRoot)
	return rejected
}

// ReadEnvelopeFromDisk reads an execution payload envelope from disk.
// [New in Gloas:EIP7732]
func (f *forkGraphDisk) ReadEnvelopeFromDisk(blockRoot common.Hash) (envelope *cltypes.SignedExecutionPayloadEnvelope, err error) {
	f.lifecycleMu.RLock()
	defer f.lifecycleMu.RUnlock()
	if !f.retainedBlock(blockRoot) {
		return nil, ErrStateNotFound
	}
	f.envelopeMu.Lock()
	defer f.envelopeMu.Unlock()
	return f.readEnvelopeFromDiskLocked(blockRoot)
}

func (f *forkGraphDisk) readEnvelopeFromDiskLocked(blockRoot common.Hash) (envelope *cltypes.SignedExecutionPayloadEnvelope, err error) {
	var file afero.File
	var corrupt bool
	if f.IsEnvelopeRejected(blockRoot) {
		return nil, fmt.Errorf("execution payload envelope rejected for root %x", blockRoot)
	}
	if _, invalid := f.invalidEnvelopes.Load(blockRoot); invalid {
		return nil, fmt.Errorf("cannot read known invalid envelope for root %x", blockRoot)
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
	sr := snappypool.Reader(readTracker)
	defer snappypool.PutReader(sr)

	versionBytes := []byte{0}
	if _, err = io.ReadFull(sr, versionBytes); err != nil {
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
	_, err = io.ReadFull(sr, lengthBytes)
	if err != nil {
		corrupt = isCorruptEnvelopeReadError(err, readTracker.err)
		return nil, fmt.Errorf("failed to read length: %w, root: %x", err, blockRoot)
	}
	envelopeLength := binary.BigEndian.Uint64(lengthBytes)
	if envelopeLength > clparams.MaxChunkSize {
		corrupt = true
		return nil, fmt.Errorf("corrupt envelope file: length %d exceeds max %d, root: %x", envelopeLength, clparams.MaxChunkSize, blockRoot)
	}
	if envelopeLength > uint64(cap(f.envelopeSSZBuffer)) {
		f.envelopeSSZBuffer = make([]byte, envelopeLength)
	} else {
		f.envelopeSSZBuffer = f.envelopeSSZBuffer[:envelopeLength]
	}
	n, err := io.ReadFull(sr, f.envelopeSSZBuffer)
	if err != nil {
		corrupt = isCorruptEnvelopeReadError(err, readTracker.err)
		return nil, fmt.Errorf("failed to read snappy buffer: %w, root: %x", err, blockRoot)
	}
	var trailing [1]byte
	if _, readErr := io.ReadFull(sr, trailing[:]); readErr == nil {
		corrupt = true
		return nil, fmt.Errorf("corrupt envelope file: trailing data after declared payload, root: %x", blockRoot)
	} else if !errors.Is(readErr, io.EOF) {
		corrupt = isCorruptEnvelopeReadError(readErr, readTracker.err)
		return nil, fmt.Errorf("failed to verify envelope end: %w, root: %x", readErr, blockRoot)
	}
	envelope = &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelopeWithVersion(f.beaconCfg, version),
	}
	if err = envelope.DecodeSSZStrict(f.envelopeSSZBuffer, int(version)); err != nil {
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
	envelope.Message.Payload.Transactions = solid.NewProgressiveTransactionsSSZFromTransactions(ownedTransactions)
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
	envelopeSize := envelope.EncodingSizeSSZ()
	if envelopeSize < 0 || uint64(envelopeSize) > clparams.MaxChunkSize {
		return fmt.Errorf("cannot persist envelope: length %d exceeds max %d", envelopeSize, clparams.MaxChunkSize)
	}
	f.lifecycleMu.RLock()
	defer f.lifecycleMu.RUnlock()
	header, ok := f.GetHeader(blockRoot)
	if !ok || header == nil {
		return fmt.Errorf("cannot dump envelope for unknown block root %x", blockRoot)
	}
	if blockRoot != f.anchorRoot && isBelowPrunedBoundary(header.Slot, f.lowestAvailableBlock.Load()) {
		return fmt.Errorf("cannot dump envelope for pruned block root %x at slot %d", blockRoot, header.Slot)
	}
	f.envelopeMu.Lock()
	defer f.envelopeMu.Unlock()

	if f.IsEnvelopeRejected(blockRoot) {
		return fmt.Errorf("execution payload envelope rejected for root %x", blockRoot)
	}

	// Populate in-memory cache on successful write
	defer func() {
		if err == nil {
			f.envelopeExists.Store(blockRoot, struct{}{})
			f.envelopeMissing.Delete(blockRoot)
			f.invalidEnvelopes.Delete(blockRoot)
		}
	}()

	// Encode the envelope
	f.envelopeSSZBuffer, err = envelope.EncodeSSZ(f.envelopeSSZBuffer[:0])
	if err != nil {
		return
	}
	if uint64(len(f.envelopeSSZBuffer)) > clparams.MaxChunkSize {
		return fmt.Errorf("cannot persist envelope: length %d exceeds max %d", len(f.envelopeSSZBuffer), clparams.MaxChunkSize)
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
			err = errors.Join(err, dumpedFile.Close())
		}
		if err != nil {
			f.envelopeExists.Delete(blockRoot)
			_ = f.fs.Remove(tempFilename)
		}
	}()

	sw := snappypool.Writer(dumpedFile)
	err = func() error {
		defer snappypool.PutWriter(sw)
		length := make([]byte, 8)
		binary.BigEndian.PutUint64(length, uint64(len(f.envelopeSSZBuffer)))
		if _, writeErr := sw.Write([]byte{byte(envelope.Message.Payload.Version())}); writeErr != nil {
			log.Error("failed to write envelope version", "err", writeErr)
			return writeErr
		}
		if _, writeErr := sw.Write(length); writeErr != nil {
			log.Error("failed to write length", "err", writeErr)
			return writeErr
		}
		if _, writeErr := sw.Write(f.envelopeSSZBuffer); writeErr != nil {
			log.Error("failed to write ssz buffer", "err", writeErr)
			return writeErr
		}
		if flushErr := sw.Flush(); flushErr != nil {
			log.Error("failed to flush snappy writer", "err", flushErr)
			return flushErr
		}
		return nil
	}()
	if err != nil {
		return err
	}

	if err = dumpedFile.Close(); err != nil {
		closed = true
		return
	}
	closed = true
	if err = f.fs.Rename(tempFilename, filename); err != nil {
		return
	}

	return
}
