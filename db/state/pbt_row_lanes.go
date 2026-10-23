// Copyright 2026 The Erigon Authors
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

package state

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

const (
	pbinRowLaneCount       = 48
	pbinRowLaneBufferBytes = 1 << 20
	pbinRowLaneLengthBytes = 4
)

type pbinRowLanes struct {
	tmpDir        string
	name          string
	lanes         map[pbinPartition]*pbinRowLane
	skip          *etl.Collector
	countRows     uint64
	skipCount     uint64
	laneByteCount uint64
	finalized     bool
	cleaned       bool
}

type pbinRowLane struct {
	partition pbinPartition
	path      string
	file      *os.File
	writer    *bufio.Writer
	last      []byte
}

type pbinRowLaneReader struct {
	partition pbinPartition
	file      *os.File
	reader    *bufio.Reader
}

type pbinRowLaneRow struct {
	key   []byte
	value []byte
}

type pbinRowLaneState struct {
	key   []byte
	value []byte
	has   bool
}

func newPBinRowLanes(tmpDir, name string) *pbinRowLanes {
	return &pbinRowLanes{
		tmpDir: tmpDir,
		name:   name,
		lanes:  make(map[pbinPartition]*pbinRowLane),
		skip:   etl.NewCollector("pbin-range-"+name+"-skip", tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), log.Root()).MergeInBackground(true),
	}
}

func (r *pbinRowLanes) collect(key, value []byte) (string, error) {
	r.countRows++
	if pbinRowLaneMustSkip(key) {
		r.skipCount++
		return "skip", r.skip.Collect(key, value)
	}
	partition := pbinPartitionOf(key)
	lane := r.lanes[partition]
	if lane == nil {
		if len(r.lanes) >= pbinRowLaneCount {
			return "", fmt.Errorf("pbin row lanes: range %s has more than %d partitions", r.name, pbinRowLaneCount)
		}
		var err error
		lane, err = r.openLane(partition)
		if err != nil {
			return "", err
		}
		r.lanes[partition] = lane
	}
	if len(lane.last) != 0 && bytes.Compare(key, lane.last) <= 0 {
		r.skipCount++
		return "skip", r.skip.Collect(key, value)
	}
	if err := lane.write(key, value); err != nil {
		return "", err
	}
	lane.last = bytes.Clone(key)
	r.laneByteCount += uint64(pbinRowLaneLengthBytes*2 + len(key) + len(value))
	return "lane", nil
}

func (r *pbinRowLanes) openLane(partition pbinPartition) (*pbinRowLane, error) {
	prefix := fmt.Sprintf("pbin-range-%s-lane-%02x-%02x-", r.name, partition[0], partition[1])
	file, err := os.CreateTemp(r.tmpDir, prefix)
	if err != nil {
		return nil, err
	}
	return &pbinRowLane{
		partition: partition,
		path:      file.Name(),
		file:      file,
		writer:    bufio.NewWriterSize(file, pbinRowLaneBufferBytes),
	}, nil
}

func (l *pbinRowLane) write(key, value []byte) error {
	var length [pbinRowLaneLengthBytes]byte
	if uint64(len(key)) > uint64(^uint32(0)) || uint64(len(value)) > uint64(^uint32(0)) {
		return fmt.Errorf("pbin row lane %02x-%02x: row is too large", l.partition[0], l.partition[1])
	}
	binary.BigEndian.PutUint32(length[:], uint32(len(key)))
	if _, err := l.writer.Write(length[:]); err != nil {
		return err
	}
	if _, err := l.writer.Write(key); err != nil {
		return err
	}
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	if _, err := l.writer.Write(length[:]); err != nil {
		return err
	}
	_, err := l.writer.Write(value)
	return err
}

func pbinRowLaneMustSkip(key []byte) bool {
	if bytes.Equal(key, commitment.KeyCommitmentState) || bytes.Equal(key, pbt.GlobalRootKey()) {
		return true
	}
	path, err := eip8297.DecodeBitPath(key)
	return err != nil || path.BitLen < pbinPartitionBits
}

func (r *pbinRowLanes) finalize() error {
	if r.finalized {
		return nil
	}
	var firstErr error
	for _, lane := range r.lanes {
		if err := lane.writer.Flush(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := lane.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	r.finalized = true
	return firstErr
}

func (r *pbinRowLanes) load(ctx context.Context, emit func([]byte, []byte) error) error {
	if err := r.finalize(); err != nil {
		return err
	}
	mergeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	skipRows := make(chan pbinRowLaneRow, 1)
	skipDone := make(chan error, 1)
	skipWaited := false
	go func() {
		err := r.skip.Load(nil, "", func(key, value []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
			row := pbinRowLaneRow{key: bytes.Clone(key), value: bytes.Clone(value)}
			select {
			case skipRows <- row:
				return nil
			case <-mergeCtx.Done():
				return mergeCtx.Err()
			}
		}, etl.TransformArgs{})
		close(skipRows)
		skipDone <- err
	}()
	defer func() {
		cancel()
		if !skipWaited {
			<-skipDone
		}
	}()

	skipRow, skipHas, skipErr := pbinNextSkipRow(mergeCtx, skipRows)
	if skipErr != nil {
		return skipErr
	}
	var previous []byte
	emitRow := func(key, value []byte) error {
		if previous != nil && bytes.Compare(key, previous) <= 0 {
			return fmt.Errorf("pbin range %s: lane merge is not strictly increasing at key %x after %x", r.name, key, previous)
		}
		if err := emit(key, value); err != nil {
			return err
		}
		previous = bytes.Clone(key)
		return nil
	}
	partitions := make([]pbinPartition, 0, len(r.lanes))
	for partition := range r.lanes {
		partitions = append(partitions, partition)
	}
	sort.Slice(partitions, func(i, j int) bool {
		return bytes.Compare(partitions[i][:], partitions[j][:]) < 0
	})
	readers := make([]*pbinRowLaneReader, 0, len(partitions))
	states := make([]pbinRowLaneState, 0, len(partitions))
	defer func() {
		for _, reader := range readers {
			_ = reader.close()
		}
	}()
	for _, partition := range partitions {
		reader, err := newPBinRowLaneReader(r.lanes[partition])
		if err != nil {
			return err
		}
		laneKey, laneValue, laneHas, readErr := reader.next()
		if readErr != nil {
			_ = reader.close()
			return readErr
		}
		readers = append(readers, reader)
		states = append(states, pbinRowLaneState{key: laneKey, value: laneValue, has: laneHas})
	}
	for skipHas || len(readers) != 0 {
		bestLane := -1
		var bestKey []byte
		var bestValue []byte
		if skipHas {
			bestKey, bestValue = skipRow.key, skipRow.value
		}
		for i := range states {
			if !states[i].has {
				continue
			}
			if bestKey != nil && bytes.Equal(states[i].key, bestKey) {
				return fmt.Errorf("pbin range %s: lane merge is not strictly increasing at key %x", r.name, bestKey)
			}
			if bestKey == nil || bytes.Compare(states[i].key, bestKey) < 0 {
				bestLane = i
				bestKey = states[i].key
				bestValue = states[i].value
			}
		}
		if bestKey == nil {
			break
		}
		if err := emitRow(bestKey, bestValue); err != nil {
			return err
		}
		if bestLane < 0 {
			skipRow, skipHas, skipErr = pbinNextSkipRow(mergeCtx, skipRows)
			if skipErr != nil {
				return skipErr
			}
			continue
		}
		states[bestLane].key, states[bestLane].value, states[bestLane].has, skipErr = readers[bestLane].next()
		if skipErr != nil {
			return skipErr
		}
	}
	skipErr = <-skipDone
	skipWaited = true
	return skipErr
}

func pbinNextSkipRow(ctx context.Context, rows <-chan pbinRowLaneRow) (pbinRowLaneRow, bool, error) {
	select {
	case row, ok := <-rows:
		if !ok {
			return pbinRowLaneRow{}, false, nil
		}
		return row, true, nil
	case <-ctx.Done():
		return pbinRowLaneRow{}, false, ctx.Err()
	}
}

func newPBinRowLaneReader(lane *pbinRowLane) (*pbinRowLaneReader, error) {
	file, err := os.Open(lane.path)
	if err != nil {
		return nil, err
	}
	return &pbinRowLaneReader{partition: lane.partition, file: file, reader: bufio.NewReaderSize(file, pbinRowLaneBufferBytes)}, nil
}

func (r *pbinRowLaneReader) next() ([]byte, []byte, bool, error) {
	var length [pbinRowLaneLengthBytes]byte
	n, err := io.ReadFull(r.reader, length[:])
	if err == io.EOF && n == 0 {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("pbin row lane %02x-%02x: read key length: %w", r.partition[0], r.partition[1], err)
	}
	key := make([]byte, binary.BigEndian.Uint32(length[:]))
	if _, err := io.ReadFull(r.reader, key); err != nil {
		return nil, nil, false, fmt.Errorf("pbin row lane %02x-%02x: read key: %w", r.partition[0], r.partition[1], err)
	}
	if _, err := io.ReadFull(r.reader, length[:]); err != nil {
		return nil, nil, false, fmt.Errorf("pbin row lane %02x-%02x: read value length: %w", r.partition[0], r.partition[1], err)
	}
	value := make([]byte, binary.BigEndian.Uint32(length[:]))
	if _, err := io.ReadFull(r.reader, value); err != nil {
		return nil, nil, false, fmt.Errorf("pbin row lane %02x-%02x: read value: %w", r.partition[0], r.partition[1], err)
	}
	return key, value, true, nil
}

func (r *pbinRowLaneReader) close() error {
	return r.file.Close()
}

func (r *pbinRowLanes) count() uint64 {
	return r.countRows
}

func (r *pbinRowLanes) skipRows() uint64 {
	return r.skipCount
}

func (r *pbinRowLanes) laneBytes() uint64 {
	return r.laneByteCount
}

func (r *pbinRowLanes) skipShare() float64 {
	if r.countRows == 0 {
		return 0
	}
	return float64(r.skipCount) / float64(r.countRows)
}

func (r *pbinRowLanes) cleanup() error {
	if r.cleaned {
		return nil
	}
	r.cleaned = true
	if r.skip != nil {
		r.skip.Close()
	}
	var firstErr error
	for _, lane := range r.lanes {
		if err := dir.RemoveFile(lane.path); err != nil && !errors.Is(err, os.ErrNotExist) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (r *pbinRowLanes) close() {
	if r.cleaned {
		return
	}
	for _, lane := range r.lanes {
		_ = lane.writer.Flush()
		_ = lane.file.Close()
	}
	_ = r.cleanup()
}
