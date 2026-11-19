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

package state_accessors

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

const (
	buildersDiffPreambleSize = 14
	buildersDiffHeaderSize   = buildersDiffPreambleSize + 8 + 4
	builderSSZSize           = 93
	buildersDiffEntrySize    = 8 + builderSSZSize
	buildersDiffVersion      = byte(1)
	buildersDiffKind         = byte(1)
)

var (
	buildersDiffPreamble      = [buildersDiffPreambleSize]byte{0, 0, 0, 0, 0, 0, 0, 0, 'G', 'L', 'O', 'A', buildersDiffVersion, buildersDiffKind}
	ErrHistoryRebuildRequired = errors.New("historical state row needs a rebuild")
)

type BuildersDiffWriter struct {
	baseline []cltypes.Builder
}

func (w *BuildersDiffWriter) Initialize(pre *solid.ListSSZ[*cltypes.Builder]) {
	w.baseline = w.baseline[:0]
	for i := range pre.Len() {
		w.baseline = append(w.baseline, *pre.Get(i))
	}
}

func (w *BuildersDiffWriter) WriteDiff(dst io.Writer, post *solid.ListSSZ[*cltypes.Builder]) error {
	if post.Len() < len(w.baseline) {
		return fmt.Errorf("builders registry shrank from %d to %d", len(w.baseline), post.Len())
	}

	row := append(make([]byte, 0, buildersDiffHeaderSize), buildersDiffPreamble[:]...)
	row = binary.LittleEndian.AppendUint64(row, uint64(post.Len()))
	row = binary.LittleEndian.AppendUint32(row, 0)
	count := 0
	for i := range post.Len() {
		builder := post.Get(i)
		if i < len(w.baseline) && *builder == w.baseline[i] {
			continue
		}
		count++
		row = binary.LittleEndian.AppendUint64(row, uint64(i))
		var err error
		if row, err = builder.EncodeSSZ(row); err != nil {
			return fmt.Errorf("encode builder at index %d: %w", i, err)
		}
	}
	binary.LittleEndian.PutUint32(row[buildersDiffPreambleSize+8:], uint32(count))

	_, err := dst.Write(row)
	return err
}

func CheckBuildersDiffRow(row []byte) error {
	if len(row) < buildersDiffPreambleSize {
		return rebuildRequired("row shorter than preamble")
	}
	if binary.LittleEndian.Uint32(row[:4]) != 0 || binary.LittleEndian.Uint32(row[4:8]) != 0 {
		return rebuildRequired("legacy row preamble")
	}
	if !bytes.Equal(row[8:12], buildersDiffPreamble[8:12]) {
		return rebuildRequired("unknown row magic")
	}
	if row[12] != buildersDiffVersion {
		return rebuildRequired("unsupported builders diff version %d", row[12])
	}
	if row[13] != buildersDiffKind {
		return rebuildRequired("unsupported historical state row kind %d", row[13])
	}
	if len(row) < buildersDiffHeaderSize {
		return rebuildRequired("truncated builders diff header")
	}

	count := binary.LittleEndian.Uint32(row[buildersDiffPreambleSize+8:])
	expectedLength := uint64(buildersDiffHeaderSize) + uint64(count)*uint64(buildersDiffEntrySize)
	if uint64(len(row)) != expectedLength {
		return rebuildRequired("builders diff length is %d, want %d", len(row), expectedLength)
	}
	return nil
}

func ApplyBuildersDiff(row []byte, builders *solid.ListSSZ[*cltypes.Builder], registryLimit uint64) error {
	if err := CheckBuildersDiffRow(row); err != nil {
		return err
	}
	newLength := binary.LittleEndian.Uint64(row[buildersDiffPreambleSize:])
	count := binary.LittleEndian.Uint32(row[buildersDiffPreambleSize+8:])
	currentLength := uint64(builders.Len())
	if newLength > registryLimit {
		return rebuildRequired("builders length %d exceeds registry limit %d", newLength, registryLimit)
	}
	if newLength < currentLength {
		return rebuildRequired("builders registry shrank from %d to %d", currentLength, newLength)
	}

	type update struct {
		index   uint64
		builder *cltypes.Builder
	}
	updates := make([]update, 0, count)
	appendIndex := currentLength
	var previous uint64
	for i := range count {
		offset := buildersDiffHeaderSize + int(i)*buildersDiffEntrySize
		index := binary.LittleEndian.Uint64(row[offset:])
		if i > 0 && index <= previous {
			return rebuildRequired("builder indices are not strictly increasing")
		}
		if index >= newLength {
			return rebuildRequired("builder index %d is outside length %d", index, newLength)
		}
		if index >= currentLength {
			if index != appendIndex {
				return rebuildRequired("builders append starts at index %d, want %d", index, appendIndex)
			}
			appendIndex++
		}
		builder := &cltypes.Builder{}
		if err := builder.DecodeSSZ(row[offset+8:offset+buildersDiffEntrySize], 0); err != nil {
			return rebuildRequired("decode builder at index %d: %v", index, err)
		}
		updates = append(updates, update{index: index, builder: builder})
		previous = index
	}
	if appendIndex != newLength {
		return rebuildRequired("builders append ends at length %d, want %d", appendIndex, newLength)
	}

	for _, update := range updates {
		if update.index < currentLength {
			builders.Set(int(update.index), update.builder)
		} else {
			builders.Append(update.builder)
		}
	}
	return nil
}

func rebuildRequired(reason string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrHistoryRebuildRequired, fmt.Sprintf(reason, args...))
}
