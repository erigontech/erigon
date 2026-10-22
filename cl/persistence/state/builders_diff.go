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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

const (
	buildersDiffHeaderSize = historyRowPreambleSize + 8 + 4
	builderSSZSize         = 93
	buildersDiffEntrySize  = 8 + builderSSZSize
	buildersDiffKind       = byte(1)
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

	row := appendHistoryRowPreamble(make([]byte, 0, buildersDiffHeaderSize), buildersDiffKind)
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
	binary.LittleEndian.PutUint32(row[historyRowPreambleSize+8:], uint32(count))

	_, err := dst.Write(row)
	return err
}

func ApplyBuildersDiff(row []byte, builders *solid.ListSSZ[*cltypes.Builder], registryLimit uint64) error {
	kind, body, err := parseHistoryRow(row, "builders diff")
	if err != nil {
		return err
	}
	if kind != buildersDiffKind {
		return rebuildRequired("unsupported historical state row kind %d", kind)
	}
	if len(row) < buildersDiffHeaderSize {
		return rebuildRequired("truncated builders diff header")
	}

	newLength := binary.LittleEndian.Uint64(body)
	count := binary.LittleEndian.Uint32(body[8:])
	expectedLength := uint64(buildersDiffHeaderSize) + uint64(count)*uint64(buildersDiffEntrySize)
	if uint64(len(row)) != expectedLength {
		return rebuildRequired("builders diff length is %d, want %d", len(row), expectedLength)
	}
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
