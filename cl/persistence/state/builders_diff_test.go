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
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
)

const testBuilderRegistryLimit = 1024

func buildersList(values ...uint64) *solid.ListSSZ[*cltypes.Builder] {
	builders := solid.NewStaticListSSZ[*cltypes.Builder](testBuilderRegistryLimit, new(cltypes.Builder).EncodingSizeSSZ())
	for i, value := range values {
		builders.Append(&cltypes.Builder{Balance: value, DepositEpoch: uint64(i + 10)})
	}
	return builders
}

func TestBuildersDiffRoundTrip(t *testing.T) {
	tests := []struct {
		name          string
		pre           []uint64
		post          []uint64
		expectedCount int
	}{
		{name: "no change", pre: []uint64{1, 2, 3}, post: []uint64{1, 2, 3}, expectedCount: 0},
		{name: "first changed", pre: []uint64{1, 2, 3}, post: []uint64{10, 2, 3}, expectedCount: 1},
		{name: "middle changed", pre: []uint64{1, 2, 3}, post: []uint64{1, 20, 3}, expectedCount: 1},
		{name: "last changed", pre: []uint64{1, 2, 3}, post: []uint64{1, 2, 30}, expectedCount: 1},
		{name: "several changed", pre: []uint64{1, 2, 3}, post: []uint64{10, 2, 30}, expectedCount: 2},
		{name: "index reused", pre: []uint64{1, 2}, post: []uint64{1, 99}, expectedCount: 1},
		{name: "append", pre: []uint64{1, 2}, post: []uint64{1, 2, 3}, expectedCount: 1},
		{name: "reuse and append", pre: []uint64{1, 2}, post: []uint64{10, 2, 3, 4}, expectedCount: 3},
		{name: "empty to one", post: []uint64{1}, expectedCount: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pre := buildersList(test.pre...)
			post := buildersList(test.post...)
			writer := new(BuildersDiffWriter)
			writer.Initialize(pre)

			var row bytes.Buffer
			require.NoError(t, writer.WriteDiff(&row, post))
			require.Len(t, row.Bytes(), 26+101*test.expectedCount)

			got := buildersList(test.pre...)
			require.NoError(t, ApplyBuildersDiff(row.Bytes(), got, testBuilderRegistryLimit))
			require.Equal(t, post.Len(), got.Len())
			for i := 0; i < post.Len(); i++ {
				require.Equal(t, *post.Get(i), *got.Get(i))
			}
		})
	}
}

func TestBuildersDiffKeepsSingleFieldChanges(t *testing.T) {
	changes := map[string]func(*cltypes.Builder){
		"pubkey":             func(b *cltypes.Builder) { b.Pubkey[47] = 0xff },
		"version":            func(b *cltypes.Builder) { b.Version++ },
		"execution address":  func(b *cltypes.Builder) { b.ExecutionAddress[19] = 0xff },
		"deposit epoch":      func(b *cltypes.Builder) { b.DepositEpoch++ },
		"withdrawable epoch": func(b *cltypes.Builder) { b.WithdrawableEpoch++ },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			pre := buildersList(1, 2, 3)
			post := buildersList(1, 2, 3)
			changed := *post.Get(1)
			change(&changed)
			post.Set(1, &changed)

			writer := new(BuildersDiffWriter)
			writer.Initialize(pre)
			var row bytes.Buffer
			require.NoError(t, writer.WriteDiff(&row, post))
			require.Len(t, row.Bytes(), 26+101)

			require.NoError(t, ApplyBuildersDiff(row.Bytes(), pre, testBuilderRegistryLimit))
			require.Equal(t, changed, *pre.Get(1))
		})
	}
}

func builderBalances(builders *solid.ListSSZ[*cltypes.Builder]) []uint64 {
	values := make([]uint64, builders.Len())
	for i := range values {
		values[i] = builders.Get(i).Balance
	}
	return values
}

func TestBuildersDiffLayout(t *testing.T) {
	post := buildersList(17)
	writer := new(BuildersDiffWriter)
	writer.Initialize(buildersList())
	var row bytes.Buffer
	require.NoError(t, writer.WriteDiff(&row, post))

	expected := []byte{0, 0, 0, 0, 0, 0, 0, 0, 'G', 'L', 'O', 'A', 1, 1}
	expected = binary.LittleEndian.AppendUint64(expected, 1)
	expected = binary.LittleEndian.AppendUint32(expected, 1)
	expected = binary.LittleEndian.AppendUint64(expected, 0)
	builderSSZ := make([]byte, 93)
	binary.LittleEndian.PutUint64(builderSSZ[69:], 17)
	binary.LittleEndian.PutUint64(builderSSZ[77:], 10)
	expected = append(expected, builderSSZ...)
	require.Equal(t, expected, row.Bytes())
}

func TestBuildersDiffWriterCopiesBaselineValues(t *testing.T) {
	pre := buildersList(1)
	writer := new(BuildersDiffWriter)
	writer.Initialize(pre)
	pre.Get(0).Balance = 2

	var row bytes.Buffer
	require.NoError(t, writer.WriteDiff(&row, pre))
	require.Len(t, row.Bytes(), 26+101)

	got := buildersList(1)
	require.NoError(t, ApplyBuildersDiff(row.Bytes(), got, testBuilderRegistryLimit))
	require.Equal(t, []uint64{2}, builderBalances(got))
}

func TestBuildersDiffWriterRejectsShrink(t *testing.T) {
	writer := new(BuildersDiffWriter)
	writer.Initialize(buildersList(1, 2))
	require.Error(t, writer.WriteDiff(io.Discard, buildersList(1)))
}

func TestApplyBuildersDiffRejectsInvalidRowsWithoutMutation(t *testing.T) {
	pre := buildersList(1, 2)
	valid := buildersDiffRow(t, pre, buildersList(10, 20, 30, 40))
	legacy := legacyBuildersDiffRow(t, pre, buildersList(10, 20))

	tests := []struct {
		name string
		row  func() []byte
	}{
		{name: "legacy row", row: func() []byte { return bytes.Clone(legacy) }},
		{name: "short preamble", row: func() []byte { return bytes.Clone(valid[:13]) }},
		{name: "wrong magic", row: func() []byte { row := bytes.Clone(valid); row[8] = 'X'; return row }},
		{name: "nonzero first word", row: func() []byte { row := bytes.Clone(valid); row[0] = 1; return row }},
		{name: "nonzero second word", row: func() []byte { row := bytes.Clone(valid); row[4] = 1; return row }},
		{name: "unknown version", row: func() []byte { row := bytes.Clone(valid); row[12] = 2; return row }},
		{name: "unknown kind", row: func() []byte { row := bytes.Clone(valid); row[13] = 2; return row }},
		{name: "truncated header", row: func() []byte { return bytes.Clone(valid[:20]) }},
		{name: "truncated index", row: func() []byte { return bytes.Clone(valid[:30]) }},
		{name: "truncated builder", row: func() []byte { return bytes.Clone(valid[:80]) }},
		{name: "truncated final entry", row: func() []byte { return bytes.Clone(valid[:len(valid)-1]) }},
		{name: "trailing byte", row: func() []byte { return append(bytes.Clone(valid), 0) }},
		{name: "duplicate index", row: func() []byte {
			row := bytes.Clone(valid)
			copy(row[26+101:26+101+8], row[26:26+8])
			return row
		}},
		{name: "decreasing index", row: func() []byte {
			row := bytes.Clone(valid)
			binary.LittleEndian.PutUint64(row[26:], 1)
			binary.LittleEndian.PutUint64(row[26+101:], 0)
			return row
		}},
		{name: "index at new length", row: func() []byte {
			row := buildersDiffRow(t, pre, buildersList(1, 20))
			binary.LittleEndian.PutUint64(row[26:], 2)
			return row
		}},
		{name: "length above registry limit", row: func() []byte {
			row := buildersDiffRow(t, pre, pre)
			binary.LittleEndian.PutUint64(row[14:], testBuilderRegistryLimit+1)
			return row
		}},
		{name: "append starts after current length", row: func() []byte {
			row := buildersDiffRow(t, pre, buildersList(1, 2, 3, 4))
			copy(row[26:26+101], row[26+101:26+202])
			binary.LittleEndian.PutUint32(row[22:], 1)
			return row[:26+101]
		}},
		{name: "append ends before new length", row: func() []byte {
			row := buildersDiffRow(t, pre, buildersList(1, 2, 3, 4))
			binary.LittleEndian.PutUint32(row[22:], 1)
			return row[:26+101]
		}},
		{name: "shrink", row: func() []byte {
			row := buildersDiffRow(t, buildersList(), buildersList())
			binary.LittleEndian.PutUint64(row[14:], 1)
			return row
		}},
		{name: "count does not match length", row: func() []byte {
			row := bytes.Clone(valid)
			binary.LittleEndian.PutUint32(row[22:], 1)
			return row
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := buildersList(1, 2)
			beforeSSZ, err := got.EncodeSSZ(nil)
			require.NoError(t, err)
			beforePointers := []*cltypes.Builder{got.Get(0), got.Get(1)}

			err = ApplyBuildersDiff(test.row(), got, testBuilderRegistryLimit)
			require.ErrorIs(t, err, ErrHistoryRebuildRequired)
			afterSSZ, encodeErr := got.EncodeSSZ(nil)
			require.NoError(t, encodeErr)
			require.Equal(t, beforeSSZ, afterSSZ)
			require.Same(t, beforePointers[0], got.Get(0))
			require.Same(t, beforePointers[1], got.Get(1))
		})
	}
}

func TestLegacyQueueDecodersRejectBuildersDiffWithoutMutation(t *testing.T) {
	row := buildersDiffRow(t, buildersList(1), buildersList(2, 3))
	tests := []struct {
		name  string
		apply func([]byte, *solid.ListSSZ[*cltypes.Builder]) error
	}{
		{name: "pre guard decoder", apply: applyLegacyBuildersQueueDiff},
		{name: "current decoder", apply: func(row []byte, builders *solid.ListSSZ[*cltypes.Builder]) error {
			return base_encoding.ApplySSZQueueDiff(bytes.NewReader(row), builders, 0)
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builders := buildersList(1)
			before := builders.Get(0)
			err := test.apply(row, builders)
			require.Error(t, err)
			require.NotErrorIs(t, err, base_encoding.ErrCorruptQueueDiff)
			require.Equal(t, []uint64{1}, builderBalances(builders))
			require.Same(t, before, builders.Get(0))
		})
	}
}

func buildersDiffRow(t *testing.T, pre, post *solid.ListSSZ[*cltypes.Builder]) []byte {
	t.Helper()
	writer := new(BuildersDiffWriter)
	writer.Initialize(pre)
	var row bytes.Buffer
	require.NoError(t, writer.WriteDiff(&row, post))
	return row.Bytes()
}

func legacyBuildersDiffRow(t *testing.T, pre, post *solid.ListSSZ[*cltypes.Builder]) []byte {
	t.Helper()
	writer := base_encoding.NewSSZQueueEncoder[*cltypes.Builder](func(a, b *cltypes.Builder) bool { return *a == *b })
	writer.Initialize(pre)
	var row bytes.Buffer
	require.NoError(t, writer.WriteDiff(&row, post))
	return row.Bytes()
}

func applyLegacyBuildersQueueDiff(row []byte, queue *solid.ListSSZ[*cltypes.Builder]) error {
	r := bytes.NewReader(row)
	var lengthToTruncate uint32
	if err := binary.Read(r, binary.LittleEndian, &lengthToTruncate); err != nil {
		return err
	}
	queue.Cut(int(lengthToTruncate))
	for {
		var length uint32
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}
		builder := &cltypes.Builder{}
		if err := builder.DecodeSSZ(buf, int(clparams.Phase0Version)); err != nil {
			return err
		}
		queue.Append(builder)
	}
}
