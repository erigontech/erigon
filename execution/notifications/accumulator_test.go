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

package notifications

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func TestAccumulatorSlotNumber(t *testing.T) {
	for _, tc := range []struct {
		name string
		slot *uint64
	}{
		{"before activation", nil},
		{"zero", common.NewUint64(0)},
		{"nonzero", common.NewUint64(123)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, unwind := range []bool{false, true} {
				acc := NewAccumulator()
				acc.StartChange(&types.Header{SlotNumber: tc.slot}, nil, unwind)
				require.Len(t, acc.Changes(), 1)
				require.Equal(t, tc.slot, acc.Changes()[0].ToProto().SlotNumber)
			}
		})
	}
}

func TestAccumulatorAppendIncompleteChanges(t *testing.T) {
	for _, tc := range []struct {
		name       string
		source     bool
		target     bool
		incomplete bool
	}{
		{"both complete", false, false, false},
		{"source incomplete", true, false, true},
		{"target incomplete", false, true, true},
		{"both incomplete", true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, target := NewAccumulator(), NewAccumulator()
			source.StartChange(&types.Header{}, nil, false)
			if tc.source {
				source.MarkIncomplete()
			}
			if tc.target {
				target.MarkIncomplete()
			}
			source.AppendAndReset(target)
			require.Equal(t, tc.incomplete, target.stateChangesIncomplete)
			require.False(t, source.stateChangesIncomplete)
			target.Reset()
			require.False(t, target.stateChangesIncomplete)
		})
	}
}

func TestAccumulatorCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name       string
		incomplete bool
	}{
		{name: "complete"},
		{name: "incomplete", incomplete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			acc := NewAccumulator()
			address, slot := common.Address{1}, common.Hash{2}
			acc.StartChange(&types.Header{Time: 1}, [][]byte{{1}}, true)
			acc.ChangeAccount(address, 0, []byte{2})
			acc.ChangeStorage(address, 0, slot, []byte{3})
			if tc.incomplete {
				acc.MarkIncomplete()
			}
			want := acc.Changes()[0].ToProto()
			restore := acc.Checkpoint()

			acc.MarkIncomplete()
			acc.StartChange(&types.Header{Time: 2}, [][]byte{{4}}, false)
			acc.ChangeStorage(address, 0, slot, []byte{5})
			restore()

			require.Equal(t, tc.incomplete, acc.stateChangesIncomplete)
			require.Len(t, acc.Changes(), 1)
			require.Equal(t, want, acc.Changes()[0].ToProto())
			require.Panics(t, func() { acc.ChangeStorage(address, 0, slot, []byte{6}) }, "restoring a checkpoint must not reopen completed changes")
			acc.StartChange(&types.Header{Time: 3}, nil, false)
			acc.ChangeStorage(address, 0, slot, []byte{7})
			require.Equal(t, want, acc.Changes()[0].ToProto())
		})
	}
}

func TestAccumulatorAppendPreservesChanges(t *testing.T) {
	for _, name := range []string{"empty source", "nonempty source"} {
		t.Run(name, func(t *testing.T) {
			source, target := NewAccumulator(), NewAccumulator()
			address, slot := common.Address{1}, common.Hash{2}
			target.StartChange(&types.Header{Time: 1}, nil, true)
			target.ChangeStorage(address, 0, slot, []byte{1})
			if name == "nonempty source" {
				source.StartChange(&types.Header{Time: 2}, nil, false)
				source.ChangeStorage(address, 0, slot, []byte{2})
			}
			want := append([]StateChange(nil), target.Changes()...)
			want = append(want, source.Changes()...)
			source.AppendAndReset(target)
			require.Equal(t, want, target.Changes())
			require.Empty(t, source.Changes())

			target.ChangeStorage(address, 0, slot, []byte{3})
			target.ChangeAccount(address, 0, []byte{4})
			target.ChangeStorage(common.Address{5}, 0, slot, []byte{6})
			last := target.Changes()[len(target.Changes())-1]
			require.Len(t, last.Changes, 2)
			require.Equal(t, []byte{4}, last.Changes[0].Data)
			require.Len(t, last.Changes[0].StorageChanges, 1)
			require.Equal(t, []byte{3}, last.Changes[0].StorageChanges[0].Data)
		})
	}
}
