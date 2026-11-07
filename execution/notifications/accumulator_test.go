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

func TestAccumulatorCopyIncompleteChanges(t *testing.T) {
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
			source.CopyAndReset(target)
			require.Equal(t, tc.incomplete, target.stateChangesIncomplete)
			require.False(t, source.stateChangesIncomplete)
			target.Reset(0)
			require.False(t, target.stateChangesIncomplete)
		})
	}
}
