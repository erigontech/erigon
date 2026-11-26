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

package historical_states_reader_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
)

func TestSelectDiffedListRange(t *testing.T) {
	const dump = clparams.SlotsPerDump
	for _, tc := range []struct {
		name     string
		slot     uint64
		progress uint64
		want     historical_states_reader.DiffedListRange
	}{
		{"at a dump slot", dump, 3 * dump, historical_states_reader.DiffedListRange{DumpSlot: dump, Forward: true, FirstDiffSlot: dump + 1, LastDiffSlot: dump}},
		{"forward from the earlier dump", dump + 10, 3 * dump, historical_states_reader.DiffedListRange{DumpSlot: dump, Forward: true, FirstDiffSlot: dump + 1, LastDiffSlot: dump + 10, HasDiffs: true}},
		{"backward from the later dump", dump + dump/2 + 1, 3 * dump, historical_states_reader.DiffedListRange{DumpSlot: 2 * dump, FirstDiffSlot: dump + dump/2 + 2, LastDiffSlot: 2 * dump, HasDiffs: true}},
		{"forward when the later dump is not written", dump + dump/2 + 1, 2 * dump, historical_states_reader.DiffedListRange{DumpSlot: dump, Forward: true, FirstDiffSlot: dump + 1, LastDiffSlot: dump + dump/2 + 1, HasDiffs: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, historical_states_reader.SelectDiffedListRange(tc.slot, tc.progress))
		})
	}
}
