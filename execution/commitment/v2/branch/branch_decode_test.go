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

package branch

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForEachCellRejectsInvalidBranch(t *testing.T) {
	for _, tc := range []struct {
		name string
		data BranchData
		want string
	}{
		{"unknown_fields", branchOfCells(t, cellOf(0x20)), "unknown cell fields"},
		{"field_length_overflow", branchOfCells(t, append(binary.AppendUvarint([]byte{byte(FieldStateHash)}, ^uint64(0)), 0), nil), "past the end"},
		{"trailing_bytes", append(branchOfCells(t), 0), "trailing bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.data.ForEachCell(func(_ int, _ BranchCell) error { return nil })
			require.ErrorContains(t, err, tc.want)
		})
	}
}
