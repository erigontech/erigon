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

package state_accessors

import (
	"testing"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func staticTableLen(tbl *StaticValidatorTable) int {
	n := 0
	tbl.ForEach(func(uint64, *StaticValidator) bool { n++; return true })
	return n
}

// A table restored short (slot advanced past 0) must be refilled by the genesis
// re-add at slot 0, not skipped — else lookups past the cut fail forever.
func TestStaticValidatorTable_RefillsRestoredShortTable(t *testing.T) {
	const full = 10
	const persisted = 6

	vals := make([]solid.Validator, full)
	for i := range vals {
		vals[i] = solid.NewValidator()
	}

	table := NewStaticValidatorTable()
	for i := range persisted {
		require.NoError(t, table.AddValidator(vals[i], uint64(i), 0))
	}
	// Simulate a restore where the processing slot advanced past genesis.
	table.SetSlot(1000)
	require.Equal(t, persisted, staticTableLen(table))

	for i := range full {
		require.NoError(t, table.AddValidator(vals[i], uint64(i), 0))
	}
	require.Equal(t, full, staticTableLen(table))
}

// A genuine index gap must error, not silently corrupt the table.
func TestStaticValidatorTable_AddValidatorRejectsGap(t *testing.T) {
	table := NewStaticValidatorTable()
	require.NoError(t, table.AddValidator(solid.NewValidator(), 0, 0))
	require.Error(t, table.AddValidator(solid.NewValidator(), 5, 0))
}
