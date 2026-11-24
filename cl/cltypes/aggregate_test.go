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

package cltypes

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncAggregateJSONRequiresBitsOfThePreallocatedWidth(t *testing.T) {
	signature := `"0x` + strings.Repeat("00", 96) + `"`
	for _, tc := range []struct {
		name    string
		size    int
		bits    string
		wantErr string
	}{
		{"mainnet width", 64, strings.Repeat("00", 64), ""},
		{"minimal width", 4, strings.Repeat("00", 4), ""},
		{"mainnet width, minimal bits", 64, "00", "sync committee bits are 1 bytes, expected 64"},
		{"minimal width, mainnet bits", 4, strings.Repeat("00", 64), "sync committee bits are 64 bytes, expected 4"},
		{"empty bits", 64, "", "sync aggregate is missing sync committee bits"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			aggregate := NewSyncAggregateWithSize(tc.size)
			input := `{"sync_committee_bits":"0x` + tc.bits + `","sync_committee_signature":` + signature + `}`
			err := aggregate.UnmarshalJSON([]byte(input))
			if tc.wantErr == "" {
				require.NoError(t, err)
				require.Len(t, aggregate.SyncCommiteeBits, tc.size)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestSyncAggregateJSONWithoutPreallocatedBitsAcceptsThem(t *testing.T) {
	aggregate := new(SyncAggregate)
	require.NoError(t, aggregate.UnmarshalJSON([]byte(
		`{"sync_committee_bits":"0x00","sync_committee_signature":"0x`+strings.Repeat("00", 96)+`"}`,
	)))
	require.Len(t, aggregate.SyncCommiteeBits, 1)
}
