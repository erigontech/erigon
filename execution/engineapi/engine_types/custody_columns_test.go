// Copyright 2025 The Erigon Authors
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

package engine_types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCustodyColumnsMarshalJSON(t *testing.T) {
	cc := CustodyColumns{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	data, err := json.Marshal(cc)
	require.NoError(t, err)
	require.Equal(t, `"0x0102030405060708090a0b0c0d0e0f10"`, string(data))
}

func TestCustodyColumnsUnmarshalJSON(t *testing.T) {
	input := `"0x0102030405060708090a0b0c0d0e0f10"`
	var cc CustodyColumns
	err := json.Unmarshal([]byte(input), &cc)
	require.NoError(t, err)
	require.Equal(t, CustodyColumns{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}, cc)
}

func TestCustodyColumnsUnmarshalJSON_WrongLength(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"too short", `"0xabcd"`},
		{"too long", `"0x0102030405060708090a0b0c0d0e0f1011"`},
		{"empty", `"0x"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cc CustodyColumns
			err := json.Unmarshal([]byte(tt.input), &cc)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid custodyColumns length")
		})
	}
}

func TestCustodyColumnsNullPointer(t *testing.T) {
	type wrapper struct {
		CC *CustodyColumns `json:"custodyColumns"`
	}
	var w wrapper
	err := json.Unmarshal([]byte(`{"custodyColumns":null}`), &w)
	require.NoError(t, err)
	require.Nil(t, w.CC)
}

func TestCustodyColumnsRoundTrip(t *testing.T) {
	original := CustodyColumns{0xff, 0x00, 0xff, 0x00, 0xff, 0x00, 0xff, 0x00,
		0xff, 0x00, 0xff, 0x00, 0xff, 0x00, 0xff, 0x00}
	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded CustodyColumns
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)
	require.Equal(t, original, decoded)
}
