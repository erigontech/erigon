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

package scalar_test

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/cmd/rpcdaemon/graphql/graph/scalar"
)

func TestMarshalUint64(t *testing.T) {
	tests := []struct {
		input uint64
		want  string
	}{
		{0, `"0x0"`},
		{2, `"0x2"`},
		{255, `"0xff"`},
		{1000, `"0x3e8"`},
		{^uint64(0), `"0xffffffffffffffff"`},
	}
	for _, tt := range tests {
		var buf bytes.Buffer
		scalar.MarshalUint64(tt.input).MarshalGQL(&buf)
		if got := buf.String(); got != tt.want {
			t.Errorf("MarshalUint64(%d) = %s, want %s", tt.input, got, tt.want)
		}
	}
}

func TestUnmarshalUint64(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    uint64
		wantErr bool
	}{
		{"hex string lowercase", "0x2", 2, false},
		{"hex string uppercase", "0xFF", 255, false},
		{"decimal string", "42", 42, false},
		{"int64 positive", int64(10), 10, false},
		{"float64", float64(7), 7, false},
		{"zero hex", "0x0", 0, false},
		{"zero decimal", "0", 0, false},
		{"negative int64", int64(-1), 0, true},
		{"empty string", "", 0, true},
		{"invalid hex chars", "0xGG", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := scalar.UnmarshalUint64(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalUint64(%v) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("UnmarshalUint64(%v) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}
