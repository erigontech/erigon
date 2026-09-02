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

package engine_block_downloader

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewPayloadGapExceedsLimit(t *testing.T) {
	tests := []struct {
		name        string
		currentHead uint64
		missingNum  uint64
		limit       uint64
		want        bool
	}{
		{"behind by more than limit", 100, 500, 96, true},
		{"ahead by more than limit", 500, 100, 96, true},
		{"behind by exact limit", 100, 196, 96, false},
		{"behind by one over limit", 100, 197, 96, true},
		{"ahead by exact limit", 196, 100, 96, false},
		{"ahead by one over limit", 197, 100, 96, true},
		{"same block", 100, 100, 96, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, newPayloadGapExceedsLimit(tt.currentHead, tt.missingNum, tt.limit))
		})
	}
}
