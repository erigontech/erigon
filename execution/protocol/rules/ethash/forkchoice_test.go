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

package ethash

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
)

func TestShouldReorg(t *testing.T) {
	t.Parallel()

	// Two distinct hashes ordered so hashLow < hashHigh (lex byte compare).
	hashLow := common.Hash{0x01}
	hashHigh := common.Hash{0x02}

	tests := []struct {
		name        string
		localTd     uint64
		localHeight uint64
		localHash   common.Hash
		newTd       uint64
		newHeight   uint64
		newHash     common.Hash
		want        bool
	}{
		{"new TD strictly higher", 100, 5, hashLow, 101, 5, hashLow, true},
		{"new TD strictly lower", 100, 5, hashLow, 99, 5, hashLow, false},
		{"equal TD, new height shallower", 100, 10, hashLow, 100, 5, hashHigh, true},
		{"equal TD, new height deeper", 100, 5, hashLow, 100, 10, hashHigh, false},
		{"equal TD, equal height, new hash larger", 100, 5, hashLow, 100, 5, hashHigh, true},
		{"equal TD, equal height, new hash smaller", 100, 5, hashHigh, 100, 5, hashLow, false},
		{"equal TD, equal height, equal hash", 100, 5, hashLow, 100, 5, hashLow, false},
	}
	for _, tc := range tests {
		got := ShouldReorg(uint256.NewInt(tc.localTd), tc.localHeight, tc.localHash,
			uint256.NewInt(tc.newTd), tc.newHeight, tc.newHash)
		if got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
