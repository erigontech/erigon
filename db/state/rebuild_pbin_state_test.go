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

package state

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func pbinRebuildStateValue(t *testing.T, trieState []byte) []byte {
	t.Helper()
	v := make([]byte, 18+len(trieState))
	binary.BigEndian.PutUint16(v[16:18], uint16(len(trieState)))
	copy(v[18:], trieState)
	return v
}

func TestValidatePBinRebuildState(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		value []byte
		ok    bool
	}{
		{"nothing stored", nil, true},
		{"header truncated", make([]byte, 9), false},
		{"no trie state", make([]byte, 18), true},
		{"length exceeds the value", func() []byte {
			v := make([]byte, 18)
			binary.BigEndian.PutUint16(v[16:18], 64)
			return v
		}(), false},
		{"hex trie state", pbinRebuildStateValue(t, []byte{0x03, 0, 0}), true},
		{"pre-version pbin blob", pbinRebuildStateValue(t, []byte{0xB1, 0x03, 0, 0}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePBinRebuildState(tc.value)
			if tc.ok {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
		})
	}
}
