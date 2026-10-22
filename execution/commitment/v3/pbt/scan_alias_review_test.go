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

package pbt

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestReviewPhaseHookPreservesCallerOps(t *testing.T) {
	for _, tc := range []struct {
		name      string
		zone      byte
		threshold int
	}{
		{"chain", eip8297.AccountZone, 0},
		{"bucket", eip8297.StorageZone, 0},
		{"subtask", eip8297.StorageZone, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			length, _ := eip8297.ZoneKeyLength(tc.zone)
			first := make([]byte, length)
			first[0], first[1], first[33] = tc.zone, 0x10, 1
			second := append([]byte(nil), first...)
			second[33] = 0x21
			ops := []Op{{Key: first, Value: testTrieValue(1)}, {Key: second, Value: testTrieValue(2)}}
			trie := newParallelRoundBase(map[string][]byte{}).trie()
			trie.SetPhaseHook(func(_ phaseTask, op *Op) error {
				if op != nil {
					op.Value = testTrieValue(9)
				}
				return nil
			})
			_, err := trie.ProcessParallelWithThreshold(ops, 2, tc.threshold)
			require.NoError(t, err)
			if ops[0].Value != testTrieValue(1) {
				t.Fatalf("phase hook changed caller ops[0].Value: want %x, got %x", testTrieValue(1), ops[0].Value)
			}
		})
	}
}
