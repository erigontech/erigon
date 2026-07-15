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

package consensus_tests

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewardDeltasDecodeSSZRejectsInvalidBounds(t *testing.T) {
	tests := []struct {
		name string
		buf  []byte
	}{
		{name: "short", buf: make([]byte, 7)},
		{name: "wrong first offset", buf: rewardDeltaOffsets(4, 8, 8)},
		{name: "reversed offsets", buf: rewardDeltaOffsets(8, 7, 8)},
		{name: "offset past end", buf: rewardDeltaOffsets(8, 16, 8)},
		{name: "partial uint64", buf: rewardDeltaOffsets(8, 9, 9)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Error(t, new(rewardDeltas).DecodeSSZ(test.buf, 0))
		})
	}
}

func TestRewardDeltasDecodeSSZ(t *testing.T) {
	buf := rewardDeltaOffsets(8, 16, 24)
	binary.LittleEndian.PutUint64(buf[8:], 11)
	binary.LittleEndian.PutUint64(buf[16:], 22)

	deltas := new(rewardDeltas)
	require.NoError(t, deltas.DecodeSSZ(buf, 0))
	require.Equal(t, []uint64{11}, deltas.rewards)
	require.Equal(t, []uint64{22}, deltas.penalties)
}

func rewardDeltaOffsets(rewardsOffset, penaltiesOffset uint32, size int) []byte {
	buf := make([]byte, size)
	if size >= 8 {
		binary.LittleEndian.PutUint32(buf, rewardsOffset)
		binary.LittleEndian.PutUint32(buf[4:], penaltiesOffset)
	}
	return buf
}
