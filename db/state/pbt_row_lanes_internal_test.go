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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

func TestPBinRowLanesRandomOrder(t *testing.T) {
	for seed := range int64(10) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			rows, err := newPBinRowLanes(t.TempDir(), fmt.Sprintf("random-%d", seed))
			require.NoError(t, err)
			defer rows.close()
			keys := make([][]byte, 0, 3004)
			values := make(map[string][]byte, 3004)
			for partition := range byte(20) {
				for index := range 150 {
					pathBytes := make([]byte, 33)
					pathBytes[0] = partition / 16
					pathBytes[1] = (partition % 16) << 4
					pathBytes[2] = byte(index >> 8)
					pathBytes[3] = byte(index)
					pathBytes[32] = byte(rng.Intn(256))
					path := eip8297.PathFromBits(pathBytes, 264)
					key, encodeErr := pbt.EncodeRowKey(&path)
					require.NoError(t, encodeErr)
					value := []byte{partition, byte(index), byte(rng.Intn(256))}
					keys = append(keys, key)
					values[string(key)] = value
				}
			}
			extra := []struct {
				key   []byte
				value []byte
			}{
				{key: pbt.GlobalRootKey(), value: []byte{1}},
				{key: commitment.KeyCommitmentState, value: []byte{2}},
				{key: []byte{0x01}, value: []byte{3}},
			}
			for _, bitLen := range []int16{12, 16, 20, 264, 528} {
				path := eip8297.PathFromBits([]byte{0xf9, byte(bitLen), byte(seed)}, bitLen)
				key, encodeErr := pbt.EncodeRowKey(&path)
				require.NoError(t, encodeErr)
				extra = append(extra, struct {
					key   []byte
					value []byte
				}{key: key, value: []byte{byte(bitLen)}})
			}
			for _, row := range extra {
				keys = append(keys, row.key)
				values[string(row.key)] = row.value
			}
			rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
			for _, key := range keys {
				_, err = rows.collect(key, values[string(key)])
				require.NoError(t, err)
			}
			require.GreaterOrEqual(t, len(rows.lanes), 20)
			var got []string
			require.NoError(t, rows.load(context.Background(), func(key, value []byte) error {
				got = append(got, fmt.Sprintf("%x:%x", key, value))
				return nil
			}))
			wantKeys := append([][]byte(nil), keys...)
			sort.Slice(wantKeys, func(i, j int) bool { return bytes.Compare(wantKeys[i], wantKeys[j]) < 0 })
			want := make([]string, 0, len(wantKeys))
			for _, key := range wantKeys {
				want = append(want, fmt.Sprintf("%x:%x", key, values[string(key)]))
			}
			require.Equal(t, want, got)
		})
	}
}

func TestPBinRowLanesRejectDuplicateWithinLane(t *testing.T) {
	rows, err := newPBinRowLanes(t.TempDir(), "duplicate-lane")
	require.NoError(t, err)
	defer rows.close()
	key := pbinRowLaneTestKey(0x20)
	partition := pbinPartitionOf(key)
	lane, err := rows.openLane(partition)
	require.NoError(t, err)
	rows.lanes[partition] = lane
	require.NoError(t, lane.write(key, []byte{1}))
	require.NoError(t, lane.write(key, []byte{2}))
	err = rows.load(context.Background(), func([]byte, []byte) error { return nil })
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate-lane")
	require.Contains(t, err.Error(), fmt.Sprintf("%x", key))
}

func TestPBinRowLanesRejectDuplicateBetweenLaneAndSkip(t *testing.T) {
	rows, err := newPBinRowLanes(t.TempDir(), "duplicate-skip")
	require.NoError(t, err)
	defer rows.close()
	key := pbinRowLaneTestKey(0x30)
	_, err = rows.collect(key, []byte{1})
	require.NoError(t, err)
	_, err = rows.collect(key, []byte{2})
	require.NoError(t, err)
	err = rows.load(context.Background(), func([]byte, []byte) error { return nil })
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate-skip")
	require.Contains(t, err.Error(), fmt.Sprintf("%x", key))
}

func pbinRowLaneTestKey(value byte) []byte {
	stem := make([]byte, 32)
	stem[0] = value
	path := eip8297.PathFromBits(append([]byte{eip8297.StorageZone}, stem...), 264)
	key, err := pbt.EncodeRowKey(&path)
	if err != nil {
		panic(err)
	}
	return key
}
