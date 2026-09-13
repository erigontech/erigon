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

package types

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/rlp"
)

// storageReceiptGolden is the RCacheDomain encoding of storageReceiptFixture,
// captured before the encoder was made allocation-free.
const storageReceiptGolden = "f9014302018301e24007f90120f85e9400000000000000000000000000000000000000a0f842a00000000000000000000000000000000000000000000000000000000000000011a000000000000000000000000000000000000000000000000000000000000000228401020300f85e9400000000000000000000000000000000000000a1f842a00000000000000000000000000000000000000000000000000000000000000011a000000000000000000000000000000000000000000000000000000000000000228401020301f85e9400000000000000000000000000000000000000a2f842a00000000000000000000000000000000000000000000000000000000000000011a0000000000000000000000000000000000000000000000000000000000000002284010203020494000000000000000000000000000000000000dead825208"

func storageReceiptFixture() *ReceiptForStorage {
	logs := make(Logs, 3)
	for i := range logs {
		logs[i] = &Log{
			Address: common.HexToAddress(fmt.Sprintf("0x%02x", 0xa0+i)),
			Topics:  []common.Hash{common.HexToHash("0x11"), common.HexToHash("0x22")},
			Data:    []byte{0x01, 0x02, 0x03, byte(i)},
			Index:   hexutil.Uint(7 + i),
		}
	}
	r := &Receipt{
		Type:                     DynamicFeeTxType,
		Status:                   ReceiptStatusSuccessful,
		CumulativeGasUsed:        123456,
		Logs:                     logs,
		GasUsed:                  21000,
		ContractAddress:          common.HexToAddress("0xdead"),
		TransactionIndex:         4,
		FirstLogIndexWithinBlock: 7,
	}
	return (*ReceiptForStorage)(r)
}

// The RCacheDomain holds these bytes across restarts, so the storage encoding
// is a format: pin it against a golden so an encoder rewrite cannot move it.
func TestReceiptForStorage_EncodingIsStable(t *testing.T) {
	t.Parallel()
	got, err := rlp.EncodeToBytes(storageReceiptFixture())
	require.NoError(t, err)
	require.Equal(t, storageReceiptGolden, hex.EncodeToString(got))
}

func TestReceiptForStorage_RoundTrip(t *testing.T) {
	t.Parallel()
	enc, err := rlp.EncodeToBytes(storageReceiptFixture())
	require.NoError(t, err)

	var got ReceiptForStorage
	require.NoError(t, rlp.DecodeBytes(enc, &got))

	want := storageReceiptFixture()
	require.Equal(t, want.CumulativeGasUsed, got.CumulativeGasUsed)
	require.Equal(t, want.ContractAddress, got.ContractAddress)
	require.Equal(t, want.TransactionIndex, got.TransactionIndex)
	require.Equal(t, want.FirstLogIndexWithinBlock, got.FirstLogIndexWithinBlock)
	require.Len(t, got.Logs, len(want.Logs))
	for i, l := range got.Logs {
		require.Equal(t, want.Logs[i].Address, l.Address)
		require.Equal(t, want.Logs[i].Topics, l.Topics)
		require.Equal(t, []byte(want.Logs[i].Data), []byte(l.Data))
	}
}

func encodeStorageReceiptWithLogs(t *testing.T, nLogs int) []byte {
	t.Helper()
	r := storageReceiptFixture()
	base := r.Logs
	r.Logs = nil
	for len(r.Logs) < nLogs {
		r.Logs = append(r.Logs, base[len(r.Logs)%len(base)])
	}
	enc, err := rlp.EncodeToBytes(r)
	require.NoError(t, err)
	return enc
}

// One block backs every Log, but only on a slice-backed stream. Both paths must
// decode to the same thing.
func TestDecodeLogsForStorageBothPaths(t *testing.T) {
	t.Parallel()
	for _, n := range []int{0, 1, 2, 5, 130} {
		enc := encodeStorageReceiptWithLogs(t, n)
		var blocked, grown ReceiptForStorage
		require.NoError(t, rlp.DecodeBytes(enc, &blocked))
		require.NoError(t, rlp.Decode(bytes.NewReader(enc), &grown))
		require.Equal(t, blocked, grown, "%d logs", n)
		require.Len(t, blocked.Logs, n)
	}
}

// Logs share one backing block, so writing through one pointer must not reach
// its neighbour.
func TestDecodeLogsForStorageBlockNotAliased(t *testing.T) {
	t.Parallel()
	var r ReceiptForStorage
	require.NoError(t, rlp.DecodeBytes(encodeStorageReceiptWithLogs(t, 3), &r))
	require.Len(t, r.Logs, 3)
	want := r.Logs[1].Address
	r.Logs[0].Address = common.Address{0xff}
	r.Logs[0].Index = 0xffff
	require.Equal(t, want, r.Logs[1].Address)
}

// The block is sized from attacker-controlled bytes: a payload of one-byte items
// must not allocate a Log for each, since a stored log needs at least 24 bytes.
func TestDecodeLogsForStorageBlockBounded(t *testing.T) {
	// No t.Parallel: TotalAlloc is process-wide, so a parallel sibling's
	// allocations would land inside the measured window.
	items := 40000
	logList := append([]byte{0xf9, byte(items >> 8), byte(items)}, make([]byte, items)...)

	s := rlp.NewBytesStream(logList)
	defer rlp.PutStream(s)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := decodeLogsForStorage(s) // malformed: must error, not balloon
	runtime.ReadMemStats(&after)
	require.Error(t, err)

	grew := after.TotalAlloc - before.TotalAlloc
	// The cap holds this near 300KB and its absence pushes it past 6MB, so the
	// limit sits between them with room for background allocation.
	t.Logf("decoding %d bytes allocated %d", len(logList), grew)
	require.Less(t, grew, uint64(32*len(logList)),
		"decoding %d bytes allocated %d", len(logList), grew)
}
