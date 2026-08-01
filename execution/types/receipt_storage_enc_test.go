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
	"encoding/hex"
	"fmt"
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
		logs[i] = Log{
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
	for i := range got.Logs {
		l := &got.Logs[i]
		require.Equal(t, want.Logs[i].Address, l.Address)
		require.Equal(t, want.Logs[i].Topics, l.Topics)
		require.Equal(t, []byte(want.Logs[i].Data), []byte(l.Data))
	}
}

func BenchmarkReceiptForStorageEncode(b *testing.B) {
	r := storageReceiptFixture()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := rlp.EncodeToBytes(r); err != nil {
			b.Fatal(err)
		}
	}
}
