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

package rawtemporaldb_test

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/race"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/execution/types"
)

// countingPutDel records the values it is handed, copying them the way the
// domain writer does. copy=false keeps it allocation-free for the alloc test.
type countingPutDel struct {
	puts [][]byte
	copy bool
}

func (p *countingPutDel) DomainPut(domain kv.Domain, k, v []byte, txNum uint64, prevVal []byte) error {
	if p.copy {
		p.puts = append(p.puts, append([]byte(nil), v...))
	}
	return nil
}

func (p *countingPutDel) DomainDel(domain kv.Domain, k []byte, txNum uint64, prevVal []byte) error {
	return nil
}

func (p *countingPutDel) DomainDelPrefix(domain kv.Domain, prefix []byte, txNum uint64) error {
	return nil
}

// A reused ReceiptWriter must keep its varint scratch off the heap: this runs
// once per transaction applied.
func TestReceiptWriterAppendMetadataIsAllocationFree(t *testing.T) {
	var w rawtemporaldb.ReceiptWriter
	putter := &countingPutDel{}

	allocs := testing.AllocsPerRun(100, func() {
		if err := w.AppendMetadata(putter, 7, 21000, 131072, 42); err != nil {
			t.Fatal(err)
		}
	})
	require.Zero(t, allocs, "ReceiptWriter.AppendMetadata must not allocate")
}

// Reusing one scratch across the three puts is only safe because each value is
// copied on the way in; pin that the three values reach the putter distinct.
func TestReceiptWriterAppendMetadataValuesAreDistinct(t *testing.T) {
	t.Parallel()
	var w rawtemporaldb.ReceiptWriter
	putter := &countingPutDel{copy: true}

	require.NoError(t, w.AppendMetadata(putter, 7, 21000, 131072, 42))
	require.Len(t, putter.puts, 3)
	require.Equal(t, receiptValueForTest(21000), putter.puts[0])
	require.Equal(t, receiptValueForTest(131072), putter.puts[1])
	require.Equal(t, receiptValueForTest(7), putter.puts[2])
}

func receiptFixture() *types.Receipt {
	logs := make(types.Logs, 3)
	for i := range logs {
		logs[i] = &types.Log{
			Address: common.HexToAddress(fmt.Sprintf("0x%02x", 0xa0+i)),
			Topics:  []common.Hash{common.HexToHash("0x11"), common.HexToHash("0x22")},
			Data:    []byte{0x01, 0x02, 0x03, byte(i)},
			Index:   hexutil.Uint(7 + i),
		}
	}
	return &types.Receipt{
		Type:                     types.DynamicFeeTxType,
		Status:                   types.ReceiptStatusSuccessful,
		CumulativeGasUsed:        123456,
		Logs:                     logs,
		GasUsed:                  21000,
		ContractAddress:          common.HexToAddress("0xdead"),
		TransactionIndex:         4,
		BlockNumber:              uint256.NewInt(1),
		FirstLogIndexWithinBlock: 7,
	}
}

// The RCache write runs once per transaction applied too; guard it against the
// scratch going back on the stack of Append.
func TestReceiptWriterAppendIsAllocationFree(t *testing.T) {
	var w rawtemporaldb.ReceiptWriter
	receipt := receiptFixture()
	putter := &countingPutDel{}

	allocs := testing.AllocsPerRun(100, func() {
		if err := w.Append(putter, receipt, 42); err != nil {
			t.Fatal(err)
		}
	})
	if !race.Enabled { // the race detector allocates inside sync.Pool
		require.Zero(t, allocs, "ReceiptWriter.Append must not allocate")
	}
}

// BenchmarkPerTxReceiptWrite is what one applied transaction pays on the receipt
// path: the storage encoding into RCacheDomain plus the three ReceiptDomain
// counters.
func BenchmarkPerTxReceiptWrite(b *testing.B) {
	var w rawtemporaldb.ReceiptWriter
	receipt := receiptFixture()
	putter := &countingPutDel{}

	b.ReportAllocs()
	for b.Loop() {
		if err := w.Append(putter, receipt, 42); err != nil {
			b.Fatal(err)
		}
		if err := w.AppendMetadata(putter, 7, receipt.CumulativeGasUsed, 131072, 42); err != nil {
			b.Fatal(err)
		}
	}
}

// receiptValueForTest is the value encoding, for assertions in tests.
func receiptValueForTest(v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	i := binary.PutUvarint(buf[:], v)
	return buf[:i]
}
