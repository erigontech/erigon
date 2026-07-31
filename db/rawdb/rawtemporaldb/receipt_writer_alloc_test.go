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

package rawtemporaldb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
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
func TestReceiptWriterAppendIsAllocationFree(t *testing.T) {
	var w rawtemporaldb.ReceiptWriter
	putter := &countingPutDel{}

	allocs := testing.AllocsPerRun(100, func() {
		if err := w.Append(putter, 7, 21000, 131072, 42); err != nil {
			t.Fatal(err)
		}
	})
	require.Zero(t, allocs, "ReceiptWriter.Append must not allocate")
}

// Reusing one scratch across the three puts is only safe because each value is
// copied on the way in; pin that the three values reach the putter distinct.
func TestReceiptWriterAppendValuesAreDistinct(t *testing.T) {
	t.Parallel()
	var w rawtemporaldb.ReceiptWriter
	putter := &countingPutDel{copy: true}

	require.NoError(t, w.Append(putter, 7, 21000, 131072, 42))
	require.Len(t, putter.puts, 3)
	require.Equal(t, rawtemporaldb.ReceiptValueForTest(21000), putter.puts[0])
	require.Equal(t, rawtemporaldb.ReceiptValueForTest(131072), putter.puts[1])
	require.Equal(t, rawtemporaldb.ReceiptValueForTest(7), putter.puts[2])
}
