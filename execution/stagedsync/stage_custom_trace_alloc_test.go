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

package stagedsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/execution/types"
)

type customTracePutDel struct{}

func (*customTracePutDel) DomainPut(kv.Domain, []byte, []byte, uint64, []byte) error {
	return nil
}

func (*customTracePutDel) DomainDel(kv.Domain, []byte, uint64, []byte) error {
	return nil
}

func (*customTracePutDel) DomainDelPrefix(kv.Domain, []byte, uint64) error {
	return nil
}

func TestWriteCustomTraceReceiptsIsAllocationFree(t *testing.T) {
	var writer rawtemporaldb.ReceiptWriter
	putter := &customTracePutDel{}
	produce := Produce{ReceiptDomain: true, RCacheDomain: true}
	receipt := &types.Receipt{Status: types.ReceiptStatusSuccessful}

	require.NoError(t, writeCustomTraceReceipts(&writer, putter, produce, 7, 21_000, 131_072, receipt, 42))
	allocs := testing.AllocsPerRun(100, func() {
		if err := writeCustomTraceReceipts(&writer, putter, produce, 7, 21_000, 131_072, receipt, 42); err != nil {
			panic(err)
		}
	})
	require.Zero(t, allocs)
}
