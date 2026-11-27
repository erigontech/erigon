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

package rpchelper

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/rpc/filters"
)

type discardLogSub struct{ Sub[*types.Log] }

func (discardLogSub) Send(*types.Log) {}

func BenchmarkDistributeLogManyAddressFilters(b *testing.B) {
	const filterCount, addressCount = 2000, 50
	a := NewLogsFilterAggregator()
	for i := range filterCount {
		criteria := filters.FilterCriteria{Addresses: []common.Address{{byte(i % addressCount), 1}}}
		a.insertLogsFilter(newLogsFilter(discardLogSub{}, criteria, nil))
	}
	reply := createLog()
	reply.Address = gointerfaces.ConvertAddressToH160(common.Address{7, 1})
	b.ReportAllocs()
	for b.Loop() {
		a.distributeLog(reply)
	}
}
