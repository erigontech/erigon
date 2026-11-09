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

package jsonrpc

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func BenchmarkGetFilterChanges(b *testing.B) {
	const items = 1000
	ff := rpchelper.New(b.Context(), rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, log.New(), nil)
	api := &APIImpl{BaseAPI: &BaseAPI{filters: ff}}

	_, headsID, err := ff.SubscribeNewHeads(32, rpchelper.ProtocolHTTP)
	if err != nil {
		b.Fatal(err)
	}
	_, logsID, err := ff.SubscribeLogs(32, filters.FilterCriteria{}, rpchelper.ProtocolHTTP)
	if err != nil {
		b.Fatal(err)
	}
	header := &types.Header{}
	lg := &types.Log{Address: common.Address{1}}

	run := func(b *testing.B, id string, fill func()) {
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			fill()
			b.StartTimer()
			changes, err := api.GetFilterChanges(b.Context(), "0x"+id)
			if err != nil {
				b.Fatal(err)
			}
			if len(changes) != items {
				b.Fatalf("got %d changes, want %d", len(changes), items)
			}
		}
	}
	b.Run("heads", func(b *testing.B) {
		run(b, string(headsID), func() {
			for range items {
				ff.AddPendingBlock(headsID, header)
			}
		})
	})
	b.Run("logs", func(b *testing.B) {
		run(b, string(logsID), func() {
			for range items {
				ff.AddLogs(logsID, lg)
			}
		})
	})
}
