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

package types

import (
	"encoding/json"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

func benchmarkLogs(n int) RPCLogs {
	topic := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	logs := make(RPCLogs, n)
	for i := range logs {
		logs[i] = &RPCLog{
			Log: Log{
				Address: common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
				Topics:  []common.Hash{topic, {}, {}},
				Data:    make([]byte, 64),
			},
			BlockTimestamp: hexutil.Uint64(1700000000),
		}
	}
	return logs
}

func BenchmarkRPCLogsMarshalReflect(b *testing.B) {
	logs := benchmarkLogs(1000)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := json.Marshal(logs); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRPCLogsMarshalFast(b *testing.B) {
	logs := benchmarkLogs(1000)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := logs.MarshalFastJSON(); err != nil {
			b.Fatal(err)
		}
	}
}
