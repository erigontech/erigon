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
	"encoding/json"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

func benchWithdrawals() Withdrawals {
	const maxWithdrawalsPerPayload = 16
	ws := make(Withdrawals, maxWithdrawalsPerPayload)
	for i := range ws {
		ws[i] = &Withdrawal{
			Index:     hexutil.Uint64(19_000_000 + i),
			Validator: hexutil.Uint64(881_234 + i),
			Address:   common.Address{byte(i), 0xab, 0xcd},
			Amount:    hexutil.Uint64(63_012_345 + i),
		}
	}
	return ws
}

func BenchmarkWithdrawalsMarshalJSON(b *testing.B) {
	ws := benchWithdrawals()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := json.Marshal(ws); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWithdrawalsUnmarshalJSON(b *testing.B) {
	encoded, err := json.Marshal(benchWithdrawals())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		var ws Withdrawals
		if err := json.Unmarshal(encoded, &ws); err != nil {
			b.Fatal(err)
		}
	}
}
