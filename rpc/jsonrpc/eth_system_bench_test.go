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

	"github.com/erigontech/erigon/execution/chain"
)

func BenchmarkGasPriceCached(b *testing.B) {
	m := createGasPriceTestKV(b, chain.TestChainOsakaConfig, 30)
	defer m.DB.Close()
	eth := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	ctx := b.Context()
	if _, err := eth.GasPrice(ctx); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := eth.GasPrice(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
