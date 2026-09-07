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

package state

import (
	"testing"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// BenchmarkNoMaterializeTx models a parallel-exec transaction: reset, then read
// the account fields of a set of distinct accounts.
func BenchmarkNoMaterializeTx(b *testing.B) {
	acc := accounts.NewAccount()
	acc.Nonce = 3
	acc.Balance.SetUint64(77)

	addrs := make([]accounts.Address, 64)
	for i := range addrs {
		addrs[i] = accounts.InternAddress([20]byte{0xAB, byte(i), byte(i >> 8)})
	}

	ibs, vm := newNoMaterializeIBS(&anyAccountReader{acc: &acc})
	defer ibs.Close()

	b.ReportAllocs()
	for b.Loop() {
		startNoMaterializeTx(ibs, vm, 0)
		for _, a := range addrs {
			_, _ = ibs.GetBalance(a)
			_, _ = ibs.GetNonce(a)
			_, _ = ibs.GetCodeHash(a)
		}
	}
}
