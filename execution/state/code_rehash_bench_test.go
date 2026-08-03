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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// benchContractIBS builds a noMaterialize IBS over one committed contract and
// warms the CodePath read set the way an EVM call does, so the account-field
// reads that follow take the getStateObject fall-through.
func benchContractIBS(tb testing.TB, codeLen int) (*IntraBlockState, accounts.Address) {
	tb.Helper()

	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	code := make([]byte, codeLen)
	for i := range code {
		code[i] = byte(i)
	}

	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1
	acc.Balance.SetUint64(1000)
	acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))

	ibs := NewWithVersionMap(&codeReader{addr: addr, account: &acc, code: code}, NewVersionMap(nil))
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	got, err := ibs.GetCode(addr)
	require.NoError(tb, err)
	require.Len(tb, got, codeLen)

	return ibs, addr
}

// BenchmarkGetStateObjectAfterCodeRead measures the state-object rebuild that
// every account-field read falls through to on a contract whose code this tx
// already read. Under noMaterialize nothing is cached, so a per-rebuild
// re-hash of the bytecode shows up as growth across the code sizes.
func BenchmarkGetStateObjectAfterCodeRead(b *testing.B) {
	for _, codeLen := range []int{32, 1024, 24576} {
		b.Run(fmt.Sprintf("code=%dB", codeLen), func(b *testing.B) {
			ibs, addr := benchContractIBS(b, codeLen)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := ibs.getStateObject(addr, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestCommittedCodeHashMatchesAccountRecord pins that a state object rebuilt
// for a contract whose code came from committed state carries the account
// record's code hash on both obj.data and obj.code.
func TestCommittedCodeHashMatchesAccountRecord(t *testing.T) {
	ibs, addr := benchContractIBS(t, 4096)

	so, err := ibs.getStateObject(addr, false)
	require.NoError(t, err)
	require.NotNil(t, so)

	expected := accounts.InternCodeHash(crypto.Keccak256Hash(so.code.Bytes))
	require.Equal(t, expected, so.data.CodeHash, "committed code and account hash must agree")
	require.Equal(t, expected, so.code.Hash)
}
