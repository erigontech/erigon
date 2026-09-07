// Copyright 2025 The Erigon Authors
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
	"math/big"
	"slices"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// BenchmarkBALReadsThenWrites is the shape removeStorageRead is quadratic in:
// an account accumulates reads, then writes disjoint slots, and every write
// used to rescan the whole read list.
func BenchmarkBALReadsThenWrites(b *testing.B) {
	addr := accounts.InternAddress(common.HexToAddress("0xbeef"))
	val := *uint256.NewInt(1)
	for _, n := range []int{64, 512, 4096} {
		reads := make([]accounts.StorageKey, n)
		writes := make([]accounts.StorageKey, n)
		for i := range reads {
			reads[i] = accounts.InternKey(common.BigToHash(new(big.Int).SetInt64(int64(i))))
			writes[i] = accounts.InternKey(common.BigToHash(new(big.Int).SetInt64(int64(i + 1<<20))))
		}
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				account := newAccountState(addr)
				for _, k := range reads {
					account.updateReadStorage(k, uint256.Int{})
				}
				for i, k := range writes {
					account.applyWriteStorage(k, val, uint32(i))
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/slot")
		})
	}
}

// BenchmarkBALAccounts covers the other shape of a block: many accounts, few
// slots each. It ends where asBlockAccessList does, with the encoded list
// assembled and ordered.
func BenchmarkBALAccounts(b *testing.B) {
	const slotsPerAccount = 4
	for _, nAccounts := range []int{64, 1024, 8192} {
		addrs := make([]accounts.Address, nAccounts)
		for i := range addrs {
			addrs[i] = accounts.InternAddress(common.BigToAddress(new(big.Int).SetInt64(int64(i))))
		}
		keys := make([]accounts.StorageKey, slotsPerAccount)
		for i := range keys {
			keys[i] = accounts.InternKey(common.BigToHash(new(big.Int).SetInt64(int64(i))))
		}
		val := *uint256.NewInt(1)
		b.Run(fmt.Sprintf("accounts=%d", nAccounts), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				bal := make(types.BlockAccessList, 0, nAccounts)
				for _, addr := range addrs {
					account := newAccountState(addr)
					for i, k := range keys {
						account.updateReadStorage(k, uint256.Int{})
						account.applyWriteStorage(k, val, uint32(i))
					}
					account.finalize()
					account.changes.Normalize()
					bal = append(bal, *account.changes)
				}
				slices.SortFunc(bal, func(a, b types.AccountChanges) int { return a.Address.Cmp(b.Address) })
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*nAccounts), "ns/account")
		})
	}
}

// BenchmarkAccountStateStorageWrites drives the EIP-7928 BAL builder the way a
// storage-bloated block does: one account, many distinct slots, each read then
// written once. The per-slot work must not grow with the number of slots
// already recorded for the account.
func BenchmarkAccountStateStorageWrites(b *testing.B) {
	addr := accounts.InternAddress(common.HexToAddress("0xbeef"))
	val := *uint256.NewInt(1)
	for _, slots := range []int{4, 64, 512, 4096, 32768} {
		keys := make([]accounts.StorageKey, slots)
		for i := range keys {
			keys[i] = accounts.InternKey(common.BigToHash(new(big.Int).SetInt64(int64(i))))
		}
		b.Run(fmt.Sprintf("slots=%d", slots), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				account := newAccountState(addr)
				for i, k := range keys {
					account.updateReadStorage(k, uint256.Int{})
					account.applyWriteStorage(k, val, uint32(i))
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*slots), "ns/slot")
		})
	}
}
