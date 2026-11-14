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
	"math/rand/v2"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func matchTestBuilder() *BlockAccessListBuilder {
	b := NewBlockAccessListBuilder()
	addr := func(n byte) accounts.Address { return accounts.InternAddress(common.Address{19: n}) }
	key := func(n int) accounts.StorageKey { return accounts.InternKey(common.Hash{30: byte(n >> 8), 31: byte(n)}) }

	b.readBalance(addr(1), *uint256.NewInt(7))

	writer := ensureAccountState(b.ac, addr(2))
	writer.updateReadBalance(*uint256.NewInt(100))
	writer.applyWriteBalance(*uint256.NewInt(90), 1)
	writer.applyWriteBalance(*uint256.NewInt(80), 3)
	writer.applyWriteNonce(5, 1)
	writer.applyWriteCode(accounts.Code{Bytes: []byte{0x60, 0x00}}, 2)

	slots := ensureAccountState(b.ac, addr(3))
	slots.updateReadStorage(key(3), *uint256.NewInt(1))
	slots.updateReadStorage(key(1), *uint256.NewInt(1))
	slots.updateReadStorage(key(3), *uint256.NewInt(1))
	slots.updateReadStorage(key(2), *uint256.NewInt(1))
	slots.applyWriteStorage(key(2), *uint256.NewInt(9), 1)
	slots.applyWriteStorage(key(4), *uint256.NewInt(8), 1)
	slots.applyWriteStorage(key(4), *uint256.NewInt(6), 2)

	bulk := ensureAccountState(b.ac, addr(4))
	for i := 40; i > 0; i-- {
		bulk.updateReadStorage(key(1000+i), *uint256.NewInt(1))
		bulk.applyWriteStorage(key(i), *uint256.NewInt(uint64(i)), uint32(i%3+1))
	}
	return b
}

func TestBlockAccessListBuilderMatchesAgreesWithHash(t *testing.T) {
	want := matchTestBuilder().Build()
	require.NoError(t, want.Validate())
	require.True(t, matchTestBuilder().Matches(want))

	key := accounts.InternKey(common.Hash{31: 0xee})
	perturb := map[string]func(bal types.BlockAccessList) types.BlockAccessList{
		"drop account": func(bal types.BlockAccessList) types.BlockAccessList { return bal[1:] },
		"extra account": func(bal types.BlockAccessList) types.BlockAccessList {
			return append(bal, types.AccountChanges{Address: common.Address{19: 9}})
		},
		"other address": func(bal types.BlockAccessList) types.BlockAccessList { bal[0].Address[0] = 1; return bal },
		"balance value": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[1].BalanceChanges[0].Value.SetUint64(91)
			return bal
		},
		"balance index": func(bal types.BlockAccessList) types.BlockAccessList { bal[1].BalanceChanges[1].Index = 4; return bal },
		"missing balance": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[1].BalanceChanges = bal[1].BalanceChanges[:1]
			return bal
		},
		"nonce value": func(bal types.BlockAccessList) types.BlockAccessList { bal[1].NonceChanges[0].Value = 6; return bal },
		"code bytes": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[1].CodeChanges[0].Bytecode = []byte{0x60}
			return bal
		},
		"storage value": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[2].StorageChanges[1].Changes[1].Value.SetUint64(7)
			return bal
		},
		"storage slot": func(bal types.BlockAccessList) types.BlockAccessList { bal[2].StorageChanges[0].Slot = key; return bal },
		"missing storage": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[3].StorageChanges = bal[3].StorageChanges[1:]
			return bal
		},
		"missing read": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[2].StorageReads = bal[2].StorageReads[1:]
			return bal
		},
		"extra read": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[0].StorageReads = []common.Hash{key.Value()}
			return bal
		},
		"read for a write": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[3].StorageReads[0] = bal[3].StorageChanges[0].Slot.Value()
			return bal
		},
		"read for a slot read then written": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[2].StorageReads[1] = bal[2].StorageChanges[0].Slot.Value()
			return bal
		},
		"duplicate read": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[3].StorageReads[1] = bal[3].StorageReads[0]
			return bal
		},
		"unordered reads": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[3].StorageReads[0], bal[3].StorageReads[1] = bal[3].StorageReads[1], bal[3].StorageReads[0]
			return bal
		},
		"duplicate slot": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[3].StorageChanges[1] = bal[3].StorageChanges[0]
			return bal
		},
		"unordered slots": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[3].StorageChanges[0], bal[3].StorageChanges[1] = bal[3].StorageChanges[1], bal[3].StorageChanges[0]
			return bal
		},
		"empty storage entry": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[0].StorageChanges = []types.SlotChanges{{Slot: key}}
			return bal
		},
		"balance of a read-only account": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[0].BalanceChanges = []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(8)}}
			return bal
		},
		"nonce of a read-only account": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[0].NonceChanges = []*types.NonceChange{{Index: 1, Value: 1}}
			return bal
		},
		"code of a read-only account": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[0].CodeChanges = []*types.CodeChange{{Index: 1, Bytecode: []byte{0x60}}}
			return bal
		},
		"storage change of a read-only account": func(bal types.BlockAccessList) types.BlockAccessList {
			bal[0].StorageChanges = []types.SlotChanges{{Slot: key, Changes: []*types.StorageChange{{Index: 1, Value: *uint256.NewInt(1)}}}}
			return bal
		},
	}
	for name, p := range perturb {
		t.Run(name, func(t *testing.T) {
			other := p(want.Copy())
			require.NotEqual(t, want.Hash(), other.Hash())
			b := matchTestBuilder()
			require.False(t, b.Matches(other))
			require.Equal(t, want.Hash(), b.Build().Hash())
		})
	}

	t.Run("nil and empty code encode alike", func(t *testing.T) {
		withEmpty := want.Copy()
		withEmpty[0].CodeChanges = []*types.CodeChange{{Index: 1, Bytecode: []byte{}}}
		withNil := want.Copy()
		withNil[0].CodeChanges = []*types.CodeChange{{Index: 1}}
		require.Equal(t, withEmpty.Hash(), withNil.Hash())
		require.True(t, withEmpty[0].Equal(&withNil[0]))
	})
}

func TestBlockAccessListBuilderMatchesAcrossWorkers(t *testing.T) {
	addrs := make([]accounts.Address, 3*matchesPerWorker+5)
	for i := range addrs {
		addrs[i] = accounts.InternAddress(common.Address{0: byte(i >> 16), 1: byte(i >> 8), 2: byte(i), 19: 1})
	}
	build := func() *BlockAccessListBuilder {
		b := NewBlockAccessListBuilder()
		for i, a := range addrs {
			if i%7 != 0 {
				b.readBalance(a, *uint256.NewInt(uint64(i)))
				continue
			}
			account := ensureAccountState(b.ac, a)
			account.updateReadBalance(*uint256.NewInt(uint64(i)))
			account.applyWriteBalance(*uint256.NewInt(uint64(i) + 1), uint32(i%5+1))
		}
		return b
	}
	want := build().Build()
	require.True(t, build().Matches(want))

	changed := want.Copy()
	changed[2*matchesPerWorker+3].BalanceChanges = []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(9)}}
	require.False(t, build().Matches(changed))

	readOnlyRead := want.Copy()
	readOnlyRead[matchesPerWorker+2].StorageReads = []common.Hash{{31: 1}}
	require.False(t, build().Matches(readOnlyRead))

	swapped := want.Copy()
	swapped[matchesPerWorker], swapped[matchesPerWorker+1] = swapped[matchesPerWorker+1], swapped[matchesPerWorker]
	require.False(t, build().Matches(swapped))

	duplicated := want.Copy()
	step := (len(want) + 2) / 3
	duplicated[0], duplicated[step+1] = want[len(want)-1], want[len(want)-1]
	require.False(t, build().Matches(duplicated))
}

func TestBlockAccessListBuilderMatchesDoesNotFinalize(t *testing.T) {
	want := matchTestBuilder().Build()
	b := matchTestBuilder()
	require.True(t, b.Matches(want))
	for _, account := range b.ac {
		require.Nil(t, account.changes.StorageReads)
		require.Nil(t, account.changes.BalanceChanges)
	}
	require.Equal(t, want.Hash(), b.Build().Hash())
}

func TestBlockAccessListBuilderMatchesRandomHistories(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	addr := accounts.InternAddress(common.Address{19: 1})
	key := func(n int) accounts.StorageKey { return accounts.InternKey(common.Hash{31: byte(n)}) }
	for range 500 {
		type op struct {
			write     bool
			slot      int
			value     uint64
			accessIdx uint32
		}
		ops := make([]op, r.IntN(80))
		for i := range ops {
			ops[i] = op{write: r.IntN(2) == 0, slot: r.IntN(40), value: uint64(r.IntN(3)), accessIdx: uint32(i + 1)}
		}
		build := func() *BlockAccessListBuilder {
			b := NewBlockAccessListBuilder()
			account := ensureAccountState(b.ac, addr)
			for _, o := range ops {
				if o.write {
					account.applyWriteStorage(key(o.slot), *uint256.NewInt(o.value), o.accessIdx)
				} else {
					account.updateReadStorage(key(o.slot), *uint256.NewInt(o.value))
				}
			}
			return b
		}
		want := build().Build()
		require.NoError(t, want.Validate())
		require.True(t, build().Matches(want))

		other := want.Copy()
		switch {
		case len(other[0].StorageReads) > 0 && r.IntN(2) == 0:
			other[0].StorageReads[len(other[0].StorageReads)-1] = key(200).Value()
		case len(other[0].StorageChanges) > 0:
			other[0].StorageChanges[0].Changes[0].Value.AddUint64(&other[0].StorageChanges[0].Changes[0].Value, 7)
		default:
			continue
		}
		require.NotEqual(t, want.Hash(), other.Hash())
		require.False(t, build().Matches(other))
	}
}
