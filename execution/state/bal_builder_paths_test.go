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
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestBlockAccessListBuilderRegistersValuelessWrites(t *testing.T) {
	addr := accounts.InternAddress(common.Address{19: 0x42})
	header := func(path AccountPath) WriteHeader { return WriteHeader{Address: addr, Path: path} }
	for name, write := range map[string]func(ws *WriteSet){
		"account": func(ws *WriteSet) {
			ws.SetAddress(addr, &VersionedWrite[*accounts.Account]{WriteHeader: header(AddressPath), Val: &accounts.Account{}})
		},
		"incarnation": func(ws *WriteSet) {
			ws.SetIncarnation(addr, &VersionedWrite[uint64]{WriteHeader: header(IncarnationPath), Val: 1})
		},
		"self-destruct not taken": func(ws *WriteSet) {
			ws.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: header(SelfDestructPath)})
		},
		"create contract": func(ws *WriteSet) {
			ws.SetCreateContract(addr, &VersionedWrite[bool]{WriteHeader: header(CreateContractPath), Val: true})
		},
		"code hash": func(ws *WriteSet) {
			ws.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: header(CodeHashPath)})
		},
		"code size": func(ws *WriteSet) {
			ws.SetCodeSize(addr, &VersionedWrite[int]{WriteHeader: header(CodeSizePath), Val: 2})
		},
	} {
		t.Run(name, func(t *testing.T) {
			ws := &WriteSet{}
			write(ws)
			b := NewBlockAccessListBuilder()
			b.Add(-1, ReadSet{}, ws)
			bal := b.Build()
			require.Len(t, bal, 1)
			require.Equal(t, addr.Value(), bal[0].Address)
		})
	}
}

func TestBlockAccessListBuilderDropsEmptyCodeOfContractCreatedInBlock(t *testing.T) {
	addr := accounts.InternAddress(common.Address{19: 0x43})
	ws := &WriteSet{}
	ws.SetCreateContract(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: CreateContractPath}, Val: true})
	ws.SetCode(addr, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath}, Val: accounts.NewCode(nil)})
	b := NewBlockAccessListBuilder()
	b.Add(-1, ReadSet{}, ws)
	bal := b.Build()
	require.Len(t, bal, 1)
	require.Empty(t, bal[0].CodeChanges)
}

// TestBlockAccessListBuilderReadOnlyAccountsDoNotAllocatePerAccount pins that an account
// with only reads gets no per-account state, so a block reading many accounts does not
// allocate one per account on the serial apply path.
func TestBlockAccessListBuilderReadOnlyAccountsDoNotAllocatePerAccount(t *testing.T) {
	const n = 1000
	rs := ReadSet{access: AccessSet{}}
	for i := range n {
		addr := accounts.InternAddress(common.BigToAddress(big.NewInt(int64(i + 1))))
		rs.SetBalance(addr, VersionedRead[uint256.Int]{Val: *uint256.NewInt(7)})
		rs.access[addr] = accessOptions{}
	}
	allocs := testing.AllocsPerRun(1, func() { NewBlockAccessListBuilder().Add(-1, rs, nil) })
	require.Less(t, allocs, float64(n/10))
	b := NewBlockAccessListBuilder()
	b.Add(-1, rs, nil)

	bal := b.Build()
	require.Len(t, bal, n)
	again := NewBlockAccessListBuilder()
	again.Add(-1, rs, nil)
	require.True(t, again.Matches(bal))
	for i := range bal {
		require.Equal(t, types.AccountChanges{Address: bal[i].Address}, bal[i])
	}
}

// TestBlockAccessListBuilderReplaysReadsOfReadOnlyAccount pins that an account
// becoming written keeps its earlier balance reads: the first one as the
// pre-block balance, the last one as the latest balance.
func TestBlockAccessListBuilderReplaysReadsOfReadOnlyAccount(t *testing.T) {
	addr := accounts.InternAddress(common.Address{19: 0x42})
	for _, write := range []uint64{10, 20, 30} {
		t.Run(fmt.Sprint(write), func(t *testing.T) {
			b := NewBlockAccessListBuilder()
			want := newAccountState(addr)
			for i, read := range []uint64{10, 20} {
				var rs ReadSet
				rs.SetBalance(addr, VersionedRead[uint256.Int]{Val: *uint256.NewInt(read)})
				b.Add(i-1, rs, nil)
				want.updateReadBalance(*uint256.NewInt(read))
			}
			ws := &WriteSet{}
			ws.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 1}}, Val: *uint256.NewInt(write)})
			b.Add(1, ReadSet{}, ws)
			want.applyWriteBalance(*uint256.NewInt(write), Version{TxIndex: 1}.blockAccessIndex())
			want.finalize()
			require.Equal(t, types.BlockAccessList{want.changes}, b.Build())
		})
	}
}

// TestBlockAccessListBuilderSizedFromBALAddsReadOnlyAccountsWithoutAllocating pins that a
// builder sized from the block's access list does not grow while it registers the accounts.
func TestBlockAccessListBuilderSizedFromBALAddsReadOnlyAccountsWithoutAllocating(t *testing.T) {
	const n = 1000
	rs := ReadSet{access: AccessSet{}}
	bal := make(types.BlockAccessList, n)
	for i := range bal {
		bal[i].Address = common.BigToAddress(big.NewInt(int64(i + 1)))
		addr := accounts.InternAddress(bal[i].Address)
		rs.SetBalance(addr, VersionedRead[uint256.Int]{Val: *uint256.NewInt(7)})
		rs.access[addr] = accessOptions{}
	}
	const runs = 10
	vm := NewVersionMap(bal)
	builders := make([]*BlockAccessListBuilder, runs+1) // AllocsPerRun makes one warm-up call
	for i := range builders {
		builders[i] = NewBlockAccessListBuilderFor(vm)
	}
	next := 0
	allocs := testing.AllocsPerRun(runs, func() {
		builders[next].Add(-1, rs, nil)
		next++
	})
	require.Zero(t, allocs)
	require.True(t, builders[0].Matches(bal))
}

func TestBlockAccessListBuilderBuildKeepsReadOnlyAccountsFlat(t *testing.T) {
	read, written := accounts.InternAddress(common.Address{19: 0x01}), accounts.InternAddress(common.Address{19: 0x02})
	rs := ReadSet{access: AccessSet{}}
	rs.SetBalance(read, VersionedRead[uint256.Int]{Val: *uint256.NewInt(7)})
	rs.access[read] = accessOptions{}
	ws := &WriteSet{}
	ws.SetBalance(written, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: written, Path: BalancePath}, Val: *uint256.NewInt(9)})
	b := NewBlockAccessListBuilder()
	b.Add(-1, rs, ws)

	bal := b.Build()

	require.Len(t, bal, 2)
	require.Equal(t, read.Value(), bal[0].Address)
	require.Empty(t, bal[0].BalanceChanges)
	require.Equal(t, written.Value(), bal[1].Address)
	require.NotEmpty(t, bal[1].BalanceChanges)
	require.NotContains(t, b.ac, read, "Build promoted a read-only account")
	require.True(t, b.Matches(bal))
}

// TestBlockAccessListBuilderReplaysEmptyCodeHashReadOfReadOnlyAccount pins that an empty
// code-hash read made while the account had only reads still marks its pre-block code empty,
// so a later change back to empty code is net-zero and stays out of the list.
func TestBlockAccessListBuilderReplaysEmptyCodeHashReadOfReadOnlyAccount(t *testing.T) {
	addr := accounts.InternAddress(common.Address{19: 0x44})
	b := NewBlockAccessListBuilder()
	var rs ReadSet
	rs.SetCodeHash(addr, VersionedRead[accounts.CodeHash]{Val: accounts.EmptyCodeHash})
	b.Add(-1, rs, nil)
	ws := &WriteSet{}
	ws.SetCode(addr, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath, Version: Version{TxIndex: 0}}, Val: accounts.NewCode(nil)})
	b.Add(0, ReadSet{}, ws)
	bal := b.Build()
	require.Len(t, bal, 1)
	require.Empty(t, bal[0].CodeChanges)
}
