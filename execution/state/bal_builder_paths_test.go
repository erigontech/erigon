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
	"math/big"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
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

func balTestAddr(n int) accounts.Address {
	return accounts.InternAddress(common.BigToAddress(big.NewInt(int64(n + 1))))
}

func balTestKey(n int) accounts.StorageKey {
	return accounts.InternKey(common.BigToHash(big.NewInt(int64(n))))
}

// randomBALTx reads cold accounts, then drives a small pool of accounts through every path the builder folds.
func randomBALTx(r *rand.Rand, txIndex, cold int) (ReadSet, *WriteSet) {
	var rs ReadSet
	ws := &WriteSet{}
	for range cold {
		rs.SetBalance(balTestAddr(1000+r.Intn(1_000_000)), VersionedRead[uint256.Int]{Val: *uint256.NewInt(uint64(r.Intn(3)))})
	}
	for range 400 {
		a := balTestAddr(r.Intn(64))
		k := balTestKey(r.Intn(16))
		v := *uint256.NewInt(uint64(r.Intn(3)))
		hdr := WriteHeader{Address: a, Version: Version{TxIndex: txIndex}}
		read := ReadHeader{internal: r.Intn(8) == 0}
		switch r.Intn(12) {
		case 0:
			rs.SetBalance(a, VersionedRead[uint256.Int]{ReadHeader: read, Val: v})
		case 1:
			rs.SetStorage(a, k, VersionedRead[uint256.Int]{ReadHeader: read, Val: v})
		case 2:
			rs.SetCodeHash(a, VersionedRead[accounts.CodeHash]{ReadHeader: read, Val: accounts.EmptyCodeHash})
		case 3:
			rs.SetNonce(a, VersionedRead[uint64]{ReadHeader: read, Val: uint64(r.Intn(3))})
		case 4:
			if rs.access == nil {
				rs.access = make(AccessSet)
			}
			rs.access[a] = accessOptions{}
		case 5:
			ws.SetBalance(a, &VersionedWrite[uint256.Int]{WriteHeader: hdr, Val: v})
		case 6:
			ws.SetNonce(a, &VersionedWrite[uint64]{WriteHeader: hdr, Val: uint64(r.Intn(3))})
		case 7:
			ws.SetStorage(a, k, &VersionedWrite[uint256.Int]{WriteHeader: hdr, Val: v})
		case 8:
			code := []byte{0x60, byte(r.Intn(3))}
			if r.Intn(2) == 0 {
				code = nil
			}
			ws.SetCode(a, &VersionedWrite[accounts.Code]{WriteHeader: hdr, Val: accounts.NewCode(code)})
		case 9:
			ws.SetCreateContract(a, &VersionedWrite[bool]{WriteHeader: hdr, Val: r.Intn(2) == 0})
		case 10:
			ws.SetSelfDestruct(a, &VersionedWrite[bool]{WriteHeader: hdr, Val: r.Intn(2) == 0})
		case 11:
			ws.SetIncarnation(a, &VersionedWrite[uint64]{WriteHeader: hdr, Val: 1})
		}
	}
	if r.Intn(6) == 0 {
		ws = nil
	}
	return rs, ws
}

func TestBlockAccessListBuilderSplitsLargeTxAcrossShards(t *testing.T) {
	rs, ws := randomBALTx(rand.New(rand.NewSource(1)), -1, 3*matchesPerWorker)
	b := newBALBuilder(8)
	b.Add(-1, rs, ws)
	used := 0
	for _, shard := range b.ac {
		if len(shard) > 0 {
			used++
		}
	}
	require.Greater(t, used, 1)
}

// A block folded across shards must give the same list as one folded serially, including for the small
// txs that stay on the serial path.
func TestBlockAccessListBuilderShardsMatchSerial(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	for range 12 {
		serial, sharded, matcher := newBALBuilder(1), newBALBuilder(8), newBALBuilder(8)
		for txIndex := -1; txIndex < 5; txIndex++ {
			cold := 2*matchesPerWorker + r.Intn(matchesPerWorker)
			if r.Intn(2) == 0 {
				cold = r.Intn(16)
			}
			rs, ws := randomBALTx(r, txIndex, cold)
			serial.Add(txIndex, rs, ws)
			sharded.Add(txIndex, rs, ws)
			matcher.Add(txIndex, rs, ws)
		}
		want := serial.Build()
		require.NotEmpty(t, want)
		require.True(t, matcher.Matches(want))
		require.Equal(t, want, sharded.Build())
	}
}
