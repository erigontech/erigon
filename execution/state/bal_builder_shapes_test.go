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
	"encoding/binary"
	"math/rand/v2"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type shapeTx struct {
	rs ReadSet
	ws *WriteSet
}

type shapeGen struct {
	txIndex int
	rs      ReadSet
	ws      *WriteSet
}

func shapeAddr(space, i int) accounts.Address {
	var a common.Address
	binary.BigEndian.PutUint32(a[:4], uint32(space))
	binary.BigEndian.PutUint64(a[12:], uint64(i))
	return accounts.InternAddress(a)
}

func shapeKey(i int) accounts.StorageKey {
	var k common.Hash
	binary.BigEndian.PutUint64(k[24:], uint64(i))
	return accounts.InternKey(k)
}

func (g *shapeGen) header(addr accounts.Address, path AccountPath) WriteHeader {
	return WriteHeader{Address: addr, Path: path, Version: Version{TxIndex: g.txIndex}}
}

func (g *shapeGen) readBalance(addr accounts.Address, v uint64) {
	g.rs.SetBalance(addr, VersionedRead[uint256.Int]{Val: *uint256.NewInt(v)})
}

func (g *shapeGen) readAccount(addr accounts.Address, exists bool) {
	var view AccountView
	if exists {
		view = NewAccountView(&accounts.Account{})
	}
	g.rs.SetAddress(addr, VersionedRead[AccountView]{Val: view})
}

func (g *shapeGen) writeBalance(addr accounts.Address, v uint64) {
	g.ws.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: g.header(addr, BalancePath), Val: *uint256.NewInt(v)})
}

func (g *shapeGen) writeNonce(addr accounts.Address, v uint64) {
	g.ws.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: g.header(addr, NoncePath), Val: v})
}

func (g *shapeGen) readSlot(addr accounts.Address, slot int, v uint64) {
	g.rs.SetStorage(addr, shapeKey(slot), VersionedRead[uint256.Int]{Val: *uint256.NewInt(v)})
}

func (g *shapeGen) writeSlot(addr accounts.Address, slot int, v uint64) {
	h := g.header(addr, StoragePath)
	h.Key = shapeKey(slot)
	g.ws.SetStorage(addr, h.Key, &VersionedWrite[uint256.Int]{WriteHeader: h, Val: *uint256.NewInt(v)})
}

// shapeBlock calls tx for every index from the system call -1 to n-1 and collects what it records.
func shapeBlock(n int, tx func(g *shapeGen)) []shapeTx {
	txs := make([]shapeTx, 0, n+1)
	for i := -1; i < n; i++ {
		g := &shapeGen{txIndex: i, ws: &WriteSet{}}
		tx(g)
		txs = append(txs, shapeTx{rs: g.rs, ws: g.ws})
	}
	return txs
}

func buildShape(txs []shapeTx) *BlockAccessListBuilder {
	b := NewBlockAccessListBuilder()
	for _, tx := range txs {
		b.Add(b.Next(), tx.rs, tx.ws)
	}
	return b
}

func transfersShape(n int) []shapeTx {
	coinbase := shapeAddr(9, 0)
	return shapeBlock(n, func(g *shapeGen) {
		if g.txIndex < 0 {
			g.readBalance(coinbase, 0)
			return
		}
		from, to := shapeAddr(1, g.txIndex), shapeAddr(2, g.txIndex)
		g.readAccount(from, true)
		g.readBalance(from, 1_000_000)
		g.rs.SetNonce(from, VersionedRead[uint64]{})
		g.rs.SetCodeHash(from, VersionedRead[accounts.CodeHash]{Val: accounts.EmptyCodeHash})
		g.readAccount(to, false)
		g.readBalance(to, 0)
		g.writeBalance(from, 1_000_000-21_000-uint64(g.txIndex%3))
		g.writeNonce(from, 1)
		g.writeBalance(to, uint64(g.txIndex%3))
		g.writeBalance(coinbase, uint64(g.txIndex+1)*21_000)
	})
}

func contractsShape(n int) []shapeTx {
	return shapeBlock(n, func(g *shapeGen) {
		if g.txIndex < 0 {
			return
		}
		from, to := shapeAddr(1, g.txIndex), shapeAddr(3, g.txIndex)
		g.readAccount(from, true)
		g.readBalance(from, 1_000_000)
		g.writeBalance(from, 1_000_000-60_000)
		g.writeNonce(from, 1)
		g.readAccount(to, true)
		g.readBalance(to, 0)
		g.writeBalance(to, 1)
		g.rs.SetCode(to, VersionedRead[accounts.Code]{Val: accounts.Code{Bytes: []byte{0x60, 0x00}}})
		g.rs.SetCodeHash(to, VersionedRead[accounts.CodeHash]{Val: accounts.InternCodeHash(common.Hash{1})})
		g.readSlot(to, 0, 1)
		g.writeSlot(to, 0, 2)
	})
}

func sharedSlotsShape(n int) []shapeTx {
	store := shapeAddr(4, 0)
	return shapeBlock(n, func(g *shapeGen) {
		if g.txIndex < 0 {
			return
		}
		i := g.txIndex
		if i%3 == 0 {
			to := shapeAddr(2, i)
			g.readBalance(to, 0)
			g.writeBalance(to, 1)
			return
		}
		g.readAccount(store, true)
		g.readSlot(store, i-i%3, uint64(i%3))
		switch i % 3 {
		case 1:
			g.writeSlot(store, i, uint64(i%5))
		case 2:
			g.writeSlot(store, i-1, uint64(i%5))
		}
	})
}

// randomShape draws every recorded path over a small address and slot pool, so accounts
// repeat across txs and cross the slot index threshold.
func randomShape(seed uint64, n int) []shapeTx {
	r := rand.New(rand.NewPCG(seed, seed+1))
	addr := func() accounts.Address {
		if r.IntN(40) == 0 {
			return accounts.NilAddress
		}
		return shapeAddr(5, r.IntN(24))
	}
	val := func() uint64 { return uint64(r.IntN(4)) }
	return shapeBlock(n, func(g *shapeGen) {
		for range 1 + r.IntN(12) {
			a := addr()
			internal := r.IntN(6) == 0
			hdr := ReadHeader{internal: internal}
			switch r.IntN(17) {
			case 0:
				g.rs.SetBalance(a, VersionedRead[uint256.Int]{ReadHeader: hdr, Val: *uint256.NewInt(val())})
			case 1:
				g.rs.SetStorage(a, shapeKey(r.IntN(40)), VersionedRead[uint256.Int]{ReadHeader: hdr, Val: *uint256.NewInt(val())})
			case 2:
				var view AccountView
				switch r.IntN(3) {
				case 0:
					view = NewAccountView(&accounts.Account{})
				case 1:
					view = NewAccountView(nil)
				}
				g.rs.SetAddress(a, VersionedRead[AccountView]{ReadHeader: hdr, Val: view})
			case 3:
				g.rs.SetNonce(a, VersionedRead[uint64]{ReadHeader: hdr})
			case 4:
				g.rs.SetIncarnation(a, VersionedRead[uint64]{ReadHeader: hdr})
			case 5:
				g.rs.SetSelfDestruct(a, VersionedRead[bool]{ReadHeader: hdr, Val: r.IntN(2) == 0})
			case 6:
				g.rs.SetCreateContract(a, VersionedRead[bool]{ReadHeader: hdr})
			case 7:
				g.rs.SetCode(a, VersionedRead[accounts.Code]{ReadHeader: hdr})
			case 8:
				h := accounts.EmptyCodeHash
				if r.IntN(2) == 0 {
					h = accounts.InternCodeHash(common.Hash{byte(r.IntN(3) + 1)})
				}
				g.rs.SetCodeHash(a, VersionedRead[accounts.CodeHash]{ReadHeader: hdr, Val: h})
			case 9:
				g.rs.SetCodeSize(a, VersionedRead[int]{ReadHeader: hdr})
			case 10:
				if g.rs.access == nil {
					g.rs.access = AccessSet{}
				}
				g.rs.access[a] = accessOptions{revertable: r.IntN(2) == 0}
			case 11:
				g.ws.SetBalance(a, &VersionedWrite[uint256.Int]{WriteHeader: g.header(a, BalancePath), Val: *uint256.NewInt(val())})
			case 12:
				g.ws.SetNonce(a, &VersionedWrite[uint64]{WriteHeader: g.header(a, NoncePath), Val: val()})
			case 13:
				h := g.header(a, StoragePath)
				h.Key = shapeKey(r.IntN(40))
				g.ws.SetStorage(a, h.Key, &VersionedWrite[uint256.Int]{WriteHeader: h, Val: *uint256.NewInt(val())})
			case 14:
				code := accounts.Code{}
				if r.IntN(2) == 0 {
					code.Bytes = []byte{0x60, byte(r.IntN(3))}
				}
				g.ws.SetCode(a, &VersionedWrite[accounts.Code]{WriteHeader: g.header(a, CodePath), Val: code})
			case 15:
				g.ws.SetSelfDestruct(a, &VersionedWrite[bool]{WriteHeader: g.header(a, SelfDestructPath), Val: r.IntN(2) == 0})
			case 16:
				switch r.IntN(5) {
				case 0:
					g.ws.SetCreateContract(a, &VersionedWrite[bool]{WriteHeader: g.header(a, CreateContractPath), Val: r.IntN(2) == 0})
				case 1:
					g.ws.SetAddress(a, &VersionedWrite[*accounts.Account]{WriteHeader: g.header(a, AddressPath), Val: &accounts.Account{}})
				case 2:
					g.ws.SetIncarnation(a, &VersionedWrite[uint64]{WriteHeader: g.header(a, IncarnationPath), Val: 1})
				case 3:
					g.ws.SetCodeHash(a, &VersionedWrite[accounts.CodeHash]{WriteHeader: g.header(a, CodeHashPath)})
				case 4:
					g.ws.SetCodeSize(a, &VersionedWrite[int]{WriteHeader: g.header(a, CodeSizePath), Val: 2})
				}
			}
		}
	})
}

// The hashes are pinned from the builder before its allocation and iteration
// changes; any drift in what Add records changes them.
func TestBlockAccessListBuilderShapesKeepHash(t *testing.T) {
	shapes := []struct {
		name string
		txs  []shapeTx
		hash string
	}{
		{"transfers", transfersShape(300), "0xaac2d12b99e2e1ba27f9247ccc630f93d848835c5d97d613474442e404642d10"},
		{"contracts", contractsShape(300), "0x7154a00d6492b60effb00eee927c2e5e6b748b9a1a394f74607dbdbe2341ad70"},
		{"shared slots", sharedSlotsShape(300), "0x131c771e66a307c3651ad6b6c0b913a2b8323284080301f24b8d7d3337ab8b98"},
		{"random 1", randomShape(1, 200), "0xb95181e7c17be43428f8c014c8a4b145d099378aaa11390fc864b3ed962970e2"},
		{"random 2", randomShape(2, 200), "0xc392b446c2b07e3f418028e1711b753af6ff462894a63057fda7d969a4aa091e"},
		{"random 3", randomShape(3, 50), "0x417c9b82e2ce9bc88698d3dd36bd13c27b105e33055bfae214eb3ca42d02a02f"},
	}
	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			bal := buildShape(s.txs).Build()
			require.NoError(t, bal.Validate())
			require.Equal(t, s.hash, bal.Hash().Hex())
			require.True(t, buildShape(s.txs).Matches(bal))
		})
	}
}
