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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func addrPathsMap(t *testing.T, s []AddrPaths) map[accounts.Address]uint16 {
	t.Helper()
	m := make(map[accounts.Address]uint16, len(s))
	for _, e := range s {
		_, dup := m[e.Addr]
		require.False(t, dup, "address listed twice")
		m[e.Addr] = e.Paths
	}
	return m
}

func TestAddrPathsReadSet(t *testing.T) {
	a := accounts.InternAddress(common.HexToAddress("0xaa"))
	b := accounts.InternAddress(common.HexToAddress("0xbb"))
	hdr := ReadHeader{Source: MapRead, Version: Version{TxIndex: 0}}
	rs := ReadSet{}
	rs.SetHeader(a, BalancePath, accounts.StorageKey{}, hdr)
	rs.SetHeader(a, NoncePath, accounts.StorageKey{}, hdr)
	for i := byte(1); i <= 5; i++ {
		rs.SetHeader(b, StoragePath, accounts.InternKey(common.Hash{i}), hdr)
	}
	got := addrPathsMap(t, rs.AddrPaths())
	require.Equal(t, map[accounts.Address]uint16{
		a: 1<<BalancePath | 1<<NoncePath,
		b: 1 << StoragePath,
	}, got)
	require.Empty(t, ReadSet{}.AddrPaths())
}

func TestAddrPathsWriteSet(t *testing.T) {
	a := accounts.InternAddress(common.HexToAddress("0xaa"))
	b := accounts.InternAddress(common.HexToAddress("0xbb"))
	c := accounts.InternAddress(common.HexToAddress("0xcc"))
	ver := Version{TxIndex: 1}
	hw := func(addr accounts.Address, p AccountPath) WriteHeader {
		return WriteHeader{Address: addr, Path: p, Version: ver}
	}
	ws := &WriteSet{}
	ws.SetBalance(a, &VersionedWrite[uint256.Int]{WriteHeader: hw(a, BalancePath)})
	ws.SetNonce(a, &VersionedWrite[uint64]{WriteHeader: hw(a, NoncePath)})
	for i := byte(1); i <= 3; i++ {
		k := accounts.InternKey(common.Hash{i})
		h := hw(b, StoragePath)
		h.Key = k
		ws.SetStorage(b, k, &VersionedWrite[uint256.Int]{WriteHeader: h})
	}
	ws.SetBalance(c, &VersionedWrite[uint256.Int]{WriteHeader: hw(c, BalancePath)})
	ws.SetSelfDestruct(c, &VersionedWrite[bool]{WriteHeader: hw(c, SelfDestructPath)})
	got := addrPathsMap(t, ws.AddrPaths())
	require.Equal(t, map[accounts.Address]uint16{
		a: 1<<BalancePath | 1<<NoncePath,
		b: 1 << StoragePath,
		c: ^uint16(0),
	}, got)
	var nilWS *WriteSet
	require.Nil(t, nilWS.AddrPaths())
}
