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
