// Copyright 2024 The Erigon Authors
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

package logger

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

var (
	addr = common.BytesToAddress([]byte{0x01, 0x71})

	slot1 = common.BytesToHash([]byte{0x01})
	slot2 = common.BytesToHash([]byte{0x02})
	slot3 = common.BytesToHash([]byte{0x03})
	slot4 = common.BytesToHash([]byte{0x04})

	ordered = types.AccessList{{
		Address: addr,
		StorageKeys: []common.Hash{
			slot1,
			slot2,
			slot3,
			slot4,
		},
	}}
)

func TestTracer_AccessList_Order(t *testing.T) {
	al := newAccessList()
	al.addAddress(addr)
	al.addSlot(addr, slot1)
	al.addSlot(addr, slot4)
	al.addSlot(addr, slot3)
	al.addSlot(addr, slot2)
	require.NotEqual(t, ordered, al.accessList())
	require.Equal(t, ordered, al.accessListSorted())
	require.True(t, al.Equal(al)) //nolint:gocritic
}

// TestTracer_AccessList_EmptyStorageKeys verifies that addresses with no storage
// accesses produce nil storageKeys (serializes as JSON null), matching Geth behaviour.
// See: https://github.com/ethereum/go-ethereum/issues/23233
func TestTracer_AccessList_EmptyStorageKeys(t *testing.T) {
	al := newAccessList()
	al.addAddress(addr)

	acl := al.accessList()
	require.Len(t, acl, 1)
	require.Nil(t, acl[0].StorageKeys)

	aclSorted := al.accessListSorted()
	require.Len(t, aclSorted, 1)
	require.Nil(t, aclSorted[0].StorageKeys)
}
