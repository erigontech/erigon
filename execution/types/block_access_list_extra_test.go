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

package types

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
)

func key(h string) accounts.StorageKey { return accounts.InternKey(common.HexToHash(h)) }

func sampleBAL() BlockAccessList {
	return BlockAccessList{&AccountChanges{
		Address:        accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa")),
		StorageChanges: []*SlotChanges{{Slot: key("0x01"), Changes: []*StorageChange{{Index: 0, Value: *uint256.NewInt(100)}}}},
		StorageReads:   []accounts.StorageKey{key("0x02")},
		BalanceChanges: []*BalanceChange{{Index: 0, Value: *uint256.NewInt(5)}},
		NonceChanges:   []*NonceChange{{Index: 0, Value: 7}},
		CodeChanges:    []*CodeChange{{Index: 0, Bytecode: []byte{1, 2, 3}}},
	}}
}

func TestChange_GetIndex(t *testing.T) {
	t.Parallel()
	require.Equal(t, uint32(3), (&StorageChange{Index: 3}).GetIndex())
	require.Equal(t, uint32(4), (&BalanceChange{Index: 4}).GetIndex())
	require.Equal(t, uint32(5), (&NonceChange{Index: 5}).GetIndex())
	require.Equal(t, uint32(6), (&CodeChange{Index: 6}).GetIndex())
}

func TestAccountChanges_Normalize(t *testing.T) {
	t.Parallel()
	ac := &AccountChanges{
		StorageChanges: []*SlotChanges{{Slot: key("0x03")}, {Slot: key("0x01")}, {Slot: key("0x02")}},
		StorageReads:   []accounts.StorageKey{key("0x05"), key("0x05"), key("0x04")},
		BalanceChanges: []*BalanceChange{{Index: 2, Value: *uint256.NewInt(1)}, {Index: 0, Value: *uint256.NewInt(1)}, {Index: 0, Value: *uint256.NewInt(99)}},
		NonceChanges:   []*NonceChange{{Index: 1}, {Index: 0}},
		CodeChanges:    []*CodeChange{{Index: 1}, {Index: 0}},
	}
	ac.Normalize()

	require.Equal(t, key("0x01"), ac.StorageChanges[0].Slot)
	require.Equal(t, key("0x03"), ac.StorageChanges[2].Slot)

	require.Equal(t, []accounts.StorageKey{key("0x04"), key("0x05")}, ac.StorageReads)

	require.Len(t, ac.BalanceChanges, 2)
	require.Equal(t, uint32(0), ac.BalanceChanges[0].Index)
	require.Equal(t, uint32(2), ac.BalanceChanges[1].Index)
	// dedupByIndex keeps the last entry for a duplicated index.
	require.Equal(t, uint64(99), ac.BalanceChanges[0].Value.Uint64())

	require.Equal(t, uint32(0), ac.NonceChanges[0].Index)
	require.Equal(t, uint32(0), ac.CodeChanges[0].Index)
}

func TestStorageChange_DecodeRLP_Errors(t *testing.T) {
	t.Parallel()

	var overIndex bytes.Buffer
	require.NoError(t, rlp.Encode(&overIndex, []interface{}{uint64(1) << 32, []byte{5}}))
	require.ErrorContains(t, (&StorageChange{}).DecodeRLP(rlp.NewStream(&overIndex, 0)), "exceeds uint32")

	var bigVal bytes.Buffer
	require.NoError(t, rlp.Encode(&bigVal, []interface{}{uint64(0), make([]byte, 33)}))
	require.ErrorContains(t, (&StorageChange{}).DecodeRLP(rlp.NewStream(&bigVal, 0)), "too large")
}

func TestBalanceChange_DecodeRLP_Errors(t *testing.T) {
	t.Parallel()

	var overIndex bytes.Buffer
	require.NoError(t, rlp.Encode(&overIndex, []interface{}{uint64(1) << 32, []byte{5}}))
	require.ErrorContains(t, (&BalanceChange{}).DecodeRLP(rlp.NewStream(&overIndex, 0)), "exceeds uint32")

	var bigVal bytes.Buffer
	require.NoError(t, rlp.Encode(&bigVal, []interface{}{uint64(0), make([]byte, 33)}))
	require.ErrorContains(t, (&BalanceChange{}).DecodeRLP(rlp.NewStream(&bigVal, 0)), "too large")
}

func TestNonceAndCodeChange_DecodeRLP_OverIndex(t *testing.T) {
	t.Parallel()

	var nc bytes.Buffer
	require.NoError(t, rlp.Encode(&nc, []interface{}{uint64(1) << 32, uint64(1)}))
	require.ErrorContains(t, (&NonceChange{}).DecodeRLP(rlp.NewStream(&nc, 0)), "exceeds uint32")

	var cc bytes.Buffer
	require.NoError(t, rlp.Encode(&cc, []interface{}{uint64(1) << 32, []byte{1}}))
	require.ErrorContains(t, (&CodeChange{}).DecodeRLP(rlp.NewStream(&cc, 0)), "exceeds uint32")
}

func TestDecodeMinimalHash_TooLarge(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	require.NoError(t, rlp.Encode(&buf, make([]byte, 33)))
	_, err := decodeMinimalHash(rlp.NewStream(&buf, 0))
	require.ErrorContains(t, err, "too large")
}

func TestBlockAccessList_HashAndDebug(t *testing.T) {
	t.Parallel()
	require.Equal(t, empty.BlockAccessListHash, BlockAccessList(nil).Hash())
	require.NotEqual(t, empty.BlockAccessListHash, sampleBAL().Hash())
	require.Contains(t, sampleBAL().DebugString(), "accounts=1")
}

func TestBlockAccessList_ExecutionProtoRoundTrip(t *testing.T) {
	t.Parallel()
	bal := sampleBAL()
	want, err := EncodeBlockAccessListBytes(bal)
	require.NoError(t, err)

	ep := ConvertBlockAccessListToExecutionProto(bal)
	require.Len(t, ep, 1)

	back, err := ConvertExecutionProtoToBlockAccessList(ep)
	require.NoError(t, err)
	got, err := EncodeBlockAccessListBytes(back)
	require.NoError(t, err)
	require.Equal(t, want, got)

	require.NotNil(t, ConvertBlockAccessListFromExecutionProto(ep))
}

func TestConvertExecutionProtoToBlockAccessList_Errors(t *testing.T) {
	t.Parallel()
	_, err := ConvertExecutionProtoToBlockAccessList([]*executionproto.BlockAccessListAccount{nil})
	require.Error(t, err)

	_, err = ConvertExecutionProtoToBlockAccessList([]*executionproto.BlockAccessListAccount{{Address: nil}})
	require.ErrorContains(t, err, "address")
}

func TestBlockAccessList_TypesProtoRoundTrip(t *testing.T) {
	t.Parallel()
	bal := sampleBAL()
	want, err := EncodeBlockAccessListBytes(bal)
	require.NoError(t, err)

	tp := ConvertBlockAccessListToTypesProto(bal)
	require.Len(t, tp, 1)

	rlpBytes := ConvertBlockAccessListFromTypesProto(tp)
	require.NotEmpty(t, rlpBytes)

	back, err := DecodeBlockAccessListBytes(rlpBytes)
	require.NoError(t, err)
	got, err := EncodeBlockAccessListBytes(back)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
