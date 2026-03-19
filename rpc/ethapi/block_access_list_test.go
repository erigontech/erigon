package ethapi

import (
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestMarshalBlockAccessList(t *testing.T) {
	bal := types.BlockAccessList{
		{
			Address: accounts.InternAddress(common.HexToAddress("0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b")),
			StorageChanges: []*types.SlotChanges{
				{
					Slot: accounts.InternKey(common.Hash{}),
					Changes: []*types.StorageChange{
						{Index: 0, Value: *uint256.NewInt(0)},
						{Index: 1, Value: *uint256.NewInt(256)},
					},
				},
			},
			StorageReads: nil,
			BalanceChanges: []*types.BalanceChange{
				{Index: 0, Value: *uint256.MustFromHex("0x56bc75e2d63100000")},
				{Index: 1, Value: *uint256.MustFromHex("0x56bc75e2d63000000")},
			},
			NonceChanges: []*types.NonceChange{
				{Index: 0, Value: 0},
				{Index: 1, Value: 1},
			},
			CodeChanges: nil,
		},
	}

	result := MarshalBlockAccessList(bal)
	data, err := json.Marshal(result)
	require.NoError(t, err)

	var parsed []map[string]any
	require.NoError(t, json.Unmarshal(data, &parsed))
	require.Len(t, parsed, 1)

	entry := parsed[0]
	require.Equal(t, "0xa94f5374fce5edbc8e2a8697c15331677e6ebf0b", entry["address"])

	// Verify storageChanges
	sc := entry["storageChanges"].([]any)
	require.Len(t, sc, 1)
	slot := sc[0].(map[string]any)
	require.Equal(t, "0x0000000000000000000000000000000000000000000000000000000000000000", slot["key"])
	changes := slot["changes"].([]any)
	require.Len(t, changes, 2)
	ch0 := changes[0].(map[string]any)
	require.Equal(t, "0x0", ch0["index"])
	require.Equal(t, "0x0000000000000000000000000000000000000000000000000000000000000000", ch0["value"])
	ch1 := changes[1].(map[string]any)
	require.Equal(t, "0x1", ch1["index"])
	require.Equal(t, "0x0000000000000000000000000000000000000000000000000000000000000100", ch1["value"])

	// Verify empty arrays are present (not null)
	require.NotNil(t, entry["storageReads"])
	sr := entry["storageReads"].([]any)
	require.Len(t, sr, 0)

	require.NotNil(t, entry["codeChanges"])
	cc := entry["codeChanges"].([]any)
	require.Len(t, cc, 0)

	// Verify balanceChanges
	bc := entry["balanceChanges"].([]any)
	require.Len(t, bc, 2)
	bc0 := bc[0].(map[string]any)
	require.Equal(t, "0x0", bc0["index"])
	require.Equal(t, "0x56bc75e2d63100000", bc0["value"])
	bc1 := bc[1].(map[string]any)
	require.Equal(t, "0x1", bc1["index"])
	require.Equal(t, "0x56bc75e2d63000000", bc1["value"])

	// Verify nonceChanges
	nc := entry["nonceChanges"].([]any)
	require.Len(t, nc, 2)
	nc0 := nc[0].(map[string]any)
	require.Equal(t, "0x0", nc0["index"])
	require.Equal(t, "0x0", nc0["value"])
	nc1 := nc[1].(map[string]any)
	require.Equal(t, "0x1", nc1["index"])
	require.Equal(t, "0x1", nc1["value"])
}

func TestMarshalBlockAccessListEmpty(t *testing.T) {
	result := MarshalBlockAccessList(types.BlockAccessList{})
	data, err := json.Marshal(result)
	require.NoError(t, err)
	require.Equal(t, "[]", string(data))
}

func TestMarshalBlockAccessListWithCode(t *testing.T) {
	bal := types.BlockAccessList{
		{
			Address:        accounts.InternAddress(common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")),
			StorageChanges: nil,
			StorageReads:   nil,
			BalanceChanges: nil,
			NonceChanges:   nil,
			CodeChanges: []*types.CodeChange{
				{Index: 0, Bytecode: common.FromHex("0x6080604052")},
			},
		},
	}

	result := MarshalBlockAccessList(bal)
	data, err := json.Marshal(result)
	require.NoError(t, err)

	var parsed []map[string]any
	require.NoError(t, json.Unmarshal(data, &parsed))
	require.Len(t, parsed, 1)

	cc := parsed[0]["codeChanges"].([]any)
	require.Len(t, cc, 1)
	cc0 := cc[0].(map[string]any)
	require.Equal(t, "0x0", cc0["index"])
	require.Equal(t, "0x6080604052", cc0["code"])
}
