package commands

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/decodedstate"
)

func TestDecodedStateCheckLooksLikeAddressHash(t *testing.T) {
	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	require.True(t, looksLikeAddressHash(common.BytesToHash(common.LeftPadBytes(addr[:], 32))))
	require.False(t, looksLikeAddressHash(common.HexToHash("0x0100000000000000000000001111111111111111111111111111111111111111")))
}

func TestDecodedStateCheckCollectBalanceCandidates(t *testing.T) {
	slot := common.HexToHash("0x01")
	addrA := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	addrB := common.HexToAddress("0x00000000000000000000000000000000000000bb")
	addrC := common.HexToAddress("0x00000000000000000000000000000000000000cc")

	storage := map[common.Hash][]decodedstate.DecodedEntry{
		slot: {
			{
				MappingSlot: slot,
				EntryType:   decodedstate.MappingEntry,
				Keys:        []common.Hash{common.BytesToHash(common.LeftPadBytes(addrA[:], 32))},
				Value:       common.HexToHash("0x10"),
			},
			{
				MappingSlot: slot,
				EntryType:   decodedstate.MappingEntry,
				Keys:        []common.Hash{common.BytesToHash(common.LeftPadBytes(addrB[:], 32))},
				Value:       common.HexToHash("0x20"),
			},
			{
				MappingSlot: slot,
				EntryType:   decodedstate.MappingEntry,
				Keys:        []common.Hash{common.BytesToHash(common.LeftPadBytes(addrC[:], 32))},
				Value:       common.HexToHash("0x21"),
			},
			{
				MappingSlot: slot,
				EntryType:   decodedstate.MappingEntry,
				Keys:        []common.Hash{common.HexToHash("0x0100000000000000000000000000000000000000000000000000000000000099")},
				Value:       common.HexToHash("0x30"),
			},
		},
	}

	candidates := collectBalanceCandidates(storage, 3)
	require.Len(t, candidates, 1)
	require.Equal(t, slot, candidates[0].Slot)
	require.Len(t, candidates[0].Samples, 3)
	require.Equal(t, addrA, candidates[0].Samples[0].Address)
	require.Equal(t, addrB, candidates[0].Samples[1].Address)
	require.Equal(t, addrC, candidates[0].Samples[2].Address)
}

func TestDecodedStateCheckCollectAllowanceCandidates(t *testing.T) {
	slot := common.HexToHash("0x02")
	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	spender := common.HexToAddress("0x00000000000000000000000000000000000000bb")

	storage := map[common.Hash][]decodedstate.DecodedEntry{
		slot: {
			{
				MappingSlot: slot,
				EntryType:   decodedstate.NestedMappingEntry,
				Keys: []common.Hash{
					common.BytesToHash(common.LeftPadBytes(owner[:], 32)),
					common.BytesToHash(common.LeftPadBytes(spender[:], 32)),
				},
				Value: common.HexToHash("0x40"),
			},
			{
				MappingSlot: slot,
				EntryType:   decodedstate.NestedMappingEntry,
				Keys: []common.Hash{
					common.BytesToHash(common.LeftPadBytes(owner[:], 32)),
					common.HexToHash("0x0100000000000000000000000000000000000000000000000000000000000001"),
				},
				Value: common.HexToHash("0x50"),
			},
		},
	}

	candidates := collectAllowanceCandidates(storage, 3)
	require.Len(t, candidates, 0)

	storage[slot] = append(storage[slot], decodedstate.DecodedEntry{
		MappingSlot: slot,
		EntryType:   decodedstate.NestedMappingEntry,
		Keys: []common.Hash{
			common.BytesToHash(common.LeftPadBytes(common.FromHex("0x00000000000000000000000000000000000000cc"), 32)),
			common.BytesToHash(common.LeftPadBytes(common.FromHex("0x00000000000000000000000000000000000000dd"), 32)),
		},
		Value: common.HexToHash("0x60"),
	})

	candidates = collectAllowanceCandidates(storage, 3)
	require.Len(t, candidates, 1)
	require.Equal(t, slot, candidates[0].Slot)
	require.Len(t, candidates[0].Samples, 2)
	require.Equal(t, owner, candidates[0].Samples[0].Owner)
	require.Equal(t, spender, candidates[0].Samples[0].Spender)
}

func TestDecodedStateCheckMappingSlotsDiffer(t *testing.T) {
	slot := common.HexToHash("0x33")
	owner := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	spender := common.HexToAddress("0x00000000000000000000000000000000000000bb")

	balanceSlot := mappingSlotForAddress(owner, slot)
	allowanceSlot := nestedMappingSlotForAddresses(owner, spender, slot)

	require.NotEqual(t, common.Hash{}, balanceSlot)
	require.NotEqual(t, common.Hash{}, allowanceSlot)
	require.NotEqual(t, balanceSlot, allowanceSlot)
}
