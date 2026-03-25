package types

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestBlockAccessListValidateOrdering(t *testing.T) {
	var addrA, addrB common.Address
	addrA[19] = 0x02
	addrB[19] = 0x01

	list := BlockAccessList{
		{Address: accounts.InternAddress(addrA)},
		{Address: accounts.InternAddress(addrB)},
	}
	if err := list.Validate(); err == nil {
		t.Fatalf("expected ordering error, got nil")
	}
}

func TestAccountChangesEncodeRejectsUnsortedReads(t *testing.T) {
	var addr common.Address
	addr[19] = 0x01
	var slotA, slotB common.Hash
	slotA[31] = 0x02
	slotB[31] = 0x01

	ac := &AccountChanges{
		Address:      accounts.InternAddress(addr),
		StorageReads: []accounts.StorageKey{accounts.InternKey(slotA), accounts.InternKey(slotB)},
	}

	var buf bytes.Buffer
	if err := ac.EncodeRLP(&buf); err == nil || !strings.Contains(err.Error(), "reads must be strictly increasing") {
		t.Fatalf("expected storage read ordering error, got %v", err)
	}
}

func TestDecodeBalanceChangesRejectsOutOfOrderIndices(t *testing.T) {
	payload, err := rlp.EncodeToBytes([][]any{
		{uint64(2), []byte{0x01}},
		{uint64(1), []byte{0x01}},
	})
	if err != nil {
		t.Fatalf("failed to build payload: %v", err)
	}

	stream := rlp.NewStream(bytes.NewReader(payload), uint64(len(payload)))
	if _, err := decodeBalanceChanges(stream); err == nil || !strings.Contains(err.Error(), "indices") {
		t.Fatalf("expected index ordering error, got %v", err)
	}
}

func TestBlockAccessListRLPEncoding(t *testing.T) {
	bal := BlockAccessList{
		{
			Address: accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000000000aa")),
			StorageChanges: []*SlotChanges{
				{
					Slot: accounts.InternKey(common.HexToHash("0x01")),
					Changes: []*StorageChange{
						{Index: 1, Value: *uint256.NewInt(2)},
						{Index: 5, Value: *uint256.NewInt(3)},
					},
				},
			},
			StorageReads: []accounts.StorageKey{
				accounts.InternKey(common.HexToHash("0x02")),
			},
			BalanceChanges: []*BalanceChange{
				{Index: 1, Value: *uint256.NewInt(4)},
			},
			NonceChanges: []*NonceChange{
				{Index: 9, Value: 7},
			},
			CodeChanges: []*CodeChange{
				{Index: 2, Bytecode: []byte{0xbe, 0xef}},
			},
		},
	}

	encoded, err := rlp.EncodeToBytes(bal)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Fixed-size encoding: slot keys and storage values are 32-byte strings,
	// storage reads are 32-byte strings, balances are 16-byte strings.
	expected := common.FromHex("0xf0ef9400000000000000000000000000000000000000aac9c801c6c20102c20503c102c3c20104c3c20907c5c40282beef")
	if !bytes.Equal(encoded, expected) {
		t.Fatalf("unexpected encoding\nhave: %x\nwant: %x", encoded, expected)
	}

	var decoded BlockAccessList
	if err := rlp.DecodeBytes(encoded, &decoded); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if !reflect.DeepEqual(decoded, bal) {
		t.Fatalf("decoded BAL mismatch\nhave: %#v\nwant: %#v", decoded, bal)
	}
}

func TestBlockAccessListValidateMaxItems(t *testing.T) {
	makeBAL := func(numAccounts, slotsPerAccount int) BlockAccessList {
		bal := make(BlockAccessList, numAccounts)
		for i := range bal {
			var addr common.Address
			addr[18] = byte(i >> 8)
			addr[19] = byte(i)
			reads := make([]accounts.StorageKey, slotsPerAccount)
			for j := range reads {
				var h common.Hash
				h[30] = byte(j >> 8)
				h[31] = byte(j)
				reads[j] = accounts.InternKey(h)
			}
			bal[i] = &AccountChanges{
				Address:      accounts.InternAddress(addr),
				StorageReads: reads,
			}
		}
		return bal
	}

	// 10 accounts + 5 slots each = 60 items; gasLimit 120000 → max 60 items → exactly at limit
	bal := makeBAL(10, 5)
	if err := bal.ValidateMaxItems(120_000); err != nil {
		t.Fatalf("expected valid at limit, got: %v", err)
	}

	// Same BAL with lower gas limit → over limit
	if err := bal.ValidateMaxItems(119_999); err == nil {
		t.Fatal("expected error for over-limit BAL")
	}

	// Empty BAL always valid
	if err := (BlockAccessList{}).ValidateMaxItems(0); err != nil {
		t.Fatalf("expected empty BAL valid, got: %v", err)
	}
}

func TestBlockAccessListSlotUniqueness(t *testing.T) {
	var addr common.Address
	addr[19] = 0x01
	slot := common.HexToHash("0x01")

	ac := &AccountChanges{
		Address: accounts.InternAddress(addr),
		StorageChanges: []*SlotChanges{
			{
				Slot:    accounts.InternKey(slot),
				Changes: []*StorageChange{{Index: 0, Value: *uint256.NewInt(1)}},
			},
		},
		StorageReads: []accounts.StorageKey{accounts.InternKey(slot)},
	}
	bal := BlockAccessList{ac}
	if err := bal.Validate(); err == nil {
		t.Fatal("expected error for slot in both changes and reads")
	}
}

func TestBlockAccessListHashEmpty(t *testing.T) {
	var bal BlockAccessList
	if h := bal.Hash(); h != common.HexToHash("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347") {
		t.Fatalf("unexpected empty BAL hash: %s", h)
	}

	if err := bal.Validate(); err != nil {
		t.Fatalf("empty BAL should be valid: %v", err)
	}
}
