package types

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
)

func TestBlockAccessListValidateOrdering(t *testing.T) {
	var addrA, addrB common.Address
	addrA[19] = 0x02
	addrB[19] = 0x01

	list := BlockAccessList{
		{Address: addrA},
		{Address: addrB},
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
		Address:      addr,
		StorageReads: []common.Hash{slotA, slotB},
	}

	var buf bytes.Buffer
	if err := ac.EncodeRLP(&buf); err == nil || !strings.Contains(err.Error(), "reads must be strictly increasing") {
		t.Fatalf("expected storage read ordering error, got %v", err)
	}
}

func TestDecodeBalanceChangesRejectsOutOfOrderIndices(t *testing.T) {
	payload, err := rlp.EncodeToBytes([][]interface{}{
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
			Address: common.HexToAddress("0x00000000000000000000000000000000000000aa"),
			StorageChanges: []*SlotChanges{
				{
					Slot: common.HexToHash("0x01"),
					Changes: []*StorageChange{
						{Index: 1, Value: common.Hash(uint256.NewInt(2).Bytes32())},
						{Index: 5, Value: common.Hash(uint256.NewInt(3).Bytes32())},
					},
				},
			},
			StorageReads: []common.Hash{
				common.HexToHash("0x02"),
			},
			BalanceChanges: []*BalanceChange{
				{Index: 1, Value: *uint256.NewInt(4)},
			},
			NonceChanges: []*NonceChange{
				{Index: 9, Value: 7},
			},
			CodeChanges: []*CodeChange{
				{Index: 2, Data: []byte{0xbe, 0xef}},
			},
		},
	}

	encoded, err := rlp.EncodeToBytes(bal)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	expected := common.FromHex("0xf8b4f8b29400000000000000000000000000000000000000aaf86bf869a00000000000000000000000000000000000000000000000000000000000000001f846e201a00000000000000000000000000000000000000000000000000000000000000002e205a00000000000000000000000000000000000000000000000000000000000000003e1a00000000000000000000000000000000000000000000000000000000000000002c3c20104c3c20907c5c40282beef")
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

func TestBlockAccessListHashEmpty(t *testing.T) {
	var bal BlockAccessList
	if h := bal.Hash(); h != common.HexToHash("0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347") {
		t.Fatalf("unexpected empty BAL hash: %s", h)
	}

	if err := bal.Validate(); err != nil {
		t.Fatalf("empty BAL should be valid: %v", err)
	}
}
