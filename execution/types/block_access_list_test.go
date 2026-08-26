package types

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestBlockAccessListCopy(t *testing.T) {
	bal := BlockAccessList{{
		Address: accounts.InternAddress(common.Address{1}),
		StorageChanges: []*SlotChanges{{
			Slot:    accounts.InternKey(common.Hash{2}),
			Changes: []*StorageChange{{Index: 1, Value: *uint256.NewInt(3)}},
		}},
		StorageReads:   []accounts.StorageKey{accounts.InternKey(common.Hash{4})},
		BalanceChanges: []*BalanceChange{{Index: 2, Value: *uint256.NewInt(5)}},
		NonceChanges:   []*NonceChange{{Index: 3, Value: 6}},
		CodeChanges:    []*CodeChange{{Index: 4, Bytecode: []byte{7}}},
	}}

	cpy := bal.Copy()
	if !reflect.DeepEqual(bal, cpy) {
		t.Fatalf("copy differs: got %v, want %v", cpy, bal)
	}
	if bal[0] == cpy[0] || bal[0].StorageChanges[0] == cpy[0].StorageChanges[0] ||
		bal[0].StorageChanges[0].Changes[0] == cpy[0].StorageChanges[0].Changes[0] ||
		bal[0].BalanceChanges[0] == cpy[0].BalanceChanges[0] ||
		bal[0].NonceChanges[0] == cpy[0].NonceChanges[0] ||
		bal[0].CodeChanges[0] == cpy[0].CodeChanges[0] {
		t.Fatal("copy shares mutable entries")
	}

	bal[0].StorageChanges[0].Changes[0].Index = 10
	bal[0].StorageReads[0] = accounts.InternKey(common.Hash{11})
	bal[0].BalanceChanges[0].Index = 12
	bal[0].NonceChanges[0].Index = 13
	bal[0].CodeChanges[0].Bytecode[0] = 14
	if reflect.DeepEqual(bal, cpy) {
		t.Fatal("mutating the original changed the copy")
	}
}

func TestBlockAccessListSidecarPreservesRLP(t *testing.T) {
	bal := BlockAccessList{{Address: accounts.InternAddress(common.Address{1})}}
	raw, err := EncodeBlockAccessListBytes(bal)
	if err != nil {
		t.Fatalf("encode BAL: %v", err)
	}
	wantRaw := bytes.Clone(raw)

	sidecar, err := DecodeBlockAccessListSidecar(raw)
	if err != nil {
		t.Fatalf("decode BAL sidecar: %v", err)
	}
	raw[0] = 0
	gotRaw, err := sidecar.Bytes()
	if err != nil {
		t.Fatalf("sidecar bytes: %v", err)
	}
	if !bytes.Equal(gotRaw, wantRaw) {
		t.Fatalf("sidecar RLP changed: got %x, want %x", gotRaw, wantRaw)
	}
	if !reflect.DeepEqual(sidecar.BlockAccessList(), bal) {
		t.Fatalf("sidecar BAL differs: got %v, want %v", sidecar.BlockAccessList(), bal)
	}
}

func TestDecodeBlockAccessListSidecarOwnedRetainsRLP(t *testing.T) {
	raw, err := EncodeBlockAccessListBytes(BlockAccessList{{
		Address: accounts.InternAddress(common.Address{1}),
	}})
	if err != nil {
		t.Fatalf("encode BAL: %v", err)
	}
	sidecar, err := DecodeBlockAccessListSidecarOwned(raw)
	if err != nil {
		t.Fatalf("decode owned BAL sidecar: %v", err)
	}
	gotRaw, err := sidecar.Bytes()
	if err != nil {
		t.Fatalf("sidecar bytes: %v", err)
	}
	if &gotRaw[0] != &raw[0] {
		t.Fatal("owned BAL sidecar copied its RLP bytes")
	}
}

func TestBlockAccessListSidecarMemoizesRLP(t *testing.T) {
	sidecar := NewBlockAccessListSidecar(BlockAccessList{{
		Address: accounts.InternAddress(common.Address{1}),
	}})
	first, err := sidecar.Bytes()
	if err != nil {
		t.Fatalf("encode sidecar: %v", err)
	}
	second, err := sidecar.Bytes()
	if err != nil {
		t.Fatalf("encode sidecar again: %v", err)
	}
	if &first[0] != &second[0] {
		t.Fatal("sidecar did not memoize its RLP bytes")
	}
}

func TestBlockAccessListSidecarMemoizesValidation(t *testing.T) {
	sidecar := NewBlockAccessListSidecar(BlockAccessList{{
		Address: accounts.InternAddress(common.Address{1}),
	}})
	if sidecar.validated.Load() {
		t.Fatal("new sidecar is already validated")
	}
	if err := sidecar.ValidateForBlock(BalItemCost); err != nil {
		t.Fatalf("validate sidecar: %v", err)
	}
	if !sidecar.validated.Load() {
		t.Fatal("successful validation was not memoized")
	}
	if got := sidecar.validatedGasLimit.Load(); got != BalItemCost {
		t.Fatalf("validated gas limit = %d, want %d", got, BalItemCost)
	}
	if err := sidecar.ValidateForBlock(BalItemCost - 1); !errors.Is(err, ErrInvalidBlockAccessList) {
		t.Fatalf("validation with a different gas limit returned %v, want ErrInvalidBlockAccessList", err)
	}
}

func TestBlockAccessListSidecarMemoizesHash(t *testing.T) {
	raw, err := EncodeBlockAccessListBytes(BlockAccessList{{
		Address: accounts.InternAddress(common.Address{1}),
	}})
	if err != nil {
		t.Fatalf("encode BAL: %v", err)
	}
	sidecar, err := DecodeBlockAccessListSidecar(raw)
	if err != nil {
		t.Fatalf("decode BAL sidecar: %v", err)
	}
	if sidecar.hash.Load() != nil {
		t.Fatal("new sidecar already has a cached hash")
	}

	got, err := sidecar.Hash()
	if err != nil {
		t.Fatalf("hash sidecar: %v", err)
	}
	if want := crypto.Keccak256Hash(raw); got != want {
		t.Fatalf("sidecar hash = %s, want %s", got, want)
	}
	cached := sidecar.hash.Load()
	if cached == nil {
		t.Fatal("sidecar hash was not cached")
	}
	gotRaw, err := sidecar.Bytes()
	if err != nil {
		t.Fatalf("get sidecar bytes: %v", err)
	}
	if !bytes.Equal(gotRaw, raw) {
		t.Fatalf("sidecar RLP changed: got %x, want %x", gotRaw, raw)
	}
	gotHash, err := sidecar.Hash()
	if err != nil {
		t.Fatalf("get sidecar hash: %v", err)
	}
	if gotHash != got {
		t.Fatalf("sidecar cached hash = %s, want %s", gotHash, got)
	}
	if sidecar.hash.Load() != cached {
		t.Fatal("sidecar replaced its cached hash")
	}
}

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

func TestBlockAccessListCodecLeavesSemanticValidationToValidateForBlock(t *testing.T) {
	tests := []struct {
		name      string
		bal       BlockAccessList
		wantError string
	}{
		{
			name: "account address order",
			bal: BlockAccessList{
				{Address: accounts.InternAddress(common.Address{2})},
				{Address: accounts.InternAddress(common.Address{1})},
			},
			wantError: "account addresses must be strictly increasing",
		},
		{
			name: "storage read order",
			bal: BlockAccessList{{
				Address: accounts.InternAddress(common.Address{1}),
				StorageReads: []accounts.StorageKey{
					accounts.InternKey(common.Hash{2}),
					accounts.InternKey(common.Hash{1}),
				},
			}},
			wantError: "storage reads must be strictly increasing",
		},
		{
			name: "balance change order",
			bal: BlockAccessList{{
				Address: accounts.InternAddress(common.Address{1}),
				BalanceChanges: []*BalanceChange{
					{Index: 2, Value: *uint256.NewInt(1)},
					{Index: 1, Value: *uint256.NewInt(1)},
				},
			}},
			wantError: "balance_changes: indices must be strictly increasing",
		},
		{
			name: "nonce change order",
			bal: BlockAccessList{{
				Address: accounts.InternAddress(common.Address{1}),
				NonceChanges: []*NonceChange{
					{Index: 2, Value: 1},
					{Index: 1, Value: 2},
				},
			}},
			wantError: "nonce_changes: indices must be strictly increasing",
		},
		{
			name: "code change order",
			bal: BlockAccessList{{
				Address: accounts.InternAddress(common.Address{1}),
				CodeChanges: []*CodeChange{
					{Index: 2, Bytecode: []byte{1}},
					{Index: 1, Bytecode: []byte{2}},
				},
			}},
			wantError: "code_changes: indices must be strictly increasing",
		},
		{
			name: "empty slot changes",
			bal: BlockAccessList{{
				Address: accounts.InternAddress(common.Address{1}),
				StorageChanges: []*SlotChanges{{
					Slot: accounts.InternKey(common.Hash{1}),
				}},
			}},
			wantError: "empty slot changes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeBlockAccessListBytes(tt.bal)
			if err != nil {
				t.Fatalf("encode semantically invalid BAL: %v", err)
			}
			decoded, err := DecodeBlockAccessListBytes(encoded)
			if err != nil {
				t.Fatalf("decode semantically invalid BAL: %v", err)
			}
			if !reflect.DeepEqual(decoded, tt.bal) {
				t.Fatalf("round trip differs: got %v, want %v", decoded, tt.bal)
			}
			err = decoded.ValidateForBlock(^uint64(0))
			if !errors.Is(err, ErrInvalidBlockAccessList) {
				t.Fatalf("ValidateForBlock() error = %v, want ErrInvalidBlockAccessList", err)
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("ValidateForBlock() error = %v, want it to contain %q", err, tt.wantError)
			}
		})
	}
}

func TestEncodeBlockAccessListRejectsUnrepresentableNil(t *testing.T) {
	if _, err := EncodeBlockAccessListBytes(BlockAccessList{nil}); err == nil {
		t.Fatal("expected nil account encoding error")
	}
}

func TestEncodeBlockAccessListRejectsNestedNil(t *testing.T) {
	tests := []struct {
		name string
		bal  BlockAccessList
	}{
		{
			name: "slot changes",
			bal: BlockAccessList{{
				Address:        accounts.InternAddress(common.Address{1}),
				StorageChanges: []*SlotChanges{nil},
			}},
		},
		{
			name: "storage change",
			bal: BlockAccessList{{
				Address: accounts.InternAddress(common.Address{1}),
				StorageChanges: []*SlotChanges{{
					Slot:    accounts.InternKey(common.Hash{1}),
					Changes: []*StorageChange{nil},
				}},
			}},
		},
		{
			name: "balance change",
			bal: BlockAccessList{{
				Address:        accounts.InternAddress(common.Address{1}),
				BalanceChanges: []*BalanceChange{nil},
			}},
		},
		{
			name: "nonce change",
			bal: BlockAccessList{{
				Address:      accounts.InternAddress(common.Address{1}),
				NonceChanges: []*NonceChange{nil},
			}},
		},
		{
			name: "code change",
			bal: BlockAccessList{{
				Address:     accounts.InternAddress(common.Address{1}),
				CodeChanges: []*CodeChange{nil},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := EncodeBlockAccessListBytes(test.bal); err == nil {
				t.Fatal("expected nested nil encoding error")
			}
		})
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

func TestHeaderHasBAL(t *testing.T) {
	nonEmptyBALHash := common.Hash{1}
	tests := []struct {
		name string
		hash *common.Hash
		want bool
	}{
		{name: "missing", want: false},
		{name: "empty", hash: &empty.BlockAccessListHash, want: true},
		{name: "non-empty", hash: &nonEmptyBALHash, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header := Header{BlockAccessListHash: test.hash}
			if got := header.HasBAL(); got != test.want {
				t.Fatalf("HasBAL() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestHeaderHasNonEmptyBAL(t *testing.T) {
	nonEmptyBALHash := common.Hash{1}
	tests := []struct {
		name string
		hash *common.Hash
		want bool
	}{
		{name: "missing", want: false},
		{name: "empty", hash: &empty.BlockAccessListHash, want: false},
		{name: "non-empty", hash: &nonEmptyBALHash, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header := Header{BlockAccessListHash: test.hash}
			if got := header.HasNonEmptyBAL(); got != test.want {
				t.Fatalf("HasNonEmptyBAL() = %t, want %t", got, test.want)
			}
		})
	}
}

// TestBlockAccessListEmptyRoundTrip verifies that an empty BAL encodes to the
// canonical empty RLP list (0xc0) and decodes back to a non-nil empty slice.
// EIP-7928 requires: "When no state changes are present, this field is the
// empty RLP list 0xc0, i.e. rlp.encode([])."
func TestBlockAccessListEmptyRoundTrip(t *testing.T) {
	// Encode nil BAL — must produce 0xc0.
	encoded, err := EncodeBlockAccessListBytes(nil)
	if err != nil {
		t.Fatalf("encode nil BAL: %v", err)
	}
	if !bytes.Equal(encoded, []byte{0xc0}) {
		t.Fatalf("nil BAL encoding: got %x, want c0", encoded)
	}

	// Encode empty (non-nil) BAL — must also produce 0xc0.
	encoded2, err := EncodeBlockAccessListBytes(make(BlockAccessList, 0))
	if err != nil {
		t.Fatalf("encode empty BAL: %v", err)
	}
	if !bytes.Equal(encoded2, []byte{0xc0}) {
		t.Fatalf("empty BAL encoding: got %x, want c0", encoded2)
	}

	// Decode 0xc0 — must produce non-nil empty slice (not nil).
	decoded, err := DecodeBlockAccessListBytes(encoded)
	if err != nil {
		t.Fatalf("decode empty BAL: %v", err)
	}
	if decoded == nil {
		t.Fatal("decoded empty BAL must be non-nil")
	}
	if len(decoded) != 0 {
		t.Fatalf("decoded empty BAL length: got %d, want 0", len(decoded))
	}
}

// TestBlockAccessListRejectsEmptySlotChanges verifies that a BlockAccessList
// containing a storage slot with zero actual changes is strictly rejected.
// As per EIP-7928: "Each SlotChanges entry MUST contain at least one StorageChange."
func TestBlockAccessListRejectsEmptySlotChanges(t *testing.T) {
	var addr common.Address
	addr[19] = 0x01
	slot := common.HexToHash("0x01")

	ac := &AccountChanges{
		Address: accounts.InternAddress(addr),
		StorageChanges: []*SlotChanges{
			{
				Slot:    accounts.InternKey(slot),
				Changes: []*StorageChange{}, // Intentionally empty list
			},
		},
	}

	bal := BlockAccessList{ac}
	err := bal.Validate()

	if err == nil {
		t.Fatal("expected error for empty slot changes, but got nil")
	}
	if !strings.Contains(err.Error(), "empty slot changes") {
		t.Fatalf("expected 'empty slot changes' error, but got: %v", err)
	}
}

func TestDecodeBlockAccessListBytesRejectsMalformedRLP(t *testing.T) {
	malformed := map[string][]byte{
		"empty input":     {},
		"string not list": {0x80},
		"truncated list":  {0xc1},
		"trailing byte":   {0xc0, 0x00},
		"trailing list":   {0xc0, 0xc0},
	}
	for name, data := range malformed {
		_, err := DecodeBlockAccessListBytes(data)
		if err == nil {
			t.Fatalf("%s: expected error", name)
		}
		if errors.Is(err, ErrInvalidBlockAccessList) {
			t.Fatalf("%s: malformed RLP must not map to ErrInvalidBlockAccessList: %v", name, err)
		}
	}
}
