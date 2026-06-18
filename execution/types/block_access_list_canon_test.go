package types

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
)

// StorageChange/BalanceChange values are uint256 integers and must be encoded
// canonically (minimal big-endian, no leading zero bytes; zero is the empty
// string 0x80). The decoder must reject non-canonical encodings — accepting
// them is an RLP malleability. Here 0x00 is a non-canonical zero.

func TestStorageChangeRejectsNonCanonicalValue(t *testing.T) {
	t.Parallel()
	input := common.FromHex("0xc20100") // list[Index=1, Value=0x00]
	var sc StorageChange
	if err := rlp.DecodeBytes(input, &sc); err == nil {
		t.Fatalf("expected error for non-canonical Value, got nil (Value=%s)", sc.Value.String())
	}
}

func TestBalanceChangeRejectsNonCanonicalValue(t *testing.T) {
	t.Parallel()
	input := common.FromHex("0xc20100") // list[Index=1, Value=0x00]
	var bc BalanceChange
	if err := rlp.DecodeBytes(input, &bc); err == nil {
		t.Fatalf("expected error for non-canonical Value, got nil (Value=%s)", bc.Value.String())
	}
}

// Slot keys and storage-read keys (uint256, decoded via decodeMinimalHash) carry
// the same canonical-encoding requirement as the value fields. A non-minimal slot
// (leading zero byte) must be rejected, not silently mapped to the same key as its
// minimal form.
func TestSlotChangesRejectsNonCanonicalSlot(t *testing.T) {
	t.Parallel()
	// SlotChanges = [Slot, [ [Index=1, Value=1] ]]; slot 0x0001 has a leading zero.
	nonCanonical := common.FromHex("0xc7820001c3c20101")
	var sc SlotChanges
	if err := rlp.DecodeBytes(nonCanonical, &sc); err == nil {
		t.Fatalf("expected error for non-canonical slot, got nil (slot=%x)", sc.Slot.Value())
	}
	// The minimal form 0x01 of the same slot must still decode.
	canonical := common.FromHex("0xc501c3c20101")
	var sc2 SlotChanges
	if err := rlp.DecodeBytes(canonical, &sc2); err != nil {
		t.Fatalf("canonical slot must decode, got %v", err)
	}
}

func TestDecodeMinimalHashRejectsNonCanonical(t *testing.T) {
	t.Parallel()
	for _, nonCanonical := range []string{"0x00", "0x820001"} {
		s := rlp.NewStream(bytes.NewReader(common.FromHex(nonCanonical)), 0)
		if _, err := decodeMinimalHash(s); err == nil {
			t.Fatalf("input %s: expected error for non-canonical key, got nil", nonCanonical)
		}
	}
	for _, c := range []struct {
		hex  string
		want uint64
	}{{"0x80", 0}, {"0x01", 1}} {
		s := rlp.NewStream(bytes.NewReader(common.FromHex(c.hex)), 0)
		h, err := decodeMinimalHash(s)
		if err != nil {
			t.Fatalf("input %s: canonical key must decode, got %v", c.hex, err)
		}
		if got := h.Big().Uint64(); got != c.want {
			t.Fatalf("input %s: got key %d, want %d", c.hex, got, c.want)
		}
	}
}
