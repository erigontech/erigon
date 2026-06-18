package types

import (
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
