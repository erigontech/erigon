package types

import (
	"bytes"
	"math/big"
	"strings"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
)

func TestBalanceChangeEncodeRejectsNegative(t *testing.T) {
	bc := &BalanceChange{
		Index: 1,
		Value: big.NewInt(-1),
	}
	var buf bytes.Buffer
	if err := bc.EncodeRLP(&buf); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("expected negative value error, got %v", err)
	}
}

func TestBalanceChangeEncodeRejectsOverflow(t *testing.T) {
	value := new(big.Int).Lsh(big.NewInt(1), 257)
	bc := &BalanceChange{
		Index: 1,
		Value: value,
	}
	var buf bytes.Buffer
	if err := bc.EncodeRLP(&buf); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected overflow error, got %v", err)
	}
}

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
