package trie

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
)

func testValue(t *testing.T) {
	h := newHasher(false)
	var hn common.Hash
	h.hash(valueNode([]byte("BLAH")), false, hn[:])
	expected := "0x0"
	if common.ToHex(hn[:]) != expected {
		t.Errorf("Expected %s, got %s", expected, common.ToHex(hn[:]))
	}
}
