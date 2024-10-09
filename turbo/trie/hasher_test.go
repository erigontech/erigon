package trie

import (
	"fmt"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

func TestValue(t *testing.T) {
	t.Skip("should be restored. skipped for Erigon")

	h := newHasher(false)
	var hn libcommon.Hash
	h.hash(valueNode("BLAH"), false, hn[:])
	expected := "0x0"
	actual := fmt.Sprintf("0x%x", hn[:])
	if actual != expected {
		t.Errorf("Expected %s, got %x", expected, actual)
	}
}
