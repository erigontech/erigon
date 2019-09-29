package trie

import (
	"fmt"
	"testing"
)

func TestKeyValue(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	bwb.keyValue([]byte("key"), []byte("value"))
	fmt.Printf("key buffer: %x\n", bwb.Keys.buffer.Bytes())
	fmt.Printf("value buffer: %x\n", bwb.Values.buffer.Bytes())
}
