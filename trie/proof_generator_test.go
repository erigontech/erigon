package trie

import (
	"fmt"
	"testing"
)

func TestKeyValue(t *testing.T) {
	bwb := NewBlockWitnessBuilder()
	if err := bwb.keyValue([]byte("key"), []byte("value")); err != nil {
		t.Errorf("Could not set key-value: %v", err)
	}
	fmt.Printf("key buffer: %x\n", bwb.Keys.buffer.Bytes())
	fmt.Printf("value buffer: %x\n", bwb.Values.buffer.Bytes())
}
