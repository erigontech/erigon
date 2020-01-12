package semantics

import (
	"fmt"
	"testing"
)

func TestSemantics(t *testing.T) {
	var stateRoot [32]byte
	var fromAddr [20]byte
	var toAddr [20]byte
	var value [16]byte
	result := Initialise(stateRoot, fromAddr, toAddr, false, value, []byte{}, 1, 100)
	if result != 0 {
		t.Errorf("Could not initialise: %d", result)
	}
	fmt.Printf("Got it1!\n")
	Cleanup()
}
