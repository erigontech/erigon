package semantics

import (
	"fmt"
	"testing"
)

func TestSemantics(t *testing.T) {
	var stateRoot [32]byte
	result := Initialise(stateRoot, []byte{}, 100)
	if result != 0 {
		t.Errorf("Could not initialise: %d", result)
	}
	fmt.Printf("Got it1!\n")
}
