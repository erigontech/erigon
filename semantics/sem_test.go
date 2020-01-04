package semantics

import (
	"testing"
)

func TestSemantics(t *testing.T) {
	y := Sem(3)
	if y != 4 {
		t.Errorf("Expected 4, got: %d", y)
	}
}
