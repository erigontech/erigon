package hermez_db

import (
	"fmt"
	"testing"
)

func TestSplitKey(t *testing.T) {
	scenarios := []struct {
		input       []byte
		outputL1    uint64
		outputBatch uint64
		hasError    bool
	}{
		{
			append(make([]byte, 8), make([]byte, 8)...),
			0, 0, false,
		},
		{
			append([]byte{0, 0, 0, 0, 0, 0, 0, 1}, make([]byte, 8)...),
			1, 0, false,
		},
		{
			append(make([]byte, 8), []byte{0, 0, 0, 0, 0, 0, 0, 1}...),
			0, 1, false,
		},
		{
			make([]byte, 15),
			0, 0, true,
		},
	}

	for _, tt := range scenarios {
		t.Run(fmt.Sprintf("Input: %v", tt.input), func(t *testing.T) {
			l1, batch, err := SplitKey(tt.input)
			if tt.hasError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
				if l1 != tt.outputL1 || batch != tt.outputBatch {
					t.Errorf("got (%d, %d), want (%d, %d)", l1, batch, tt.outputL1, tt.outputBatch)
				}
			}
		})
	}
}
