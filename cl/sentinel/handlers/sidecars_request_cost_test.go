package handlers

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSidecarsRequestCost(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		slots, sidecarsPerSlot, limit uint64
		want                          int
	}{
		{"no slots", 0, 6, 72, 0},
		{"no sidecars", 2, 0, 72, 0},
		{"zero limit", 2, 6, 0, 0},
		{"admission only", 1, 1, 72, 0},
		{"small product", 2, 6, 72, 11},
		{"at cap", 12, 6, 72, 71},
		{"above cap", 13, 6, 72, 71},
		{"partial slot at cap", 12, 6, 71, 70},
		{"overflowing product", math.MaxUint64, math.MaxUint64, 72, 71},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, sidecarsRequestCost(tc.slots, tc.sidecarsPerSlot, tc.limit))
		})
	}
}
