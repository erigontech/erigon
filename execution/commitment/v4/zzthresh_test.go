package v4

import "testing"

func TestZZThresholdPerSeed(t *testing.T) {
	for _, seed := range []int64{424242, 1, 2, 3, 7, 99, 12345, 777, 31337, 5150} {
		first := -1
		for n := 1; n <= 80; n++ {
			if err := oneAccountNSlots(t, n, seed); err != nil {
				first = n
				break
			}
		}
		t.Logf("seed=%-8d first failing slot count = %d", seed, first)
		if first != -1 {
			t.Errorf("seed=%d: v4 failed at %d slots; must handle any slot count", seed, first)
		}
	}
}
