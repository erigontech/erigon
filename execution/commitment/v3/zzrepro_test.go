package v3

import (
	"context"
	"testing"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
)

func oneAccountNSlots(t *testing.T, n int, seed int64) error {
	input, genErr := commitmenttest.Generate(commitmenttest.MathRand(seed), commitmenttest.SequenceSpec{Kind: "whale", Count: n})
	if genErr != nil {
		return genErr
	}
	entries := parityEntries(input.Rounds[0])
	c := newParityContext()
	tr := &Trie{}
	tr.ResetContext(c)
	defer tr.Release()
	u := commitment.NewUpdates(commitment.ModeCollect, t.TempDir(), commitment.KeyToHexNibbleHash)
	for _, e := range entries {
		u.TouchPlainKeyDirect(string(e.key), e.update)
	}
	_, err := tr.Process(context.Background(), u, "", nil, commitment.WarmupConfig{})
	return err
}

func TestZZOneAccountManySlots(t *testing.T) {
	for _, n := range []int{16, 17, 18, 19, 20, 21, 22, 24, 26, 28, 30, 31, 32} {
		err := oneAccountNSlots(t, n, 424242)
		status := "ok"
		if err != nil {
			status = "FAIL: " + err.Error()
		}
		t.Logf("slots=%-5d %s", n, status)
		if err != nil {
			t.Errorf("v3 must hash a %d-slot storage trie: %v", n, err)
		}
	}
}

func TestZZSeedSweepTwoSlots(t *testing.T) {
	fails := 0
	for seed := range int64(200) {
		if err := oneAccountNSlots(t, 2, seed); err != nil {
			fails++
		}
	}
	t.Logf("2 slots, 200 seeds: %d failed", fails)
}
