package v4

import (
	"context"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
)

func oneAccountNSlots(t *testing.T, n int, seed int64) error {
	rnd := rand.New(rand.NewSource(seed))
	addr := make([]byte, length.Addr)
	rnd.Read(addr)
	entries := []parityUpdate{{key: addr, update: accountParityUpdate(1)}}
	for i := range n {
		slot := make([]byte, length.Hash)
		rnd.Read(slot)
		entries = append(entries, parityUpdate{
			key:    append(append([]byte{}, addr...), slot...),
			update: storageParityUpdate(i),
		})
	}
	c := newParityContext()
	tr := &Trie{}
	tr.ResetContext(c)
	defer tr.Release()
	u := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
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
			t.Errorf("v4 must hash a %d-slot storage trie: %v", n, err)
		}
	}
}

func TestZZSeedSweepTwoSlots(t *testing.T) {
	fails := 0
	for seed := int64(0); seed < 200; seed++ {
		if err := oneAccountNSlots(t, 2, seed); err != nil {
			fails++
		}
	}
	t.Logf("2 slots, 200 seeds: %d failed", fails)
}
