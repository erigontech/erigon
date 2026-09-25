package v3

import (
	"bytes"
	"context"
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
)

func zzSeed(ctx *parityContext, es []parityUpdate) {
	for _, e := range es {
		if e.update == nil {
			continue
		}
		if e.update.Deleted() {
			if len(e.key) == 20 {
				delete(ctx.accounts, string(e.key))
			} else {
				delete(ctx.storage, string(e.key))
			}
			continue
		}
		if e.update.Flags&commitment.StorageUpdate != 0 {
			ctx.storage[string(e.key)] = e.update.Copy()
		} else if len(e.key) == 20 {
			ctx.accounts[string(e.key)] = e.update.Copy()
		}
	}
}

func oneSlotPerAccount(t *testing.T, n1, n2 int, seed int64, mode string) (m1, m2 bool, err error) {
	input, genErr := commitmenttest.Generate(commitmenttest.MathRand(seed), commitmenttest.SequenceSpec{Kind: "one-slot", BatchSizes: []int{n1, n2}, Rewrite: mode != "new"})
	if genErr != nil {
		return false, false, genErr
	}
	mk := func(mode commitment.Mode, es []parityUpdate) *commitment.Updates {
		u := commitment.NewUpdates(mode, t.TempDir(), commitment.KeyToHexNibbleHash)
		for _, e := range es {
			u.TouchPlainKeyDirect(string(e.key), e.update)
		}
		return u
	}
	cv, ch := newParityContext(), newParityContext()
	tr := &Trie{}
	tr.ResetContext(cv)
	defer tr.Release()
	hph := commitment.NewHexPatriciaHashed(length.Addr, ch, commitment.DefaultTrieConfig())
	defer hph.Release()
	ctx := context.Background()

	b1 := parityEntries(input.Rounds[0])
	v1, err := tr.Process(ctx, mk(commitment.ModeCollect, b1), "", nil, commitment.WarmupConfig{})
	if err != nil {
		return false, false, err
	}
	zzSeed(ch, b1)
	h1, err := hph.Process(ctx, mk(commitment.ModeUpdate, b1), "", nil, commitment.WarmupConfig{})
	if err != nil {
		return false, false, err
	}
	m1 = bytes.Equal(v1, h1)

	b2 := parityEntries(input.Rounds[1])
	v2, err := tr.Process(ctx, mk(commitment.ModeCollect, b2), "", nil, commitment.WarmupConfig{})
	if err != nil {
		return m1, false, err
	}
	zzSeed(ch, b2)
	h2, err := hph.Process(ctx, mk(commitment.ModeUpdate, b2), "", nil, commitment.WarmupConfig{})
	if err != nil {
		return m1, false, err
	}
	return m1, bytes.Equal(v2, h2), nil
}

func TestZZOneSlotPerAccountIncremental(t *testing.T) {
	for _, mode := range []string{"new", "update"} {
		ok1, bad1, ok2, bad2, errs := 0, 0, 0, 0, 0
		for seed := int64(1); seed <= 40; seed++ {
			for _, n1 := range []int{2, 5, 20} {
				for _, n2 := range []int{1, 2, 5} {
					a, b, err := oneSlotPerAccount(t, n1, n2, seed, mode)
					if err != nil {
						errs++
						continue
					}
					if a {
						ok1++
					} else {
						bad1++
					}
					if b {
						ok2++
					} else {
						bad2++
					}
				}
			}
		}
		t.Logf("mode=%-7s batch1 ok=%d bad=%d | batch2 ok=%d bad=%d | err=%d", mode, ok1, bad1, ok2, bad2, errs)
		if bad1 != 0 || bad2 != 0 || errs != 0 {
			t.Errorf("mode=%s: v3 must match HPH: batch1 bad=%d batch2 bad=%d err=%d", mode, bad1, bad2, errs)
		}
	}
}
