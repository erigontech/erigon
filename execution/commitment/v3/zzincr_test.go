package v3

import (
	"bytes"
	"context"
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
)

func incrRoots(t *testing.T, batch1, batch2 int, seed int64) (v3r, hr []byte, err error) {
	input, genErr := commitmenttest.Generate(commitmenttest.MathRand(seed), commitmenttest.SequenceSpec{Kind: "whale", BatchSizes: []int{batch1, batch2}})
	if genErr != nil {
		return nil, nil, genErr
	}
	rounds := [][]parityUpdate{parityEntries(input.Rounds[0]), parityEntries(input.Rounds[1])}
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
	if _, err = tr.Process(ctx, mk(commitment.ModeCollect, rounds[0]), "", nil, commitment.WarmupConfig{}); err != nil {
		return nil, nil, err
	}
	zzSeed(ch, rounds[0])
	if _, err = hph.Process(ctx, mk(commitment.ModeUpdate, rounds[0]), "", nil, commitment.WarmupConfig{}); err != nil {
		return nil, nil, err
	}
	if v3r, err = tr.Process(ctx, mk(commitment.ModeCollect, rounds[1]), "", nil, commitment.WarmupConfig{}); err != nil {
		return nil, nil, err
	}
	zzSeed(ch, rounds[1])
	hr, err = hph.Process(ctx, mk(commitment.ModeUpdate, rounds[1]), "", nil, commitment.WarmupConfig{})
	return v3r, hr, err
}

func TestZZIncrementalDivergence(t *testing.T) {
	good, bad, errs := 0, 0, 0
	for seed := int64(1); seed <= 40; seed++ {
		for _, b1 := range []int{2, 3, 5} {
			for _, b2 := range []int{1, 2, 3} {
				v3r, hr, err := incrRoots(t, b1, b2, seed)
				switch {
				case err != nil:
					errs++
				case !bytes.Equal(v3r, hr):
					bad++
					if bad <= 3 {
						t.Logf("DIVERGE seed=%d b1=%d b2=%d v3=%x hph=%x", seed, b1, b2, v3r[:8], hr[:8])
					}
				default:
					good++
				}
			}
		}
	}
	t.Logf("TOTAL match=%d diverge=%d error=%d", good, bad, errs)
	if bad != 0 || errs != 0 {
		t.Errorf("v3 must match HPH on every incremental batch: %d diverged, %d errored", bad, errs)
	}
}
