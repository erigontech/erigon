package v4

import (
	"bytes"
	"context"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
)

func incrRoots(t *testing.T, batch1, batch2 int, seed int64) (v4r, hr []byte, err error) {
	rnd := rand.New(rand.NewSource(seed))
	addr := make([]byte, length.Addr)
	rnd.Read(addr)
	slots := make([][]byte, batch1+batch2)
	for i := range slots {
		s := make([]byte, length.Hash)
		rnd.Read(s)
		slots[i] = s
	}
	build := func(lo, hi int, withAcct bool) []parityUpdate {
		var out []parityUpdate
		if withAcct {
			out = append(out, parityUpdate{key: addr, update: accountParityUpdate(1)})
		}
		for i := lo; i < hi; i++ {
			out = append(out, parityUpdate{
				key: append(append([]byte{}, addr...), slots[i]...), update: storageParityUpdate(i)})
		}
		return out
	}
	mk := func(es []parityUpdate) *commitment.Updates {
		u := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
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
	if _, err = tr.Process(ctx, mk(build(0, batch1, true)), "", nil, commitment.WarmupConfig{}); err != nil {
		return nil, nil, err
	}
	if _, err = hph.Process(ctx, mk(build(0, batch1, true)), "", nil, commitment.WarmupConfig{}); err != nil {
		return nil, nil, err
	}
	if v4r, err = tr.Process(ctx, mk(build(batch1, batch1+batch2, false)), "", nil, commitment.WarmupConfig{}); err != nil {
		return nil, nil, err
	}
	hr, err = hph.Process(ctx, mk(build(batch1, batch1+batch2, false)), "", nil, commitment.WarmupConfig{})
	return v4r, hr, err
}

func TestZZIncrementalDivergence(t *testing.T) {
	good, bad, errs := 0, 0, 0
	for seed := int64(1); seed <= 40; seed++ {
		for _, b1 := range []int{2, 3, 5} {
			for _, b2 := range []int{1, 2, 3} {
				v4r, hr, err := incrRoots(t, b1, b2, seed)
				switch {
				case err != nil:
					errs++
				case !bytes.Equal(v4r, hr):
					bad++
					if bad <= 3 {
						t.Logf("DIVERGE seed=%d b1=%d b2=%d v4=%x hph=%x", seed, b1, b2, v4r[:8], hr[:8])
					}
				default:
					good++
				}
			}
		}
	}
	t.Logf("TOTAL match=%d diverge=%d error=%d", good, bad, errs)
	if bad != 0 || errs != 0 {
		t.Errorf("v4 must match HPH on every incremental batch: %d diverged, %d errored", bad, errs)
	}
}
