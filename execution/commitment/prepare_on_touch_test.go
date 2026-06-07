package commitment

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// preparedSplits holds one mounted HexPatriciaHashed per top-nibble split point.
// Keys are fed via touch() (the "during block execution" phase, hidden under
// exec in production); Process() then ONLY folds each split and stitches — the
// prepare-on-touch model.
type preparedSplits struct {
	base     *HexPatriciaHashed
	splits   [16]*HexPatriciaHashed
	present  [16]bool
	cleanups []func()
}

func newPreparedSplits(t testing.TB, factory TrieContextFactory) *preparedSplits {
	t.Helper()
	base := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())
	bctx, bclean := factory()
	base.ResetContext(bctx)
	base.branchEncoder.setDeferUpdates(true)
	base.SetLeaveDeferredForCaller(true)
	zero := []byte{0}
	for u := base.needUnfolding(zero); u > 0; u = base.needUnfolding(zero) {
		require.NoError(t, base.unfold(zero, u))
	}
	ps := &preparedSplits{base: base}
	if bclean != nil {
		ps.cleanups = append(ps.cleanups, bclean)
	}
	for i := 0; i < 16; i++ {
		w := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())
		w.mountTo(base, i)
		wctx, wclean := factory()
		w.ResetContext(wctx)
		w.branchEncoder.setDeferUpdates(true)
		w.SetLeaveDeferredForCaller(true)
		ps.splits[i] = w
		if wclean != nil {
			ps.cleanups = append(ps.cleanups, wclean)
		}
	}
	return ps
}

func (ps *preparedSplits) touch(hk, pk []byte, upd *Update) error {
	nib := hk[0]
	ps.present[nib] = true
	return ps.splits[nib].followAndUpdate(hk, pk, upd)
}

func (ps *preparedSplits) process() ([]byte, error) {
	ctx := context.Background()
	var cells [16]cell
	for i := 0; i < 16; i++ {
		if !ps.present[i] {
			continue
		}
		c, err := ps.splits[i].foldMounted(ctx, i)
		if err != nil {
			return nil, err
		}
		cells[i] = c
	}
	base := ps.base
	for nib := 0; nib < 16; nib++ {
		if !ps.present[nib] {
			continue
		}
		c := cells[nib]
		if c.extLen > 0 && c.accountAddrLen == 0 && c.storageAddrLen == 0 {
			c.extLen--
			copy(c.extension[:], c.extension[1:])
			c.hashedExtLen -= 2
			copy(c.hashedExtension[:], c.hashedExtension[2:])
		}
		base.touchMap[0] |= uint16(1) << nib
		if !c.IsEmpty() {
			base.afterMap[0] |= uint16(1) << nib
		} else {
			base.afterMap[0] &^= uint16(1) << nib
		}
		base.depths[0] = 1
		base.grid[0][nib] = c
	}
	if base.activeRows == 0 {
		base.activeRows = 1
	}
	for base.activeRows > 0 {
		if err := base.fold(); err != nil {
			return nil, err
		}
	}
	return base.RootHash()
}

func (ps *preparedSplits) release() {
	for _, c := range ps.cleanups {
		c()
	}
}

// sortedTriples returns (hashedKey, plainKey, *update) sorted by hashed key — the
// order followAndUpdate requires. In production the eager-unfold/deferred-apply
// drain produces this; here we sort directly.
type triple struct {
	hk, pk []byte
	upd    *Update
}

func sortedTriples(pk [][]byte, upds []Update) []triple {
	ts := make([]triple, len(pk))
	for i := range pk {
		ts[i] = triple{hk: KeyToHexNibbleHash(pk[i]), pk: pk[i], upd: &upds[i]}
	}
	sort.Slice(ts, func(i, j int) bool {
		a, b := ts[i].hk, ts[j].hk
		for k := 0; k < len(a) && k < len(b); k++ {
			if a[k] != b[k] {
				return a[k] < b[k]
			}
		}
		return len(a) < len(b)
	})
	return ts
}

func TestPrepareOnTouch_Parity(t *testing.T) {
	pk, upds := buildMixedCorpus(99, 6000)

	seqMs := NewMockState(t)
	require.NoError(t, seqMs.applyPlainUpdates(pk, upds))
	seq := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	sUpd := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
	seqRoot, err := seq.Process(context.Background(), sUpd, "", nil, WarmupConfig{})
	require.NoError(t, err)
	sUpd.Close()

	ms := NewMockState(t)
	require.NoError(t, ms.applyPlainUpdates(pk, upds))
	ps := newPreparedSplits(t, mockTrieCtxFactory(ms))
	defer ps.release()
	for _, tr := range sortedTriples(pk, upds) {
		require.NoError(t, ps.touch(tr.hk, tr.pk, tr.upd))
	}
	root, err := ps.process()
	require.NoError(t, err)
	require.Equal(t, seqRoot, root, "prepare-on-touch root != sequential")
}

// Benchmark_PrepareOnTouch reports prepare (touch/followAndUpdate, hidden under
// execution in production) vs process (fold+stitch, the commitment-time cost) on
// the whale corpus, with top-nibble split points.
func Benchmark_PrepareOnTouch(b *testing.B) {
	pk, upds := build1MWhaleCorpus(b)
	for b.Loop() {
		b.StopTimer()
		ms := NewMockState(b)
		require.NoError(b, ms.applyPlainUpdates(pk, upds))
		ts := sortedTriples(pk, upds)
		ps := newPreparedSplits(b, mockTrieCtxFactory(ms))

		tp := time.Now()
		for _, tr := range ts {
			require.NoError(b, ps.touch(tr.hk, tr.pk, tr.upd))
		}
		prepare := time.Since(tp)

		b.StartTimer()
		_, err := ps.process()
		b.StopTimer()
		require.NoError(b, err)
		b.ReportMetric(float64(prepare.Nanoseconds()), "prepare-ns")
		ps.release()
		b.StartTimer()
	}
}
