package commitment

import (
	"context"
	"sort"
	"testing"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/stretchr/testify/require"
)

type countingCtx struct {
	inner      PatriciaContext
	readN      map[int]int
	readB      map[int]int
	writeN     map[int]int
	writeB     map[int]int
	readKeys   map[string]int
	emptyReads int
	collecting bool
}

func newCountingCtx(inner PatriciaContext) *countingCtx {
	return &countingCtx{
		inner: inner,
		readN: map[int]int{}, readB: map[int]int{},
		writeN: map[int]int{}, writeB: map[int]int{},
		readKeys: map[string]int{},
	}
}

func (c *countingCtx) Branch(prefix []byte) ([]byte, kv.Step, error) {
	v, s, err := c.inner.Branch(prefix)
	if c.collecting {
		d := len(prefix)
		c.readN[d]++
		c.readB[d] += len(v)
		c.readKeys[string(prefix)]++
		if len(v) == 0 {
			c.emptyReads++
		}
	}
	return v, s, err
}

func (c *countingCtx) PutBranch(prefix, data, prev []byte) error {
	if c.collecting {
		d := len(prefix)
		c.writeN[d]++
		c.writeB[d] += len(data)
	}
	return c.inner.PutBranch(prefix, data, prev)
}

func (c *countingCtx) Account(k []byte) (*Update, error) { return c.inner.Account(k) }
func (c *countingCtx) Storage(k []byte) (*Update, error) { return c.inner.Storage(k) }

func TestDeltaBranchDepthShape(t *testing.T) {
	for _, c := range whaleCases() {
		t.Run(c.name, func(t *testing.T) {
			pk, upds := buildWhaleCorpus(c.opts)
			dk, du := buildDelta(pk, upds, 500, 4242)
			ctx := context.Background()

			ms := NewMockState(t)
			require.NoError(t, ms.applyPlainUpdates(pk, upds))
			cc := newCountingCtx(ms)
			hph := NewHexPatriciaHashed(length.Addr, cc, DefaultTrieConfig())
			u1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
			_, err := hph.Process(ctx, u1, "", nil, WarmupConfig{})
			require.NoError(t, err)
			u1.Close()

			require.NoError(t, ms.applyPlainUpdates(dk, du))
			cc.collecting = true
			u2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, dk, du)
			_, err = hph.Process(ctx, u2, "", nil, WarmupConfig{})
			require.NoError(t, err)
			u2.Close()

			depths := []int{}
			for d := range cc.readN {
				depths = append(depths, d)
			}
			sort.Ints(depths)
			totN, totB, repeats, repeatN := 0, 0, 0, 0
			for _, d := range depths {
				totN += cc.readN[d]
				totB += cc.readB[d]
			}
			for _, n := range cc.readKeys {
				if n > 1 {
					repeats++
					repeatN += n - 1
				}
			}
			t.Logf("base=%d delta=%d  totalReads=%d totalReadBytes=%d distinctPrefixes=%d",
				len(pk), len(dk), totN, totB, len(cc.readKeys))
			t.Logf("  re-read prefixes=%d extraReads=%d (%.1f%% of reads)  emptyReads=%d (%.1f%%)",
				repeats, repeatN, 100*float64(repeatN)/float64(totN),
				cc.emptyReads, 100*float64(cc.emptyReads)/float64(totN))
			t.Logf("  %-6s %8s %8s %10s %8s", "depth", "reads", "writes", "readBytes", "cum%")
			cum := 0
			for _, d := range depths {
				cum += cc.readB[d]
				t.Logf("  %-6d %8d %8d %10d %7.1f%%", d, cc.readN[d], cc.writeN[d], cc.readB[d],
					100*float64(cum)/float64(totB))
			}
		})
	}
}
