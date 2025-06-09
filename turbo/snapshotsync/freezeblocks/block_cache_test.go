package freezeblocks

import (
	"testing"

	"github.com/erigontech/erigon/turbo/snapshotsync"
	"github.com/stretchr/testify/require"
)

func TestCache2(t *testing.T) {
	ran := snapshotsync.NewRange(10000, 10500)
	// assume block i has maxTxNum = 200*(2+i-ran.from) (inclusive)

	b2tx := func(blk uint64) uint64 {
		// assume expensive op
		return 200 * (2 + blk - ran.From())
	}

	test := func(queryTxNum, expectedBlk, cacheMissExpected uint64) {
		cache := NewBlockTxNumLookupCache(128)
		//q := cache.NewQuery(ran)
		cacheMiss := 0
		f := func(i uint64) bool {
			val, exists := cache.GetMaxTxNum(ran, i)
			if !exists {
				val = b2tx(i)
				if cache.SetMaxTxNum(ran, i, val) {
					cacheMiss++
				}
			}

			return val >= queryTxNum
		}

		l, r := uint64(10000), uint64(10500)
		for l < r {
			h := uint64(uint(l+r) >> 1)
			if !f(h) {
				l = h + 1
			} else {
				r = h
			}
		}
		answer := l
		require.Equal(t, cacheMissExpected, uint64(cacheMiss))
		require.Equal(t, expectedBlk, answer)

		l, r = uint64(10000), uint64(10500)
		cacheMiss = 0
		for l < r {
			h := uint64(uint(l+r) >> 1)
			if !f(h) {
				// right
				l = h + 1
			} else {
				// left
				r = h
			}
		}
		require.Equal(t, answer, l)
		require.Equal(t, 0, cacheMiss)
	}

	test(b2tx(10015)-100, uint64(10015), 2)
	test(400, uint64(10000), 3)
	test(b2tx(10499), uint64(10499), 3)
	test(b2tx(10499)-200, uint64(10498), 3)
	test(b2tx(10550), uint64(10500), 3)
	test(b2tx(10250), uint64(10250), 2)
}
