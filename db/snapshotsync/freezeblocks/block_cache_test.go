package freezeblocks

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snapshotsync"
)

func TestCache2(t *testing.T) {
	ran := snapshotsync.NewRange(10000, 10500)
	// assume block i has maxTxNum = 200*(2+i-ran.from) (inclusive)
	cacheMiss := 0
	b2tx := func(blk uint64) (uint64, error) {
		cacheMiss++
		// assume expensive op
		return 200 * (2 + blk - ran.From()), nil
	}

	b2txNoErr := func(blk uint64) uint64 {
		blk, _ = b2tx(blk)
		return blk
	}

	test := func(queryTxNum, expectedBlk, cacheMissFirstTime, cacheMissSecondTime uint64) {
		cache := NewBlockTxNumLookupCache(100)
		//q := cache.NewQuery(ran)
		cacheMiss = 0
		answer, err := cache.Find(ran, queryTxNum, b2tx)
		require.NoError(t, err)
		require.Equal(t, cacheMissFirstTime, uint64(cacheMiss))
		require.Equal(t, expectedBlk, answer)

		cacheMiss = 0
		answer, err = cache.Find(ran, queryTxNum, b2tx)
		require.NoError(t, err)
		require.Equal(t, expectedBlk, answer)
		require.Equal(t, uint64(cacheMiss), cacheMissSecondTime)
	}

	test(b2txNoErr(10015)-100, uint64(10015), 10, 7)
	test(400, uint64(10000), 3, 0) //  queries 10000, which is cached
	test(b2txNoErr(10499), uint64(10499), 9, 6)
	test(b2txNoErr(10499)-200, uint64(10498), 10, 7)
	test(b2txNoErr(10250), uint64(10250), 9, 6)
	test(b2txNoErr(10488), uint64(10488), 10, 7)
}
