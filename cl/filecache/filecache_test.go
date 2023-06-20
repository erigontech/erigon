package filecache_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/cl/filecache"
	"github.com/stretchr/testify/require"
)

type s struct {
	A int32
	B uint64
}

func TestCache(t *testing.T) {

	rootDir := filepath.Join(os.TempDir(), fmt.Sprintf("caplin-fixture-%d", time.Now().Unix()))
	func() {
		c, err := filecache.NewCache[string, s](3, rootDir)
		require.NoError(t, err)
		defer c.Close()

		// ensure the folder exists
		_, err = os.Stat(rootDir)
		require.NoError(t, err)

		var evicted bool
		evicted, err = c.Add("one", &s{A: 4, B: 4})
		require.NoError(t, err)
		require.False(t, evicted)

		evicted, err = c.Add("two", &s{A: 8, B: 5})
		require.NoError(t, err)
		require.False(t, evicted)

		evicted, err = c.Add("three", &s{A: 12, B: 6})
		require.NoError(t, err)
		require.False(t, evicted)

		// access one to reset the timer
		v1, ok, err := c.Get("one")
		require.NoError(t, err)
		require.True(t, ok)
		require.EqualValues(t, &s{A: 4, B: 4}, v1)

		// add four
		evicted, err = c.Add("four", &s{A: 16, B: 7})
		require.NoError(t, err)
		require.True(t, evicted)

		// ensure four is here
		v4, ok, err := c.Get("four")
		require.NoError(t, err)
		require.True(t, ok)
		require.EqualValues(t, &s{A: 16, B: 7}, v4)

		// ensure one is still here
		v1, ok, err = c.Get("one")
		require.NoError(t, err)
		require.True(t, ok)
		require.EqualValues(t, &s{A: 4, B: 4}, v1)

		// ensure two is evicted
		_, ok, err = c.Get("two")
		require.NoError(t, err)
		require.False(t, ok)
	}()

	// ensure that close cleans up the dir
	_, err := os.Stat(rootDir)
	require.Error(t, err)

}
