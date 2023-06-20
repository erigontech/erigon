package filecache_test

import (
	"bytes"
	"encoding/binary"
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

func (o *s) MarshalBinary() (data []byte, err error) {
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, o)
	return buf.Bytes(), err
}

func (o *s) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.LittleEndian, o)
	return err
}

func TestCache(t *testing.T) {

	rootDir := filepath.Join(os.TempDir(), fmt.Sprintf("caplin-fixture-%d", time.Now().Unix()))
	func() {
		c, err := filecache.NewCache[string, *s](3, rootDir)
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

		val := &s{}
		// access one to reset the timer
		ok, err := c.Get("one", val)
		require.NoError(t, err)
		require.True(t, ok)
		require.EqualValues(t, &s{A: 4, B: 4}, val)

		// add four
		evicted, err = c.Add("four", &s{A: 16, B: 7})
		require.NoError(t, err)
		require.True(t, evicted)

		// ensure four is here
		ok, err = c.Get("four", val)
		require.NoError(t, err)
		require.True(t, ok)
		require.EqualValues(t, &s{A: 16, B: 7}, val)

		// ensure one is still here
		ok, err = c.Get("one", val)
		require.NoError(t, err)
		require.True(t, ok)
		require.EqualValues(t, &s{A: 4, B: 4}, val)

		// ensure two is evicted
		ok, err = c.Get("two", val)
		require.NoError(t, err)
		require.False(t, ok)
	}()

	// ensure that close cleans up the dir
	_, err := os.Stat(rootDir)
	require.Error(t, err)

}
