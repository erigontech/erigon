package btindex

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/seg"
)

// shortKeyPairs returns a sorted key set mixing 0-, 1- and 2+-byte keys under the
// same first byte, like the commitment domain does with short nibble paths.
func shortKeyPairs() (keys, vals [][]byte) {
	keys = [][]byte{
		{},
		{0x0a},
		{0x0a, 0x00},
		{0x0a, 0x00, 0x01},
		{0x0a, 0x01},
		{0x0a, 0x02},
		{0x0b},
		{0x0b, 0x00},
		{0x0c, 0x00},
	}
	vals = make([][]byte, len(keys))
	for i := range keys {
		vals[i] = []byte{byte(i), 0xff}
	}
	return keys, vals
}

func TestPrefixIndexShortKeyGet(t *testing.T) {
	t.Parallel()
	keys, vals := shortKeyPairs()
	compressFlags := seg.CompressNone
	kvPath := generateMinimalKV(t, t.TempDir(), keys, vals, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	for i := range keys {
		if len(keys[i]) == 0 {
			continue // empty key has its own contract, covered elsewhere
		}
		v, ok, _, err := pi.Get(g, keys[i])
		require.NoError(t, err)
		require.True(t, ok, "key %x not found", keys[i])
		require.Equal(t, vals[i], v, "key %x", keys[i])
	}
}

func TestPrefixIndexShortKeySeek(t *testing.T) {
	t.Parallel()
	keys, vals := shortKeyPairs()
	compressFlags := seg.CompressNone
	kvPath := generateMinimalKV(t, t.TempDir(), keys, vals, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	for i := range keys {
		if len(keys[i]) == 0 {
			continue
		}
		c, err := pi.Seek(g, keys[i])
		require.NoError(t, err)
		require.NotNil(t, c, "seek %x returned nil", keys[i])
		require.Truef(t, bytes.Equal(c.Key(), keys[i]), "seek %x landed on %x", keys[i], c.Key())
		c.Close()
	}
}
