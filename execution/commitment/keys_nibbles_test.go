package commitment

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
)

func TestAccountKey(t *testing.T) {
	t.Parallel()
	accKey := hexutil.MustDecode("0x00112233445566778899aabbccddeeff00112233")
	nibblizedHashedKey := KeyToHexNibbleHash(accKey)

	require.Equal(t, hexutil.MustDecode("0x0b070f0f040d05000b0d01080705010601060800020a0400060c09040b0109000f010a030f0d040f0c08020b00060d0b04000904030e000101090c050e080b0c"), nibblizedHashedKey)
}

func TestStorageKey(t *testing.T) {
	t.Parallel()
	accKey := hexutil.MustDecode("0x00112233445566778899aabbccddeeff00112233")
	storageKey := hexutil.MustDecode("0x00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff")
	nibblizedHashedKey := KeyToHexNibbleHash(append(accKey, storageKey...))

	require.Equal(t, hexutil.MustDecode(
		"0x0b070f0f040d05000b0d01080705010601060800020a0400060c09040b0109000f010a030f0d040f0c08020b00060d0b04000904030e000101090c050e080b0c"+
			"020d040906010f0e0803000401080b02000a070601050b0d060003030f0e01040100060a090303090500060c0c0905020c0b0b040e0d0007030a03000807030c",
	), nibblizedHashedKey)
}

func TestKeyToHexNibbleHashCached_MatchesUncached(t *testing.T) {
	t.Parallel()

	t.Run("account_keys", func(t *testing.T) {
		var c AddrHashCache
		for i := range 100 {
			addr := make([]byte, length.Addr)
			addr[0] = byte(i)
			addr[19] = byte(i * 7)
			assert.Equal(t, KeyToHexNibbleHash(addr), KeyToHexNibbleHashCached(addr, &c), "account key %d", i)
		}
	})

	t.Run("storage_keys", func(t *testing.T) {
		var c AddrHashCache
		for i := range 100 {
			key := make([]byte, 52)
			key[0] = byte(i % 30)
			key[19] = byte(i)
			key[20] = byte(i)
			key[51] = byte(i * 3)
			assert.Equal(t, KeyToHexNibbleHash(key), KeyToHexNibbleHashCached(key, &c), "storage key %d", i)
		}
	})

	t.Run("whale_storage", func(t *testing.T) {
		var c AddrHashCache
		addr := make([]byte, length.Addr)
		addr[0], addr[1], addr[19] = 0xDE, 0xAD, 0xBE
		for slot := range 1000 {
			key := make([]byte, 52)
			copy(key[:20], addr)
			key[20] = byte(slot >> 8)
			key[51] = byte(slot)
			assert.Equal(t, KeyToHexNibbleHash(key), KeyToHexNibbleHashCached(key, &c), "whale slot %d", slot)
		}
	})

	t.Run("interleaved", func(t *testing.T) {
		var c AddrHashCache
		for i := range 200 {
			addr := make([]byte, length.Addr)
			addr[0] = byte(i % 4)
			addr[19] = byte(i % 4)
			assert.Equal(t, KeyToHexNibbleHash(addr), KeyToHexNibbleHashCached(addr, &c), "acct %d", i)

			key := make([]byte, 52)
			copy(key[:20], addr)
			key[20] = byte(i)
			key[51] = byte(i)
			assert.Equal(t, KeyToHexNibbleHash(key), KeyToHexNibbleHashCached(key, &c), "storage %d", i)
		}
	})
}

func TestAddrHashCache_ReuseAndInvalidation(t *testing.T) {
	t.Parallel()
	var c AddrHashCache
	require.False(t, c.valid)

	mkKey := func(addrByte, slot byte) []byte {
		key := make([]byte, 52)
		key[0] = addrByte
		key[51] = slot
		return key
	}

	KeyToHexNibbleHashCached(mkKey(0xAA, 0), &c)
	require.True(t, c.valid)
	require.Equal(t, byte(0xAA), c.addr[0])
	firstNibs := c.nibs

	KeyToHexNibbleHashCached(mkKey(0xAA, 1), &c)
	require.Equal(t, firstNibs, c.nibs)

	KeyToHexNibbleHashCached(mkKey(0xBB, 0), &c)
	require.Equal(t, byte(0xBB), c.addr[0])
	require.NotEqual(t, firstNibs, c.nibs)

	acctBefore := c.addr
	KeyToHexNibbleHashCached(make([]byte, length.Addr), &c)
	require.Equal(t, acctBefore, c.addr)

	c.reset()
	require.False(t, c.valid)
}

func TestUpdatesHashKey_MatchesHasher(t *testing.T) {
	t.Parallel()
	keys := [][]byte{
		{0x01, 0x02},
		make([]byte, length.Addr),
		func() []byte { k := make([]byte, 52); k[0], k[51] = 0x11, 0x22; return k }(),
	}
	for _, mode := range []Mode{ModeDirect, ModeUpdate, ModeParallel} {
		u := NewUpdates(mode, t.TempDir(), KeyToHexNibbleHash)
		require.True(t, u.addrCacheReuse, "cache must be enabled for the nibblizing hasher")
		for _, k := range keys {
			assert.Equal(t, KeyToHexNibbleHash(k), u.hashKey(k), "mode=%d key=%x", mode, k)
		}
	}
}

func TestHasherReusesAddrPrefix(t *testing.T) {
	t.Parallel()
	assert.True(t, hasherReusesAddrPrefix(KeyToHexNibbleHash))
	assert.False(t, hasherReusesAddrPrefix(keyHasherNoop))
}
