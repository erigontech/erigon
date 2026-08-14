package commitment

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/hexutil"
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
			"020d040906010f0e0803000401080b02000a070601050b0d060003030f0e01040100060a090303090500060c0c0905020c0b0b040e0d0007030a03000807030c"), nibblizedHashedKey)
}

func BenchmarkKeyToHexNibbleHash(b *testing.B) {
	key := make([]byte, 20)
	for i := range key {
		key[i] = byte(i)
	}
	for b.Loop() {
		KeyToHexNibbleHash(key)
	}
}
