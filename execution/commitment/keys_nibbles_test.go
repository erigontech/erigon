package commitment

import (
	"testing"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/stretchr/testify/require"
)

// 20 bytes account key -> 32 bytes keccak -> 64 bytes nibblelized
func TestAccountKey(t *testing.T) {
	t.Parallel()
	accKey := hexutil.MustDecode("0x00112233445566778899aabbccddeeff00112233") // keccak == 0xb7ff4d50bd18751616802a406c94b190f1a3fd4fc82b06db40943e0119c5e8bc
	nibblizedHashedKey := KeyToHexNibbleHash(accKey)

	require.Equal(t, hexutil.MustDecode("0x0b070f0f040d05000b0d01080705010601060800020a0400060c09040b0109000f010a030f0d040f0c08020b00060d0b04000904030e000101090c050e080b0c"), nibblizedHashedKey)
}

// 20 bytes account key | 32 bytes storage key-> 32 bytes keccak | 32 bytes keccak -> 128 bytes nibblelized
func TestStorageKey(t *testing.T) {
	t.Parallel()
	accKey := hexutil.MustDecode("0x00112233445566778899aabbccddeeff00112233")                             // keccak == 0xb7ff4d50bd18751616802a406c94b190f1a3fd4fc82b06db40943e0119c5e8bc
	storageKey := hexutil.MustDecode("0x00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff") // keccak == 0x2d4961fe830418b20a7615bd6033fe14106a9339506cc952cbb4ed073a30873c
	nibblizedHashedKey := KeyToHexNibbleHash(append(accKey, storageKey...))

	require.Equal(t, hexutil.MustDecode(
		"0x0b070f0f040d05000b0d01080705010601060800020a0400060c09040b0109000f010a030f0d040f0c08020b00060d0b04000904030e000101090c050e080b0c"+
			"020d040906010f0e0803000401080b02000a070601050b0d060003030f0e01040100060a090303090500060c0c0905020c0b0b040e0d0007030a03000807030c"), nibblizedHashedKey)
}
