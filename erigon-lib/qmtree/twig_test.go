package qmtree

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewBits(t *testing.T) {
	bits := ActiveBits{}
	for i := range 255 {
		require.Equal(t, byte(0), bits[i])
	}
}

func TestSetBit(t *testing.T) {
	bits := ActiveBits{}

	bits.SetBit(25)
	require.Equal(t, byte(0b00000010), bits[3])

	bits.SetBit(70)
	require.Equal(t, byte(0b01000000), bits[8])

	bits.SetBit(83)
	require.Equal(t, byte(0b00001000), bits[10])

	bits.SetBit(801)
	require.Equal(t, byte(0b00000010), bits[100])
}

func TestClearBits(t *testing.T) {
	bits := ActiveBits{}

	bits.SetBit(2047)
	bits.SetBit(2044)
	require.Equal(t, byte(0b10010000), bits[255])

	bits.ClearBit(2047)
	require.Equal(t, byte(0b00010000), bits[255])
	bits.ClearBit(2044)
	require.Equal(t, byte(0b00000000), bits[255])
}

func TestGetBit(t *testing.T) {
	bits := ActiveBits{}

	bits.SetBit(2047)
	bits.SetBit(2044)
	require.Equal(t, byte(0b10010000), bits[255])

	require.True(t, bits.GetBit(2047))
	require.False(t, bits.GetBit(2046))
	require.False(t, bits.GetBit(2045))
	require.True(t, bits.GetBit(2044))
	require.False(t, bits.GetBit(2043))
	require.False(t, bits.GetBit(2042))
	require.False(t, bits.GetBit(2041))
	require.False(t, bits.GetBit(2040))
}

func TestSetBitIdxOutOfRange(t *testing.T) {
	defer func() {
		x := recover()
		require.NotNil(t, x)
		require.Equal(t, "invalid id", x)
	}()

	bits := ActiveBits{}
	bits.SetBit(LEAF_COUNT_IN_TWIG + 1)
}

func TestClearBitIdxOutOfRange(t *testing.T) {
	defer func() {
		x := recover()
		require.NotNil(t, x)
		require.Equal(t, "invalid id", x)
	}()

	bits := ActiveBits{}
	bits.ClearBit(LEAF_COUNT_IN_TWIG + 1)
}

func TestGetBitIdxOutOfRange(t *testing.T) {
	defer func() {
		x := recover()
		require.NotNil(t, x)
		require.Equal(t, "invalid id", x)
	}()

	bits := ActiveBits{}
	bits.GetBit(LEAF_COUNT_IN_TWIG + 1)
}

func TestGetBits(t *testing.T) {
	bits := ActiveBits{}
	for i := range byte(255) {
		bits[i] = i
	}

	require.Equal(t,
		bits.GetBits(3, 32),
		[]byte{96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112,
			113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127})
}

func TestSync(t *testing.T) {
	var twig Twig
	var activeBits ActiveBits
	for i := range byte(255) {
		activeBits[i] = i
	}

	var hasher Sha256Hasher

	twig.syncL1(hasher, 0, activeBits)
	twig.syncL1(hasher, 1, activeBits)
	twig.syncL1(hasher, 2, activeBits)
	twig.syncL1(hasher, 3, activeBits)
	require.Equal(t,
		"ebdc6bccc0d70075f48ab3c602652a1787d41c05f5a0a851ffe479df0975e683",
		hex.EncodeToString(twig.activeBitsMtl1[0][:]))
	require.Equal(t,
		"3eac125482e6c5682c92af7dd633d9e99d027cf3f53237b46e2507ca2c9cd599",
		hex.EncodeToString(twig.activeBitsMtl1[1][:]))
	require.Equal(t,
		"e208457ddd8f66e95ea947bc1beb5c463de054daa3f0ae1c3682a973c1861a32",
		hex.EncodeToString(twig.activeBitsMtl1[2][:]))
	require.Equal(t,
		"e8b9fd47cce5df56b8d4b0b098af1b49ff3ea97d0c093c8ef6eccb34ae73ac8f",
		hex.EncodeToString(twig.activeBitsMtl1[3][:]))

	twig.syncL2(hasher, 0)
	twig.syncL2(hasher, 1)
	require.Equal(t,
		"cf1a0078d5a94742b42bf05d301919b5ae89c155fc1e68a08d260e7ec27c967e",
		hex.EncodeToString(twig.activeBitsMtl2[0][:]))
	require.Equal(t,
		"cf1a0078d5a94742b42bf05d301919b5ae89c155fc1e68a08d260e7ec27c967e",
		hex.EncodeToString(twig.activeBitsMtl2[0][:]))

	twig.syncL3(hasher)
	require.Equal(t,
		"d911c0d3beffe478f28b2ebc7cb824ad02ff2793534f37a0c6ddaf9d84527a66",
		hex.EncodeToString(twig.activeBitsMtl3[:]))

	twig.leftRoot = [32]byte{}
	for i := range 32 {
		twig.leftRoot[i] = 88
	}
	twig.syncTop(hasher)
	require.Equal(t,
		"9312922a448932555a5f1d07b98f422fc0a4259e450f7536161b8ef8ddc96e08",
		hex.EncodeToString(twig.twigRoot[:]))
}

func TestInitData(t *testing.T) {
	null_twigmt_hashes := []string{
		"cce5498796e1da850e39978e5e7bc572779e8ddc5eca8532aa8d28eb8b9fa839", // 1
		"6625f6aa53d328b2572979b52d98b376f26d86ead0fc89b386d4ed026e944e42", // 2
		"c6085473880d2de6339201f1855d088c7a7fc74ab884c5bcc8b851d202328646", // 4
		"730bf342c9b3d3e9a5ecd86d26d9bb3333a6038a110455bd98cae0b91284a50b", // 8
		"122c8ce9fa6aaa67e3afa2e1b47a704ad12c1e6608b2a21e84fd19bd07c30713", // 16
		"692f5b1dc974510438da37d0c46c8e39946a79af1246fe6fbc3f44fc80bc40c3", // 32
		"053d9c73883c8ee7eac9cf011458c61433bbd4bba561e3ddc3f49cf76e52e288", // 64
		"7c81680ffb753a36d9e0b345f308fd818a402b0ecb5e1366cc94991a56075044", // 128
		"e4c3f379b7a5789594c9109e9896aecf749b85c4a6a0b3d26a2c697e26f36fd3", // 256
		"065ac2fd5a856e8e35e104a78235fc5f8c7e75fabbf8064cda207c4babbeb56c", // 512
		"28c0cc1650e8b10b29de7eb17201be478391272380e55745fd52d5feb8554eaa", // 1024
		"ca2337691033ab0a24c10fbc70b49bea8c5978db1a0ec6510e7e97f528301c39", // 2048
	}

	var hasher Sha256Hasher

	for i := range 12 {
		require.Equal(t,
			null_twigmt_hashes[i],
			hex.EncodeToString(hasher.nullMtForTwig()[1<<i][:]))
	}

	twigRoot := hasher.nullTwig().twigRoot
	require.Equal(t,
		"37f6d34b5f4fe4aba10fd7411d6f58efc4bf844935c37dbe83c5686ceb62ce9d",
		hex.EncodeToString(twigRoot[:]))

	nullNode := hasher.nullNodeInHigerTree(63)
	require.Equal(t,
		"c787c83f6f8402c636a2f48f1bf2c02ceb31ea5ccdd4bd9e6fe6efcc3031b640",
		hex.EncodeToString(nullNode[:]))
}
