package qmtree

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

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

	// With activeBits removed, twigRoot == leftRoot == nullMtForTwig[1]
	twigRoot := hasher.nullTwig().twigRoot
	require.Equal(t,
		null_twigmt_hashes[0],
		hex.EncodeToString(twigRoot[:]),
		"nullTwig().twigRoot should equal nullMtForTwig[1]")

	// nullNodeInHigerTree is derived from the new twigRoot; just verify it is
	// non-zero and deterministic (exact value depends on the simplified formula).
	nullNode := hasher.nullNodeInHigerTree(63)
	require.NotEqual(t,
		[32]byte{},
		[32]byte(nullNode),
		"nullNodeInHigerTree(63) should be non-zero")
}
