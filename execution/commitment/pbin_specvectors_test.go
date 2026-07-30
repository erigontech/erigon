package commitment

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

// Vectors exported from the EIP-8297 reference implementation in
// ethereum/execution-specs (branch projects/binary-trie), which hashes with
// BLAKE3. The BASIC_DATA packing involves no hash and is compared as-is; key
// derivation is replayed under BLAKE3 through the injectable seam, so the
// tree-key bodies compare in full.
type pbinSpecVectors struct {
	Meta      map[string]string `json:"meta"`
	BasicData []struct {
		CodeSize uint64 `json:"code_size"`
		Nonce    uint64 `json:"nonce"`
		Balance  string `json:"balance"`
		Value    string `json:"value"`
	} `json:"basic_data_vectors"`
	Embedding struct {
		Address      string `json:"address"`
		BasicDataKey string `json:"basic_data_key"`
		CodeHashKey  string `json:"code_hash_key"`
		// slot reaches 2**255, so it must not go through float64
		Slots []struct {
			Slot json.Number `json:"slot"`
			Key  string      `json:"key"`
		} `json:"slots"`
	} `json:"embedding_vectors"`
	Chunkify []pbinSpecChunkifyVector `json:"chunkify_vectors"`
}

type pbinSpecChunkifyVector struct {
	Name   string   `json:"name"`
	Code   string   `json:"code"`
	Chunks []string `json:"chunks"`
}

func loadPBinSpecVectors(t *testing.T) pbinSpecVectors {
	t.Helper()
	raw, err := os.ReadFile("testdata/eip8297_vectors.json")
	require.NoError(t, err)
	var v pbinSpecVectors
	require.NoError(t, json.Unmarshal(raw, &v))
	return v
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s[2:])
	require.NoError(t, err)
	return b
}

// TestPBinSpecBasicDataVectors is the one fully external check available under a
// different hash: BASIC_DATA packing is pure byte layout.
func TestPBinSpecBasicDataVectors(t *testing.T) {
	t.Parallel()
	v := loadPBinSpecVectors(t)
	require.NotEmpty(t, v.BasicData)

	for _, tc := range v.BasicData {
		bal, err := uint256.FromDecimal(tc.Balance)
		require.NoError(t, err)

		got, err := pbinEncodeBasicData(tc.Nonce, bal, tc.CodeSize)
		require.NoError(t, err)
		require.Equal(t, mustHex(t, tc.Value), got[:],
			"BASIC_DATA mismatch for code_size=%d nonce=%d balance=%s", tc.CodeSize, tc.Nonce, tc.Balance)
	}
}

// TestPBinSpecKeyRouting compares full tree keys against the reference under
// the BLAKE3 the vectors were generated with — zone, digest bodies and
// sub-index alike. Full equality through the production keyHasher seam is what
// proves no derivation step hashes outside it: a hardcoded Keccak site would
// diverge here (guards H3).
func TestPBinSpecKeyRouting(t *testing.T) {
	t.Parallel()
	v := loadPBinSpecVectors(t)
	addr := mustHex(t, v.Embedding.Address)
	require.Len(t, addr, 20)

	hasher := pbinKeyHasherWith(pbinBlake3Hash)
	require.Equal(t, mustHex(t, v.Embedding.BasicDataKey), hasher(addr), "BASIC_DATA key")

	c := pbinDigestCache{sum: pbinBlake3Hash}
	require.Equal(t, mustHex(t, v.Embedding.CodeHashKey), c.accountKey(addr, pbinCodeHashLeafKey), "CODE_HASH key")

	for _, s := range v.Embedding.Slots {
		slot, err := uint256.FromDecimal(s.Slot.String())
		require.NoError(t, err, "slot %s", s.Slot)
		slotBytes := slot.Bytes32()

		plainKey := append(append(make([]byte, 0, len(addr)+len(slotBytes)), addr...), slotBytes[:]...)
		require.Equal(t, mustHex(t, s.Key), hasher(plainKey), "slot %s key", s.Slot)
	}
}
