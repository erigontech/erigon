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
// ethereum/execution-specs (branch projects/binary-trie). The reference hashes
// with BLAKE3 and this engine with Keccak-256, so every digest-bearing vector —
// the trie roots and the tree-key bodies — cannot be compared directly. What
// survives the hash difference is checked here: the BASIC_DATA packing, which
// involves no hash at all, and the zone/length/sub-index routing, which is
// positional.
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

// TestPBinSpecKeyRouting checks the positional half of key derivation against
// the reference: which zone a key lands in, how long it is, and which sub-index
// it carries. The 32-byte digest bodies differ by hash and are not compared.
func TestPBinSpecKeyRouting(t *testing.T) {
	t.Parallel()
	v := loadPBinSpecVectors(t)
	addr := mustHex(t, v.Embedding.Address)
	require.Len(t, addr, 20)

	var c pbinDigestCache

	header := mustHex(t, v.Embedding.BasicDataKey)
	got := c.accountKey(addr, pbinBasicDataLeafKey)
	require.Len(t, got, len(header))
	require.Equal(t, header[0], got[0], "account zone byte")
	require.Equal(t, header[len(header)-1], got[len(got)-1], "BASIC_DATA sub-index")

	codeHash := mustHex(t, v.Embedding.CodeHashKey)
	got = c.accountKey(addr, pbinCodeHashLeafKey)
	require.Equal(t, codeHash[len(codeHash)-1], got[len(got)-1], "CODE_HASH sub-index")

	for _, s := range v.Embedding.Slots {
		slot, err := uint256.FromDecimal(s.Slot.String())
		require.NoError(t, err, "slot %s", s.Slot)
		want := mustHex(t, s.Key)
		slotBytes := slot.Bytes32()

		got := c.storageKey(addr, slotBytes[:])
		require.Len(t, got, len(want), "slot %s key length", s.Slot)
		require.Equal(t, want[0], got[0], "slot %s zone byte", s.Slot)
		require.Equal(t, want[len(want)-1], got[len(got)-1], "slot %s sub-index", s.Slot)
	}
}
