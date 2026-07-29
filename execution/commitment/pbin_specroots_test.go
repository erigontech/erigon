package commitment

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"
)

// Root vectors exported from the EIP-8297 reference implementation in
// ethereum/execution-specs (branch projects/binary-trie), which hashes with
// BLAKE3. Replaying them under BLAKE3 checks this package's oracle against an
// implementation that was written independently and, more importantly, builds
// the tree by a different algorithm: the reference rebuilds canonically, the
// oracle inserts incrementally as the EIP's pseudocode does. Agreement across
// that difference is what rules out a shared misreading of the spec.
//
// The engine itself is tied to this oracle by the differential tests, so the
// chain reaches the engine even though the engine hashes with Keccak-256.

type pbinRootVectors struct {
	Meta      map[string]string `json:"meta"`
	EmptyRoot string            `json:"empty_root"`
	Trie      []struct {
		Name    string `json:"name"`
		Entries []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"entries"`
		Root string `json:"root"`
	} `json:"trie_vectors"`
	Sequences []struct {
		Seed int `json:"seed"`
		Ops  []struct {
			Op    string `json:"op"`
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"ops"`
		RootsAfter []string `json:"roots_after"`
	} `json:"sequence_vectors"`
}

func blake3Sum(b []byte) [32]byte { return blake3.Sum256(b) }

func loadPBinRootVectors(t *testing.T) pbinRootVectors {
	t.Helper()
	raw, err := os.ReadFile("testdata/eip8297_vectors.json")
	require.NoError(t, err)
	var v pbinRootVectors
	require.NoError(t, json.Unmarshal(raw, &v))
	require.Equal(t, "blake3", v.Meta["hasher"], "vectors are only replayable under the hash they were generated with")
	return v
}

// pbinOracleRootOf builds the oracle trie from a whole key set and merkelizes it
// under BLAKE3. Building from the surviving set is also how a delete is applied:
// the EIP's insert has no removal, and the reference's removal semantics are
// still open, so nothing here depends on a delete algorithm.
func pbinOracleRootOf(t *testing.T, entries map[string][]byte) [32]byte {
	t.Helper()
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	tree := &pbinOracleTree{}
	for _, k := range keys {
		tree.insert([]byte(k), entries[k])
	}
	return pbinOracleMerkelizeWith(tree.root, blake3Sum)
}

func TestPBinOracleMatchesSpecTrieRoots(t *testing.T) {
	t.Parallel()
	v := loadPBinRootVectors(t)
	require.NotEmpty(t, v.Trie)

	for _, tc := range v.Trie {
		t.Run(tc.Name, func(t *testing.T) {
			entries := make(map[string][]byte, len(tc.Entries))
			for _, e := range tc.Entries {
				key, err := hex.DecodeString(e.Key[2:])
				require.NoError(t, err)
				val, err := hex.DecodeString(e.Value[2:])
				require.NoError(t, err)
				entries[string(key)] = val
			}
			got := pbinOracleRootOf(t, entries)
			require.Equal(t, tc.Root[2:], hex.EncodeToString(got[:]))
		})
	}
}

// TestPBinOracleMatchesSpecSequenceRoots replays the reference's op sequences,
// checking the root after every operation rather than only at the end, so a
// divergence is pinned to the op that caused it.
func TestPBinOracleMatchesSpecSequenceRoots(t *testing.T) {
	t.Parallel()
	v := loadPBinRootVectors(t)
	require.NotEmpty(t, v.Sequences)

	checked := 0
	for _, seq := range v.Sequences {
		require.Len(t, seq.RootsAfter, len(seq.Ops))
		entries := make(map[string][]byte)
		for i, op := range seq.Ops {
			key, err := hex.DecodeString(op.Key[2:])
			require.NoError(t, err)
			switch op.Op {
			case "set":
				val, err := hex.DecodeString(op.Value[2:])
				require.NoError(t, err)
				entries[string(key)] = val
			case "delete":
				delete(entries, string(key))
			default:
				t.Fatalf("unknown op %q", op.Op)
			}
			got := pbinOracleRootOf(t, entries)
			require.Equal(t, seq.RootsAfter[i][2:], hex.EncodeToString(got[:]),
				"seed %d diverges at op %d (%s)", seq.Seed, i, op.Op)
			checked++
		}
	}
	t.Logf("replayed %d reference roots across %d sequences", checked, len(v.Sequences))
}
