package commitment

import (
	"encoding/hex"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"

	"github.com/erigontech/erigon/common"
)

// Replays the reference's root vectors (see pbinSpecVectors) against the oracle
// under BLAKE3. The reference rebuilds the tree canonically while the oracle
// inserts incrementally as the EIP's pseudocode does, so agreement across the
// two algorithms is what rules out a shared misreading of the spec.

func pbinBlake3Sum(b []byte) [32]byte { return blake3.Sum256(b) }

var pbinBlake3Hash pbinHashFn = func(b []byte) common.Hash { return common.Hash(blake3.Sum256(b)) }

// pbinOracleRootOf rebuilds the oracle trie from the whole key set, so a removed
// key is simply one the set no longer holds and nothing here depends on an
// incremental delete algorithm.
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
	return pbinOracleMerkelizeWith(tree.root, pbinBlake3Sum)
}

func TestPBinOracleMatchesSpecTrieRoots(t *testing.T) {
	t.Parallel()
	v := pbinLoadSpecVectors(t)
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

// Checks the root after every op in a reference sequence, not only at the end,
// so a divergence pins to the op that caused it.
func TestPBinOracleMatchesSpecSequenceRoots(t *testing.T) {
	t.Parallel()
	v := pbinLoadSpecVectors(t)
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
