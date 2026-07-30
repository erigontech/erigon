package commitment

import (
	"encoding/binary"
	"encoding/hex"
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// Drives the engine itself over the reference's root vectors, rather than the
// oracle. The vectors carry raw tree keys and raw 32-byte values, while the
// engine rebuilds a leaf's value from an Update according to where the key sits,
// so each value has to be mapped back onto the field the engine will read.
//
// Every position the embedding defines is expressible, so the exclusion list is
// asserted empty: a vector the mapping cannot express fails this test rather
// than being skipped.

type pbinEngineLeaf struct {
	treeKey  []byte
	plainKey []byte
	update   Update
}

// pbinLeafFromVector maps a raw (key, value) pair onto the Update the engine
// reads for that key's position. ok is false when the position has no Update
// field to carry the value.
func pbinLeafFromVector(key, value []byte, seq int) (pbinEngineLeaf, bool) {
	var l pbinEngineLeaf
	l.treeKey = key

	// The plain key is synthetic: tree keys are digests and cannot be inverted.
	// Only its length is read, to decide which cell field holds it.
	account := make([]byte, length.Addr)
	binary.BigEndian.PutUint32(account, uint32(seq))
	storage := make([]byte, length.Addr+length.Hash)
	binary.BigEndian.PutUint32(storage, uint32(seq))

	storageLeaf := func() {
		l.plainKey = storage
		l.update.Flags = StorageUpdate
		l.update.StorageLen = int8(copy(l.update.Storage[:], value))
	}
	// A leaf carrying its own 32 bytes has no plain key: a code chunk, or a
	// sub-index the embedding has reserved and defined no packing for.
	recordLeaf := func() {
		l.plainKey = nil
		l.update.Flags = StorageUpdate
		l.update.StorageLen = int8(copy(l.update.Storage[:], value))
	}

	if key[0] == pbinStorageZone {
		storageLeaf()
		return l, true
	}
	if key[0] == pbinCodeZone {
		recordLeaf()
		return l, true
	}
	switch sub := key[len(key)-1]; {
	case sub == pbinBasicDataLeafKey:
		l.plainKey = account
		l.update.Flags = BalanceUpdate | NonceUpdate
		l.update.CodeSize = uint64(binary.BigEndian.Uint32(value[pbinBasicDataCodeSizeOffset:]))
		l.update.Nonce = binary.BigEndian.Uint64(value[pbinBasicDataNonceOffset:])
		l.update.Balance = *new(uint256.Int).SetBytes(value[pbinBasicDataBalanceOffset:])
		return l, true
	case sub == pbinCodeHashLeafKey:
		l.plainKey = account
		l.update.Flags = CodeUpdate
		l.update.CodeHash = common.BytesToHash(value)
		return l, true
	case sub >= pbinHeaderStorageOffset && sub < pbinCodeOffset:
		storageLeaf()
		return l, true
	default:
		recordLeaf()
		return l, true
	}
}

func TestPBinEngineMatchesSpecTrieRoots(t *testing.T) {
	t.Parallel()
	v := pbinLoadRootVectors(t)

	var ran, excluded []string
	for _, tc := range v.Trie {
		leaves := make([]pbinEngineLeaf, 0, len(tc.Entries))
		representable := true
		for i, e := range tc.Entries {
			key, err := hex.DecodeString(e.Key[2:])
			require.NoError(t, err)
			val, err := hex.DecodeString(e.Value[2:])
			require.NoError(t, err)
			l, ok := pbinLeafFromVector(key, val, i+1)
			if !ok {
				representable = false
				break
			}
			leaves = append(leaves, l)
		}
		if !representable {
			excluded = append(excluded, tc.Name)
			continue
		}
		ran = append(ran, tc.Name)

		t.Run(tc.Name, func(t *testing.T) {
			// tree-key order is the engine's visit invariant
			sort.Slice(leaves, func(i, j int) bool {
				return string(leaves[i].treeKey) < string(leaves[j].treeKey)
			})

			ms := NewMockState(t)
			pph := NewPBinPatriciaHashed(ms)
			pph.setHashSuite(pbinBlake3Hash)

			for i := range leaves {
				require.NoError(t, pph.followAndUpdate(leaves[i].treeKey, leaves[i].plainKey, &leaves[i].update),
					"insert %x", leaves[i].treeKey)
			}
			for pph.grid.activeRows > 0 {
				require.NoError(t, pph.fold())
			}
			require.NoError(t, pph.storeRoot())

			got, err := pph.RootHash()
			require.NoError(t, err)
			require.Equal(t, tc.Root[2:], hex.EncodeToString(got))
		})
	}

	t.Logf("engine ran %d/%d reference root vectors: %v", len(ran), len(v.Trie), ran)
	require.Empty(t, excluded, "every reference root vector must reproduce through the engine")
	require.Len(t, ran, len(v.Trie))
}

// TestPBinReleaseClearsHashSuite pins pooling hygiene: a released engine must
// come back on the Keccak default, not carrying a previous user's BLAKE3.
// Not parallel — it inspects a pooled object.
func TestPBinReleaseClearsHashSuite(t *testing.T) {
	pph := NewPBinPatriciaHashed(NewMockState(t))
	pph.setHashSuite(pbinBlake3Hash)
	require.NotNil(t, pph.hasher.sum)

	pph.Release()
	require.Nil(t, pph.hasher.sum, "Release must drop the hash override before pooling")

	reused := NewPBinPatriciaHashed(NewMockState(t))
	require.Nil(t, reused.hasher.sum, "a pooled engine must start on the Keccak default")
	reused.Release()
}
