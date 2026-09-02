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

// Drives the engine itself over the reference's root vectors, rather than the oracle.

type pbinEngineLeaf struct {
	treeKey  []byte
	plainKey []byte
	update   Update
}

// pbinLeafFromVector maps a raw (key, value) vector onto the Update the engine
// reads for that key's position: the engine rebuilds a leaf's value from Update
// fields, so a raw 32-byte value has to land on the field it will be read from.
func pbinLeafFromVector(key, value []byte, seq int) pbinEngineLeaf {
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
	// reserved sub-index with no defined packing.
	recordLeaf := func() {
		l.plainKey = nil
		l.update.Flags = StorageUpdate
		l.update.StorageLen = int8(copy(l.update.Storage[:], value))
	}

	if key[0] == pbinStorageZone {
		storageLeaf()
		return l
	}
	if key[0] == pbinCodeZone {
		recordLeaf()
		return l
	}
	switch sub := key[len(key)-1]; {
	case sub == pbinBasicDataLeafKey:
		l.plainKey = account
		l.update.Flags = BalanceUpdate | NonceUpdate
		l.update.CodeSize = uint64(binary.BigEndian.Uint32(value[pbinBasicDataCodeSizeOffset:]))
		l.update.Nonce = binary.BigEndian.Uint64(value[pbinBasicDataNonceOffset:])
		l.update.Balance = *new(uint256.Int).SetBytes(value[pbinBasicDataBalanceOffset:])
		return l
	case sub == pbinCodeHashLeafKey:
		l.plainKey = account
		l.update.Flags = CodeUpdate
		l.update.CodeHash = common.BytesToHash(value)
		return l
	case sub >= pbinHeaderStorageOffset && sub < pbinHeaderStorageOffset+pbinHeaderStorageSlots:
		storageLeaf()
		return l
	default:
		recordLeaf()
		return l
	}
}

func pbinSpecEngineRoot(t *testing.T, pph *PBinPatriciaHashed, tc pbinSpecTrieVector) string {
	t.Helper()
	leaves := make([]pbinEngineLeaf, len(tc.Entries))
	for i, e := range tc.Entries {
		leaves[i] = pbinLeafFromVector(pbinMustHex(t, e.Key), pbinMustHex(t, e.Value), i+1)
	}
	sort.Slice(leaves, func(i, j int) bool {
		return string(leaves[i].treeKey) < string(leaves[j].treeKey)
	})

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
	return hex.EncodeToString(got)
}

// Not parallel: it inspects engines coming out of the shared pool.
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
