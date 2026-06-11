package commitment

import (
	"testing"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// newAccountCell returns a cell loaded with the given account data at addr.
func newAccountCell(addr common.Address, acc *accounts.Account) *cell {
	c := new(cell)
	copy(c.accountAddr[:], addr[:])
	c.accountAddrLen = length.Addr
	c.Nonce = acc.Nonce
	c.Balance.Set(&acc.Balance)
	c.CodeHash = acc.CodeHash.Value()
	c.Flags = BalanceUpdate | NonceUpdate | CodeUpdate
	c.loaded = cellLoadAccount
	return c
}

// addStorageToCell loads the given storage slot and value into the cell, making it a
// singleton account+storage cell when account data is already present.
func addStorageToCell(c *cell, addr common.Address, loc common.Hash, val []byte) {
	copy(c.storageAddr[:length.Addr], addr[:])
	copy(c.storageAddr[length.Addr:], loc[:])
	c.storageAddrLen = length.Addr + length.Hash
	copy(c.Storage[:], val)
	c.StorageLen = int8(len(val))
	c.Flags |= StorageUpdate
	c.loaded |= cellLoadStorage
}

// encodeCellToBranch computes the cell hash at the given depth (memoizing its stateHash) and
// encodes the cell as a single-child branch at the given nibble.
func encodeCellToBranch(t *testing.T, hph *HexPatriciaHashed, c *cell, depth int16, nibble int) BranchData {
	t.Helper()

	_, err := hph.computeCellHash(c, depth, nil)
	require.NoError(t, err)
	require.True(t, c.stateHashLen > 0, "stateHash should be set after computeCellHash")

	touchMap := uint16(1 << nibble)
	var cellData [16]cellEncodeData
	cellData[nibble] = cellEncodeDataFromCell(c)
	branchData, err := NewBranchEncoder(1024).EncodeBranch(touchMap, touchMap, touchMap, &cellData)
	require.NoError(t, err)
	require.NotEmpty(t, branchData)
	return BranchData(branchData)
}

func TestVerifyBranchHashes_RoundTrip(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())

	addr := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	acc := accounts.Account{
		Nonce:    42,
		Balance:  *uint256.NewInt(1000000),
		CodeHash: accounts.EmptyCodeHash,
	}
	c := newAccountCell(addr, &acc)

	// Hash the account key to get the hashed extension.
	// In production, root branch is folded at depth=1 (branchKey has 0 nibbles, depth = 0+1).
	keccak := keccak.NewFastKeccak()
	depth := int16(1)
	hashBuf := make([]byte, length.Hash)
	require.NoError(t, c.hashAccKey(keccak, depth, hashBuf))
	c.hashedExtension[64-depth] = terminatorHexByte

	branchData := encodeCellToBranch(t, hph, c, depth, int(c.hashedExtension[0]))

	// Root branch: empty compacted path
	branchKey := nibbles.HexToCompact(nil)

	accountValues := map[string][]byte{
		string(addr[:]): accounts.SerialiseV3(&acc),
	}
	storageValues := map[string][]byte{}

	err := VerifyBranchHashes(branchKey, branchData, accountValues, storageValues)
	require.NoError(t, err)

	// Now corrupt the value and verify mismatch
	corruptedAcc := accounts.Account{
		Nonce:    43, // different nonce
		Balance:  *uint256.NewInt(1000000),
		CodeHash: accounts.EmptyCodeHash,
	}
	corruptedAccountValues := map[string][]byte{
		string(addr[:]): accounts.SerialiseV3(&corruptedAcc),
	}

	err = VerifyBranchHashes(branchKey, branchData, corruptedAccountValues, storageValues)
	require.Error(t, err)
	t.Logf("Expected mismatch error: %v", err)
}

func TestVerifyBranchHashes_Singleton(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())

	// Singleton cell with BOTH account and storage data at depth=2.
	addr := common.HexToAddress("0x4c888535841acbe0709b0758083f61d375bc02b4")
	loc := common.HexToHash("0x1df4f91929e18fda9e6f82e54e748e81e79e4bbd6fe34cdcba843ee8d63e8c4f")
	acc := accounts.Account{
		Nonce:    2,
		Balance:  *uint256.NewInt(2000),
		CodeHash: accounts.EmptyCodeHash,
	}
	storageVal := []byte{0x4c, 0x1d}
	c := newAccountCell(addr, &acc)
	addStorageToCell(c, addr, loc, storageVal)

	keccak := keccak.NewFastKeccak()
	depth := int16(2)

	// First hash storage key (offset 0 since depth < 64)
	hashBuf := make([]byte, length.Hash)
	require.NoError(t, c.hashStorageKey(keccak, length.Addr, 0, 0, hashBuf))
	c.hashedExtension[64] = terminatorHexByte

	// The nibble is determined by the hashed account key at depth=2
	require.NoError(t, c.hashAccKey(keccak, depth, hashBuf))
	nibble := int(c.hashedExtension[0])

	// hashAccKey overwrote the storage path in hashedExtension; restore it before hashing the cell
	require.NoError(t, c.hashStorageKey(keccak, length.Addr, 0, 0, hashBuf))
	c.hashedExtension[64] = terminatorHexByte

	branchData := encodeCellToBranch(t, hph, c, depth, nibble)

	// branchKey for depth=2: 1 nibble of the hashed account key path
	require.NoError(t, c.hashAccKey(keccak, 0, hashBuf))
	branchKey := nibbles.HexToCompact([]byte{c.hashedExtension[0]})

	accountValues := map[string][]byte{
		string(addr[:]): accounts.SerialiseV3(&acc),
	}
	storageValues := map[string][]byte{
		string(c.storageAddr[:c.storageAddrLen]): storageVal,
	}

	err := VerifyBranchHashes(branchKey, branchData, accountValues, storageValues)
	require.NoError(t, err)
}

// TestVerifyBranchHashes_SingletonDepth1 tests singleton at depth=1 (root branch).
// In production, the root branch is folded at depth=1 (branchKey has 0 nibbles).
func TestVerifyBranchHashes_SingletonDepth1(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())

	addr := common.HexToAddress("0x4c888535841acbe0709b0758083f61d375bc02b4")
	loc := common.HexToHash("0x1df4f91929e18fda9e6f82e54e748e81e79e4bbd6fe34cdcba843ee8d63e8c4f")
	acc := accounts.Account{
		Nonce:    2,
		Balance:  *uint256.NewInt(2000),
		CodeHash: accounts.EmptyCodeHash,
	}
	storageVal := []byte{0x4c, 0x1d}
	c := newAccountCell(addr, &acc)
	addStorageToCell(c, addr, loc, storageVal)

	keccak := keccak.NewFastKeccak()
	depth := int16(1)

	hashBuf := make([]byte, length.Hash)
	require.NoError(t, c.hashStorageKey(keccak, length.Addr, 0, 0, hashBuf))
	c.hashedExtension[64] = terminatorHexByte

	require.NoError(t, c.hashAccKey(keccak, depth, hashBuf))
	nibble := int(c.hashedExtension[0])

	require.NoError(t, c.hashStorageKey(keccak, length.Addr, 0, 0, hashBuf))
	c.hashedExtension[64] = terminatorHexByte

	branchData := encodeCellToBranch(t, hph, c, depth, nibble)

	// branchKey for root branch: empty path (0 nibbles → depth = 0 + 1 = 1)
	branchKey := nibbles.HexToCompact(nil)

	accountValues := map[string][]byte{
		string(addr[:]): accounts.SerialiseV3(&acc),
	}
	storageValues := map[string][]byte{
		string(c.storageAddr[:c.storageAddrLen]): storageVal,
	}

	err := VerifyBranchHashes(branchKey, branchData, accountValues, storageValues)
	require.NoError(t, err)
}

func TestVerifyBranchHashes_Storage(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())

	// Pure storage cell (depth >= 64).
	addr := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	loc := common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	storageVal := []byte{0x01, 0x02, 0x03}
	c := new(cell)
	addStorageToCell(c, addr, loc, storageVal)

	// Hash the storage key (depth=65 means non-singleton storage-only cell)
	keccak := keccak.NewFastKeccak()
	depth := int16(65)
	hashedKeyOffset := depth - 64
	hashBuf := make([]byte, length.Hash)
	require.NoError(t, c.hashStorageKey(keccak, length.Addr, 0, hashedKeyOffset, hashBuf))
	c.hashedExtension[64-hashedKeyOffset] = terminatorHexByte

	branchData := encodeCellToBranch(t, hph, c, depth, int(c.hashedExtension[0]))

	// branchKey for depth=65: 64 nibbles (hash of the account address), matching the
	// production storage branch at currentKeyLen=64.
	keccak.Reset()
	keccak.Write(addr[:])
	var hashBuf2 [32]byte
	keccak.Read(hashBuf2[:])
	var nib [64]byte
	for i := 0; i < 32; i++ {
		nib[i*2] = hashBuf2[i] >> 4
		nib[i*2+1] = hashBuf2[i] & 0x0f
	}
	branchKey := nibbles.HexToCompact(nib[:])

	storageValues := map[string][]byte{
		string(c.storageAddr[:c.storageAddrLen]): storageVal,
	}

	err := VerifyBranchHashes(branchKey, branchData, map[string][]byte{}, storageValues)
	require.NoError(t, err)
}
