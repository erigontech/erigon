package commitment

import (
	"encoding/hex"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon/common"
	cryptokeccak "github.com/erigontech/erigon/common/crypto/keccak"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestVerifyBranchHashes_RoundTrip(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil)

	// Build a cell with account data.
	c := new(cell)
	addr := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	copy(c.accountAddr[:], addr[:])
	c.accountAddrLen = length.Addr

	acc := accounts.Account{
		Nonce:    42,
		Balance:  *uint256.NewInt(1000000),
		CodeHash: accounts.EmptyCodeHash,
	}
	c.Nonce = acc.Nonce
	c.Balance.Set(&acc.Balance)
	c.CodeHash = acc.CodeHash.Value()
	c.Flags = BalanceUpdate | NonceUpdate | CodeUpdate
	c.loaded = cellLoadAccount

	// Hash the account key to get the hashed extension.
	// In production, root branch is folded at depth=1 (branchKey has 0 nibbles, depth = 0+1).
	keccak := sha3.NewLegacyKeccak256().(cryptokeccak.KeccakState)
	depth := int16(1) // root branch fold depth
	hashBuf := make([]byte, length.Hash)
	if err := c.hashAccKey(keccak, depth, hashBuf); err != nil {
		t.Fatal(err)
	}
	c.hashedExtension[64-depth] = terminatorHexByte

	// Compute the cell hash
	hph.memoizationOff = false // allow memoization
	hash, err := hph.computeCellHash(c, depth, nil)
	require.NoError(t, err)
	require.True(t, c.stateHashLen > 0, "stateHash should be set after computeCellHash")

	t.Logf("stateHash: %x (len=%d)", c.stateHash[:c.stateHashLen], c.stateHashLen)
	t.Logf("computed hash: %x", hash)

	// Now encode this cell into branch data using EncodeBranch.
	encoder := NewBranchEncoder(1024)
	// First nibble of the hashed key at depth=1 starts from index 1
	// But the cell's hashedExtension was filled from depth=1, so [0] is the first extension nibble
	nibble := int(c.hashedExtension[0]) // first nibble of the hashed extension
	touchMap := uint16(1 << nibble)
	afterMap := touchMap

	row := [16]*cell{}
	row[nibble] = c
	readCell := func(nib int, skip bool) (*cell, error) {
		return row[nib], nil
	}
	branchData, _, err := encoder.EncodeBranch(touchMap, afterMap, touchMap, readCell)
	require.NoError(t, err)
	require.True(t, len(branchData) > 0)

	// Build the branchKey (compacted nibbles for depth=0 means empty path)
	branchKey := HexNibblesToCompactBytes(nil) // empty path

	// Serialize the account value
	accVal := accounts.SerialiseV3(&acc)

	// Verify the branch hashes
	accountValues := map[string][]byte{
		hex.EncodeToString(addr[:]): accVal,
	}
	storageValues := map[string][]byte{}

	err = VerifyBranchHashes(branchKey, BranchData(branchData), accountValues, storageValues)
	require.NoError(t, err)

	// Now corrupt the value and verify mismatch
	corruptedAcc := accounts.Account{
		Nonce:    43, // different nonce
		Balance:  *uint256.NewInt(1000000),
		CodeHash: accounts.EmptyCodeHash,
	}
	corruptedVal := accounts.SerialiseV3(&corruptedAcc)
	corruptedAccountValues := map[string][]byte{
		hex.EncodeToString(addr[:]): corruptedVal,
	}

	err = VerifyBranchHashes(branchKey, BranchData(branchData), corruptedAccountValues, storageValues)
	require.Error(t, err)
	t.Logf("Expected mismatch error: %v", err)
}

func TestVerifyBranchHashes_Singleton(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil)

	// Build a singleton cell with BOTH account and storage data at depth=2.
	c := new(cell)
	addr := common.HexToAddress("0x4c888535841acbe0709b0758083f61d375bc02b4")
	loc := common.HexToHash("0x1df4f91929e18fda9e6f82e54e748e81e79e4bbd6fe34cdcba843ee8d63e8c4f")
	copy(c.accountAddr[:], addr[:])
	c.accountAddrLen = length.Addr
	copy(c.storageAddr[:length.Addr], addr[:])
	copy(c.storageAddr[length.Addr:], loc[:])
	c.storageAddrLen = length.Addr + length.Hash

	acc := accounts.Account{
		Nonce:    2,
		Balance:  *uint256.NewInt(2000),
		CodeHash: accounts.EmptyCodeHash,
	}
	c.Nonce = acc.Nonce
	c.Balance.Set(&acc.Balance)
	c.CodeHash = acc.CodeHash.Value()
	c.Flags = BalanceUpdate | NonceUpdate | CodeUpdate | StorageUpdate
	c.loaded = cellLoadAccount | cellLoadStorage

	storageVal := []byte{0x4c, 0x1d}
	copy(c.Storage[:], storageVal)
	c.StorageLen = int8(len(storageVal))

	// Hash keys at depth=2
	keccak := sha3.NewLegacyKeccak256().(cryptokeccak.KeccakState)
	depth := int16(2)

	// First hash storage key
	hashedKeyOffset := int16(0)
	if depth >= 64 {
		hashedKeyOffset = depth - 64
	}
	hashBuf := make([]byte, length.Hash)
	if err := c.hashStorageKey(keccak, length.Addr, 0, hashedKeyOffset, hashBuf); err != nil {
		t.Fatal(err)
	}
	c.hashedExtension[64-hashedKeyOffset] = terminatorHexByte

	// Compute storage leaf hash (as part of computeCellHash for singleton)
	hph.memoizationOff = false
	hash, err := hph.computeCellHash(c, depth, nil)
	require.NoError(t, err)
	require.True(t, c.stateHashLen > 0, "stateHash should be set for singleton")

	t.Logf("singleton stateHash: %x (len=%d)", c.stateHash[:c.stateHashLen], c.stateHashLen)
	t.Logf("computed hash: %x", hash)

	// Encode into branch data
	encoder := NewBranchEncoder(1024)
	// The nibble is determined by the hashed account key at depth=2
	if err := c.hashAccKey(keccak, depth, hashBuf); err != nil {
		t.Fatal(err)
	}
	nibble := int(c.hashedExtension[0])
	touchMap := uint16(1 << nibble)
	afterMap := touchMap

	row := [16]*cell{}
	row[nibble] = c
	readCell := func(nib int, skip bool) (*cell, error) {
		return row[nib], nil
	}
	branchData, _, err := encoder.EncodeBranch(touchMap, afterMap, touchMap, readCell)
	require.NoError(t, err)

	// branchKey for depth=2: depth-1 = 1 nibble of the hashed account key path
	if err := c.hashAccKey(keccak, 0, hashBuf); err != nil {
		t.Fatal(err)
	}
	branchNibbles := []byte{c.hashedExtension[0]}
	branchKey := HexNibblesToCompactBytes(branchNibbles)

	// Build domain values maps
	accVal := accounts.SerialiseV3(&acc)
	accountValues := map[string][]byte{
		hex.EncodeToString(addr[:]): accVal,
	}
	stoKey := hex.EncodeToString(c.storageAddr[:c.storageAddrLen])
	storageValues := map[string][]byte{
		stoKey: storageVal,
	}

	err = VerifyBranchHashes(branchKey, BranchData(branchData), accountValues, storageValues)
	require.NoError(t, err)
}

// TestVerifyBranchHashes_SingletonDepth1 tests singleton at depth=1 (root branch).
// In production, the root branch is folded at depth=1 (branchKey has 0 nibbles).
func TestVerifyBranchHashes_SingletonDepth1(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil)

	c := new(cell)
	addr := common.HexToAddress("0x4c888535841acbe0709b0758083f61d375bc02b4")
	loc := common.HexToHash("0x1df4f91929e18fda9e6f82e54e748e81e79e4bbd6fe34cdcba843ee8d63e8c4f")
	copy(c.accountAddr[:], addr[:])
	c.accountAddrLen = length.Addr
	copy(c.storageAddr[:length.Addr], addr[:])
	copy(c.storageAddr[length.Addr:], loc[:])
	c.storageAddrLen = length.Addr + length.Hash

	acc := accounts.Account{
		Nonce:    2,
		Balance:  *uint256.NewInt(2000),
		CodeHash: accounts.EmptyCodeHash,
	}
	c.Nonce = acc.Nonce
	c.Balance.Set(&acc.Balance)
	c.CodeHash = acc.CodeHash.Value()
	c.Flags = BalanceUpdate | NonceUpdate | CodeUpdate | StorageUpdate
	c.loaded = cellLoadAccount | cellLoadStorage

	storageVal := []byte{0x4c, 0x1d}
	copy(c.Storage[:], storageVal)
	c.StorageLen = int8(len(storageVal))

	// Root branch is folded at depth=1 (branchKey has 0 nibbles, depth = 0 + 1 = 1)
	keccak := sha3.NewLegacyKeccak256().(cryptokeccak.KeccakState)
	depth := int16(1)

	// First hash storage key
	hashedKeyOffset := int16(0)
	hashBuf := make([]byte, length.Hash)
	if err := c.hashStorageKey(keccak, length.Addr, 0, hashedKeyOffset, hashBuf); err != nil {
		t.Fatal(err)
	}
	c.hashedExtension[64-hashedKeyOffset] = terminatorHexByte

	// Compute hash at depth=1
	hph.memoizationOff = false
	hash, err := hph.computeCellHash(c, depth, nil)
	require.NoError(t, err)
	require.True(t, c.stateHashLen > 0, "stateHash should be set for singleton")

	t.Logf("singleton depth=1 stateHash: %x (len=%d)", c.stateHash[:c.stateHashLen], c.stateHashLen)
	t.Logf("computed hash: %x", hash)

	// Encode into branch data
	encoder := NewBranchEncoder(1024)
	if err := c.hashAccKey(keccak, depth, hashBuf); err != nil {
		t.Fatal(err)
	}
	nibble := int(c.hashedExtension[0])
	touchMap := uint16(1 << nibble)
	afterMap := touchMap

	row := [16]*cell{}
	row[nibble] = c
	readCell := func(nib int, skip bool) (*cell, error) {
		return row[nib], nil
	}
	branchData, _, err := encoder.EncodeBranch(touchMap, afterMap, touchMap, readCell)
	require.NoError(t, err)

	// branchKey for root branch: empty path (0 nibbles → depth = 0 + 1 = 1)
	branchKey := HexNibblesToCompactBytes(nil)

	// Build domain values maps
	accVal := accounts.SerialiseV3(&acc)
	accountValues := map[string][]byte{
		hex.EncodeToString(addr[:]): accVal,
	}
	stoKey := hex.EncodeToString(c.storageAddr[:c.storageAddrLen])
	storageValues := map[string][]byte{
		stoKey: storageVal,
	}

	err = VerifyBranchHashes(branchKey, BranchData(branchData), accountValues, storageValues)
	require.NoError(t, err)
}

func TestVerifyBranchHashes_Storage(t *testing.T) {
	t.Parallel()

	hph := NewHexPatriciaHashed(length.Addr, nil)

	// Build a cell with storage data (depth >= 64 = pure storage cell).
	c := new(cell)
	addr := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	loc := common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	copy(c.storageAddr[:length.Addr], addr[:])
	copy(c.storageAddr[length.Addr:], loc[:])
	c.storageAddrLen = length.Addr + length.Hash

	storageVal := []byte{0x01, 0x02, 0x03}
	copy(c.Storage[:], storageVal)
	c.StorageLen = int8(len(storageVal))
	c.loaded = cellLoadStorage

	// Hash the storage key (depth=65 means non-singleton storage-only cell)
	keccak := sha3.NewLegacyKeccak256().(cryptokeccak.KeccakState)
	depth := int16(65)
	hashedKeyOffset := depth - 64
	hashBuf := make([]byte, length.Hash)
	if err := c.hashStorageKey(keccak, length.Addr, 0, hashedKeyOffset, hashBuf); err != nil {
		t.Fatal(err)
	}
	c.hashedExtension[64-hashedKeyOffset] = terminatorHexByte

	hph.memoizationOff = false
	hash, err := hph.computeCellHash(c, depth, nil)
	require.NoError(t, err)
	require.True(t, c.stateHashLen > 0)

	t.Logf("stateHash: %x (len=%d)", c.stateHash[:c.stateHashLen], c.stateHashLen)
	t.Logf("computed hash: %x", hash)

	// Encode cell into branch data
	encoder := NewBranchEncoder(1024)
	nibble := int(c.hashedExtension[0])
	touchMap := uint16(1 << nibble)
	afterMap := touchMap

	row := [16]*cell{}
	row[nibble] = c
	readCell := func(nib int, skip bool) (*cell, error) {
		return row[nib], nil
	}
	branchData, _, err := encoder.EncodeBranch(touchMap, afterMap, touchMap, readCell)
	require.NoError(t, err)

	// Build branchKey for depth=65: depth-1 = 64 nibbles (hash of account address).
	// In production, the storage branch at depth=65 has currentKeyLen=64.
	keccak.Reset()
	keccak.Write(addr[:])
	var hashBuf2 [32]byte
	keccak.Read(hashBuf2[:])
	var nibbles [64]byte
	for i := 0; i < 32; i++ {
		nibbles[i*2] = hashBuf2[i] >> 4
		nibbles[i*2+1] = hashBuf2[i] & 0x0f
	}
	branchKey := HexNibblesToCompactBytes(nibbles[:])

	// Build domain values map
	stoKey := hex.EncodeToString(c.storageAddr[:c.storageAddrLen])
	storageValues := map[string][]byte{
		stoKey: storageVal,
	}

	err = VerifyBranchHashes(branchKey, BranchData(branchData), map[string][]byte{}, storageValues)
	require.NoError(t, err)
}
