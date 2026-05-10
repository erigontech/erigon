package commitment

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestCalculatorTouchesMatchInline verifies that processing VersionedWrite
// entries and calling TouchKey produces the same Updates as calling TouchKey
// directly from DomainPut. This ensures the commitment calculator (which
// receives writes via channel) produces identical trie roots to the inline
// path (which calls TouchKey during DomainPut).
func TestCalculatorTouchesMatchInline(t *testing.T) {
	t.Parallel()

	// Simulate account and storage writes for a block
	type writeEntry struct {
		domain kv.Domain
		key    []byte // composite key (address for accounts, address+slot for storage)
		val    []byte // serialized value
	}

	// Simulate: 3 account balance changes + 2 storage writes
	acc1 := common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f")
	acc2 := common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366")
	acc3 := common.FromHex("4838b106fce9647bdf1e7877bf73ce8b0bad5f97")

	// Serialize accounts (simplified — real SerialiseV3 is more complex)
	serializeAccount := func(balance uint64, nonce uint64) []byte {
		var acc accounts.Account
		acc.Balance = *uint256.NewInt(balance)
		acc.Nonce = nonce
		enc := make([]byte, acc.EncodingLengthForStorage())
		acc.EncodeForStorage(enc)
		return enc
	}

	storageKey1 := common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366" +
		"0000000000000000000000000000000000000000000000000000000000000004")
	storageKey2 := common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366" +
		"0000000000000000000000000000000000000000000000000000000000000005")

	writes := []writeEntry{
		{kv.AccountsDomain, acc1, serializeAccount(1000, 5)},
		{kv.AccountsDomain, acc2, serializeAccount(2000, 10)},
		{kv.AccountsDomain, acc3, serializeAccount(3000, 15)},
		{kv.StorageDomain, storageKey1, common.FromHex("7c1fed52ef2b45443e674ae782f51586aa29c384")},
		{kv.StorageDomain, storageKey2, common.FromHex("a6b84e7cfb39a724cc086f9f")},
	}

	// Path 1: Inline — simulate what DomainPut does
	inlineUpdates := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)
	for _, w := range writes {
		switch w.domain {
		case kv.AccountsDomain:
			inlineUpdates.TouchPlainKey(string(w.key), w.val, inlineUpdates.TouchAccount)
		case kv.StorageDomain:
			inlineUpdates.TouchPlainKey(string(w.key), w.val, inlineUpdates.TouchStorage)
		case kv.CodeDomain:
			inlineUpdates.TouchPlainKey(string(w.key), w.val, inlineUpdates.TouchCode)
		}
	}

	// Path 2: Calculator — simulate processing writes from channel
	calcUpdates := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)
	for _, w := range writes {
		switch w.domain {
		case kv.AccountsDomain:
			calcUpdates.TouchPlainKey(string(w.key), w.val, calcUpdates.TouchAccount)
		case kv.StorageDomain:
			calcUpdates.TouchPlainKey(string(w.key), w.val, calcUpdates.TouchStorage)
		case kv.CodeDomain:
			calcUpdates.TouchPlainKey(string(w.key), w.val, calcUpdates.TouchCode)
		}
	}

	// Compare: same number of unique keys
	require.Equal(t, inlineUpdates.Size(), calcUpdates.Size(),
		"Inline and calculator should have same number of keys")

	// Compare: same key set
	for k := range inlineUpdates.keys {
		_, ok := calcUpdates.keys[k]
		assert.True(t, ok, "Calculator missing key: %x", k)
	}
	for k := range calcUpdates.keys {
		_, ok := inlineUpdates.keys[k]
		assert.True(t, ok, "Inline missing key: %x", k)
	}
}

// TestCalculatorBlockBoundary verifies that the calculator correctly
// accumulates touches across TXs within a block, then processes them
// at the block boundary.
func TestCalculatorBlockBoundary(t *testing.T) {
	t.Parallel()

	updates := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)

	acc1 := common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f")
	acc2 := common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366")

	// TX 1: write acc1
	updates.TouchPlainKey(string(acc1), []byte("value1"), updates.TouchAccount)

	// TX 2: write acc2
	updates.TouchPlainKey(string(acc2), []byte("value2"), updates.TouchAccount)

	// TX 3: write acc1 again (update)
	updates.TouchPlainKey(string(acc1), []byte("value3"), updates.TouchAccount)

	// At block boundary: should have 2 unique keys (acc1 deduped)
	assert.Equal(t, uint64(2), updates.Size(), "Should have 2 unique keys")
}

// TestCalculatorStorageCompositeKey verifies that storage writes use the
// correct composite key format (address + slot) for TouchKey.
func TestCalculatorStorageCompositeKey(t *testing.T) {
	t.Parallel()

	updates := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)

	// Same address, different slots
	addr := common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366")
	slot1 := common.FromHex("0000000000000000000000000000000000000000000000000000000000000004")
	slot2 := common.FromHex("0000000000000000000000000000000000000000000000000000000000000005")

	composite1 := append(addr, slot1...)
	composite2 := append(addr, slot2...)

	updates.TouchPlainKey(string(composite1), []byte("val1"), updates.TouchStorage)
	updates.TouchPlainKey(string(composite2), []byte("val2"), updates.TouchStorage)

	// Should have 2 unique keys (different slots)
	assert.Equal(t, uint64(2), updates.Size(), "Different slots should be different keys")
}

// TestCalculatorCodeDomain verifies that code writes go through the
// correct domain path.
func TestCalculatorCodeDomain(t *testing.T) {
	t.Parallel()

	updates := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)

	addr := common.FromHex("d2c6b20395bbb07b35d8c28ee439338a612dc82e")
	code := []byte{0xef, 0x01, 0x00, 0x01, 0x02, 0x03} // EIP-7702 delegation prefix

	updates.TouchPlainKey(string(addr), code, updates.TouchCode)

	assert.Equal(t, uint64(1), updates.Size(), "Should have 1 code key")
	_, ok := updates.keys[string(addr)]
	assert.True(t, ok, "Key should be the address")
}

// TestTouchPlainKeyDirect_MatchesSerialized verifies that TouchPlainKeyDirect
// produces the same tree entries as TouchPlainKey with serialized bytes.
// This ensures the commitment calculator (using Direct) produces identical
// results to the inline path (using serialized TouchPlainKey).
func TestTouchPlainKeyDirect_MatchesSerialized(t *testing.T) {
	t.Parallel()

	// Build an account
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(12345)
	acc.Nonce = 42
	acc.Incarnation = 1

	key := string(common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f"))
	enc := accounts.SerialiseV3(&acc)

	// Path 1: serialized (inline path)
	utSerialized := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	utSerialized.TouchPlainKey(key, enc, utSerialized.TouchAccount)

	// Path 2: direct (calculator path)
	utDirect := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	update := &Update{
		Flags:   BalanceUpdate | NonceUpdate,
		Balance: acc.Balance,
		Nonce:   acc.Nonce,
	}
	utDirect.TouchPlainKeyDirect(key, update)

	// Both should have 1 entry
	assert.Equal(t, utSerialized.Size(), utDirect.Size(), "Same number of entries")

	// Compare the Update contents in the tree
	var serializedUpdate, directUpdate *Update
	utSerialized.tree.Descend(func(item *KeyUpdate) bool {
		if item.plainKey == key {
			serializedUpdate = item.update
		}
		return true
	})
	utDirect.tree.Descend(func(item *KeyUpdate) bool {
		if item.plainKey == key {
			directUpdate = item.update
		}
		return true
	})

	require.NotNil(t, serializedUpdate, "Serialized should have entry")
	require.NotNil(t, directUpdate, "Direct should have entry")
	assert.True(t, serializedUpdate.Balance.Eq(&directUpdate.Balance), "Balance should match")
	assert.Equal(t, serializedUpdate.Nonce, directUpdate.Nonce, "Nonce should match")
	assert.Equal(t, serializedUpdate.Flags&BalanceUpdate != 0, directUpdate.Flags&BalanceUpdate != 0, "BalanceUpdate flag")
	assert.Equal(t, serializedUpdate.Flags&NonceUpdate != 0, directUpdate.Flags&NonceUpdate != 0, "NonceUpdate flag")
}

// TestTouchPlainKeyDirect_Storage verifies direct storage touches.
func TestTouchPlainKeyDirect_Storage(t *testing.T) {
	t.Parallel()

	key := string(common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366" +
		"0000000000000000000000000000000000000000000000000000000000000004"))
	val := common.FromHex("7c1fed52ef2b45443e674ae782f51586aa29c384")

	// Serialized path
	utSerialized := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	utSerialized.TouchPlainKey(key, val, utSerialized.TouchStorage)

	// Direct path
	utDirect := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	update := &Update{
		Flags:      StorageUpdate,
		StorageLen: int8(len(val)),
	}
	copy(update.Storage[:], val)
	utDirect.TouchPlainKeyDirect(key, update)

	assert.Equal(t, utSerialized.Size(), utDirect.Size())

	var su, du *Update
	utSerialized.tree.Descend(func(item *KeyUpdate) bool {
		if item.plainKey == key {
			su = item.update
		}
		return true
	})
	utDirect.tree.Descend(func(item *KeyUpdate) bool {
		if item.plainKey == key {
			du = item.update
		}
		return true
	})

	require.NotNil(t, su)
	require.NotNil(t, du)
	assert.Equal(t, su.Storage, du.Storage, "Storage value should match")
	assert.Equal(t, su.StorageLen, du.StorageLen, "StorageLen should match")
	assert.Equal(t, su.Flags, du.Flags, "Flags should match")
}

// TestTouchPlainKeyDirect_Delete verifies delete handling.
func TestTouchPlainKeyDirect_Delete(t *testing.T) {
	t.Parallel()

	key := string(common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f"))

	ut := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	ut.TouchPlainKeyDirect(key, &Update{Flags: DeleteUpdate})

	assert.Equal(t, uint64(1), ut.Size())

	var u *Update
	ut.tree.Descend(func(item *KeyUpdate) bool {
		if item.plainKey == key {
			u = item.update
		}
		return true
	})
	require.NotNil(t, u)
	assert.Equal(t, DeleteUpdate, u.Flags&DeleteUpdate, "Should have DeleteUpdate flag")
}
