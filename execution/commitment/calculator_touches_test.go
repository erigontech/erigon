package commitment

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestCalculatorBlockBoundary(t *testing.T) {
	t.Parallel()

	updates := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)

	acc1 := common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f")
	acc2 := common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366")

	updates.TouchPlainKey(string(acc1), []byte("value1"), updates.TouchAccount)
	updates.TouchPlainKey(string(acc2), []byte("value2"), updates.TouchAccount)
	updates.TouchPlainKey(string(acc1), []byte("value3"), updates.TouchAccount)

	assert.Equal(t, uint64(2), updates.Size(), "Should have 2 unique keys")
}

func TestTouchKeyIdempotent(t *testing.T) {
	t.Parallel()

	key := common.FromHex("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
	acc := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(1000), Incarnation: 1}
	val := accounts.SerialiseV3(&acc)

	single := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	single.TouchPlainKey(string(key), val, single.TouchAccount)

	double := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	double.TouchPlainKey(string(key), val, double.TouchAccount)
	double.TouchPlainKey(string(key), val, double.TouchAccount)

	assert.Equal(t, single.Size(), double.Size(),
		"Double touch should produce same key count as single touch")
}

func TestCalculatorStorageCompositeKey(t *testing.T) {
	t.Parallel()

	updates := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)

	addr := common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366")
	slot1 := common.FromHex("0000000000000000000000000000000000000000000000000000000000000004")
	slot2 := common.FromHex("0000000000000000000000000000000000000000000000000000000000000005")

	composite1 := make([]byte, 0, len(addr)+len(slot1))
	composite1 = append(composite1, addr...)
	composite1 = append(composite1, slot1...)

	composite2 := make([]byte, 0, len(addr)+len(slot2))
	composite2 = append(composite2, addr...)
	composite2 = append(composite2, slot2...)

	updates.TouchPlainKey(string(composite1), []byte("val1"), updates.TouchStorage)
	updates.TouchPlainKey(string(composite2), []byte("val2"), updates.TouchStorage)

	assert.Equal(t, uint64(2), updates.Size(), "Different slots should be different keys")
}

func TestCalculatorCodeDomain(t *testing.T) {
	t.Parallel()

	updates := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)

	addr := common.FromHex("d2c6b20395bbb07b35d8c28ee439338a612dc82e")
	// 0xef 0x01 is the EIP-7702 delegation designator prefix, not arbitrary bytes.
	code := []byte{0xef, 0x01, 0x00, 0x01, 0x02, 0x03}

	updates.TouchPlainKey(string(addr), code, updates.TouchCode)

	assert.Equal(t, uint64(1), updates.Size(), "Should have 1 code key")
	_, ok := updates.keys[string(addr)]
	assert.True(t, ok, "Key should be the address")
}

func TestTouchKeyDomainMapping(t *testing.T) {
	t.Parallel()

	updates := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)

	accKey := common.FromHex("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
	acc := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(1000)}
	accVal := accounts.SerialiseV3(&acc)
	updates.TouchPlainKey(string(accKey), accVal, updates.TouchAccount)

	stgKey := common.FromHex("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2" + "0000000000000000000000000000000000000000000000000000000000000001")
	updates.TouchPlainKey(string(stgKey), []byte{0x42}, updates.TouchStorage)

	codeKey := common.FromHex("d2c6b20395bbb07b35d8c28ee439338a612dc82e")
	updates.TouchPlainKey(string(codeKey), []byte{0xef, 0x01, 0x00}, updates.TouchCode)

	require.Equal(t, uint64(3), updates.Size(), "Should have 3 touched keys")
}

func TestTouchKey_AccountAndCodeShareKey(t *testing.T) {
	t.Parallel()

	addr := common.FromHex("c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
	slot := common.FromHex("0000000000000000000000000000000000000000000000000000000000000004")
	stgKey := append(bytes.Clone(addr), slot...)

	updates := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	acc := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(100)}
	updates.TouchPlainKey(string(addr), accounts.SerialiseV3(&acc), updates.TouchAccount)
	updates.TouchPlainKey(string(stgKey), []byte{0x42}, updates.TouchStorage)
	updates.TouchPlainKey(string(addr), []byte{0xef}, updates.TouchCode)

	assert.Equal(t, uint64(2), updates.Size(), "Should have 2 unique key entries (addr merged, addr+slot)")
}

func TestTouchPlainKeyDirect_MatchesSerialized(t *testing.T) {
	t.Parallel()

	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(12345)
	acc.Nonce = 42
	acc.Incarnation = 1

	key := string(common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f"))
	enc := accounts.SerialiseV3(&acc)

	utSerialized := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	utSerialized.TouchPlainKey(key, enc, utSerialized.TouchAccount)

	utDirect := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	update := &Update{
		Flags:   BalanceUpdate | NonceUpdate,
		Balance: acc.Balance,
		Nonce:   acc.Nonce,
	}
	utDirect.TouchPlainKeyDirect(key, update)

	assert.Equal(t, utSerialized.Size(), utDirect.Size(), "Same number of entries")

	serializedUpdate := findKeyUpdate(t, utSerialized, key)
	directUpdate := findKeyUpdate(t, utDirect, key)
	assert.True(t, serializedUpdate.Balance.Eq(&directUpdate.Balance), "Balance should match")
	assert.Equal(t, serializedUpdate.Nonce, directUpdate.Nonce, "Nonce should match")
	assert.Equal(t, serializedUpdate.Flags&BalanceUpdate != 0, directUpdate.Flags&BalanceUpdate != 0, "BalanceUpdate flag")
	assert.Equal(t, serializedUpdate.Flags&NonceUpdate != 0, directUpdate.Flags&NonceUpdate != 0, "NonceUpdate flag")
}

func findKeyUpdate(t *testing.T, ut *Updates, plainKey string) *Update {
	t.Helper()
	entry, ok := ut.treeIdx[plainKey]
	require.True(t, ok, "key %x should be present", plainKey)
	require.NotNil(t, entry.update)
	return entry.update
}

func TestTouchPlainKeyDirect_Storage(t *testing.T) {
	t.Parallel()

	key := string(common.FromHex("553bba1d92398a69fbc9f01593bbc51b58862366" +
		"0000000000000000000000000000000000000000000000000000000000000004"))
	val := common.FromHex("7c1fed52ef2b45443e674ae782f51586aa29c384")

	utSerialized := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	utSerialized.TouchPlainKey(key, val, utSerialized.TouchStorage)

	utDirect := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	update := &Update{
		Flags:      StorageUpdate,
		StorageLen: int8(len(val)),
	}
	copy(update.Storage[:], val)
	utDirect.TouchPlainKeyDirect(key, update)

	assert.Equal(t, utSerialized.Size(), utDirect.Size())

	su := findKeyUpdate(t, utSerialized, key)
	du := findKeyUpdate(t, utDirect, key)
	assert.Equal(t, su.Storage, du.Storage, "Storage value should match")
	assert.Equal(t, su.StorageLen, du.StorageLen, "StorageLen should match")
	assert.Equal(t, su.Flags, du.Flags, "Flags should match")
}

func TestTouchPlainKeyDirect_Delete(t *testing.T) {
	t.Parallel()

	key := string(common.FromHex("c17fa85f22306d37cec90b0ec74c5623dbbac68f"))

	ut := NewUpdates(ModeUpdate, t.TempDir(), keyHasherNoop)
	ut.TouchPlainKeyDirect(key, &Update{Flags: DeleteUpdate})

	assert.Equal(t, uint64(1), ut.Size())

	u := findKeyUpdate(t, ut, key)
	assert.Equal(t, DeleteUpdate, u.Flags&DeleteUpdate, "Should have DeleteUpdate flag")
}

// TestTouchPlainKeyDirect_UpdateDoesNotEscape pins the escape-analysis property
// this call site depends on: the caller builds an Update per write, so the
// parameter must not escape or every write costs a heap allocation. Taking the
// address of any field of update outside a value-taking helper breaks this.
func TestTouchPlainKeyDirect_UpdateDoesNotEscape(t *testing.T) {
	// Not t.Parallel: AllocsPerRun panics in a parallel test.
	ut := NewUpdates(ModeDirect, t.TempDir(), keyHasherNoop)
	defer ut.Close()

	key := string(make([]byte, 128))
	ut.TouchPlainKeyDirect(key, &Update{Flags: BalanceUpdate})

	allocs := testing.AllocsPerRun(100, func() {
		ut.TouchPlainKeyDirect(key, &Update{
			Flags:   BalanceUpdate,
			Balance: *uint256.NewInt(7),
		})
	})
	require.Zero(t, allocs, "TouchPlainKeyDirect must not allocate: the Update escaped")
}
