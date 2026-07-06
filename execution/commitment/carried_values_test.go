package commitment

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func serializedAccount(nonce uint64, balance uint64, codeHash common.Hash) []byte {
	acc := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(balance), CodeHash: accounts.InternCodeHash(codeHash)}
	return accounts.SerialiseV3(&acc)
}

// TestNewAccountUpdate_ReadShapeParity pins that the carried account update has the
// exact shape TrieContext.Account builds from a re-read: Nonce+Balance flags always
// (full values, even zero), CodeUpdate iff the code hash is non-zero, DeleteUpdate
// with empty CodeHash for an absent account. Partial-flag updates suppress the
// cell's lazy re-read and hash wrong leaves, so full shape is load-bearing.
func TestNewAccountUpdate_ReadShapeParity(t *testing.T) {
	t.Parallel()

	codeHash := common.HexToHash("0xdeadbeef00000000000000000000000000000000000000000000000000000001")

	t.Run("live with code", func(t *testing.T) {
		u, err := NewCarriedAccountUpdate(serializedAccount(7, 1000, codeHash))
		require.NoError(t, err)
		require.Equal(t, NonceUpdate|BalanceUpdate|CodeUpdate, u.Flags)
		require.EqualValues(t, 7, u.Nonce)
		require.EqualValues(t, 1000, u.Balance.Uint64())
		require.EqualValues(t, codeHash, u.CodeHash)
	})

	t.Run("live without code", func(t *testing.T) {
		u, err := NewCarriedAccountUpdate(serializedAccount(0, 0, common.Hash{}))
		require.NoError(t, err)
		// DeserialiseV3 restores EmptyCodeHash (not zero) for code-less accounts,
		// so the re-read oracle sets CodeUpdate with the canonical empty hash.
		require.Equal(t, NonceUpdate|BalanceUpdate|CodeUpdate, u.Flags)
		require.EqualValues(t, 0, u.Nonce)
		require.True(t, u.Balance.IsZero())
		require.Equal(t, empty.CodeHash, u.CodeHash)
	})

	t.Run("delete", func(t *testing.T) {
		u, err := NewCarriedAccountUpdate(nil)
		require.NoError(t, err)
		require.Equal(t, DeleteUpdate, u.Flags)
		require.Equal(t, empty.CodeHash, u.CodeHash)
	})
}

// TestNewStorageUpdate_ReadShapeParity pins parity with TrieContext.Storage.
func TestNewStorageUpdate_ReadShapeParity(t *testing.T) {
	t.Parallel()

	val := []byte{0x01, 0x02, 0x03}
	u := NewCarriedStorageUpdate(val)
	require.Equal(t, StorageUpdate, u.Flags)
	require.EqualValues(t, 3, u.StorageLen)
	require.Equal(t, val, u.Storage[:u.StorageLen])

	del := NewCarriedStorageUpdate(nil)
	require.Equal(t, DeleteUpdate, del.Flags)
	require.EqualValues(t, 0, del.StorageLen)
}

func deliverAll(t *testing.T, ut *Updates) map[string]*Update {
	t.Helper()
	got := map[string]*Update{}
	err := ut.HashSort(context.Background(), nil, func(hk, pk []byte, upd *Update) error {
		var cp *Update
		if upd != nil {
			cp = new(Update)
			*cp = *upd
		}
		got[string(pk)] = cp
		return nil
	})
	require.NoError(t, err)
	return got
}

// TestUpdatesModeDirect_CarriedWriteTouch pins that a value-carrying touch
// (TouchPlainKeyDirect) delivers its update through HashSort while marker touches
// (TouchPlainKey) keep delivering nil for the re-read fallback.
func TestUpdatesModeDirect_CarriedWriteTouch(t *testing.T) {
	t.Parallel()

	ut := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	writeKey := string(make([]byte, 20))
	markerKey := string(append([]byte{1}, make([]byte, 19)...))

	u, err := NewCarriedAccountUpdate(serializedAccount(3, 42, common.Hash{}))
	require.NoError(t, err)
	ut.TouchPlainKeyDirect(writeKey, u)
	ut.TouchPlainKey(markerKey, serializedAccount(1, 1, common.Hash{}), ut.TouchAccount)

	got := deliverAll(t, ut)
	require.Len(t, got, 2)
	require.Nil(t, got[markerKey], "marker touch must keep re-read semantics")
	require.NotNil(t, got[writeKey])
	require.Equal(t, NonceUpdate|BalanceUpdate|CodeUpdate, got[writeKey].Flags)
	require.EqualValues(t, 3, got[writeKey].Nonce)
	require.EqualValues(t, 42, got[writeKey].Balance.Uint64())
}

// TestUpdatesModeDirect_LastWriteWins pins the merge semantics: the delivered
// update is the LAST write's full state (delete→recreate resolves to the recreate;
// write→delete resolves to delete), a marker retouch never clears a carried value,
// and a write retouch upgrades a marker entry. Size stays one entry per key.
func TestUpdatesModeDirect_LastWriteWins(t *testing.T) {
	t.Parallel()

	mkUpd := func(nonce, bal uint64) *Update {
		u, err := NewCarriedAccountUpdate(serializedAccount(nonce, bal, common.Hash{}))
		require.NoError(t, err)
		return u
	}
	delUpd := func() *Update {
		u, err := NewCarriedAccountUpdate(nil)
		require.NoError(t, err)
		return u
	}

	t.Run("second write wins", func(t *testing.T) {
		ut := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
		k := string(make([]byte, 20))
		ut.TouchPlainKeyDirect(k, mkUpd(1, 10))
		ut.TouchPlainKeyDirect(k, mkUpd(2, 20))
		require.EqualValues(t, 1, ut.Size())

		got := deliverAll(t, ut)
		require.NotNil(t, got[k])
		require.EqualValues(t, 2, got[k].Nonce)
		require.EqualValues(t, 20, got[k].Balance.Uint64())
	})

	t.Run("delete then recreate", func(t *testing.T) {
		ut := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
		k := string(make([]byte, 20))
		ut.TouchPlainKeyDirect(k, mkUpd(5, 50))
		ut.TouchPlainKeyDirect(k, delUpd())
		ut.TouchPlainKeyDirect(k, mkUpd(5, 50)) // recreate with identical fields
		got := deliverAll(t, ut)
		require.NotNil(t, got[k])
		require.Equal(t, NonceUpdate|BalanceUpdate|CodeUpdate, got[k].Flags, "full flags after recreate — no differential baseline")
		require.EqualValues(t, 5, got[k].Nonce)
	})

	t.Run("write then delete", func(t *testing.T) {
		ut := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
		k := string(make([]byte, 20))
		ut.TouchPlainKeyDirect(k, mkUpd(1, 10))
		ut.TouchPlainKeyDirect(k, delUpd())
		got := deliverAll(t, ut)
		require.NotNil(t, got[k])
		require.Equal(t, DeleteUpdate, got[k].Flags)
	})

	t.Run("marker retouch keeps carried value", func(t *testing.T) {
		ut := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
		k := string(make([]byte, 20))
		ut.TouchPlainKeyDirect(k, mkUpd(9, 90))
		ut.TouchPlainKey(k, nil, ut.TouchAccount)
		got := deliverAll(t, ut)
		require.NotNil(t, got[k])
		require.EqualValues(t, 9, got[k].Nonce)
	})

	t.Run("write upgrades marker entry", func(t *testing.T) {
		ut := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
		k := string(make([]byte, 20))
		ut.TouchPlainKey(k, nil, ut.TouchAccount)
		ut.TouchPlainKeyDirect(k, mkUpd(4, 40))
		got := deliverAll(t, ut)
		require.NotNil(t, got[k])
		require.EqualValues(t, 4, got[k].Nonce)
	})
}

// TestUpdatesModeDirect_CarriedDroppedOnSpill pins that spilling to the etl
// collector downgrades every entry to nil delivery (re-read fallback) — carried
// values never cross the etl encoding.
func TestUpdatesModeDirect_CarriedDroppedOnSpill(t *testing.T) {
	t.Parallel()

	ut := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	ut.directMemLimit = 1 // spill on first touch

	k := string(make([]byte, 20))
	u, err := NewCarriedAccountUpdate(serializedAccount(1, 10, common.Hash{}))
	require.NoError(t, err)
	ut.TouchPlainKeyDirect(k, u)
	k2 := string(append([]byte{2}, make([]byte, 19)...))
	ut.TouchPlainKeyDirect(k2, u)
	// Post-spill retouch of a spilled key must not dereference stale indices.
	ut.TouchPlainKeyDirect(k, u)
	require.NotNil(t, ut.etl)

	got := deliverAll(t, ut)
	require.Len(t, got, 2)
	require.Nil(t, got[k])
	require.Nil(t, got[k2])
}

// TestUpdatesModeDirect_DropCarriedValues pins the staleness hook: after
// DropCarriedValues (unwind / state-reader swap), every entry delivers nil so the
// fold re-reads current state.
func TestUpdatesModeDirect_DropCarriedValues(t *testing.T) {
	t.Parallel()

	ut := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	k := string(make([]byte, 20))
	u, err := NewCarriedAccountUpdate(serializedAccount(1, 10, common.Hash{}))
	require.NoError(t, err)
	ut.TouchPlainKeyDirect(k, u)

	ut.DropCarriedValues()

	got := deliverAll(t, ut)
	require.Len(t, got, 1)
	require.Nil(t, got[k])
}

// TestUpdatesModeDirect_CarriedKeepsDeliveryOrder pins that carrying values does
// not disturb the hashedKey-sorted, insertion-tiebroken delivery the trie requires.
func TestUpdatesModeDirect_CarriedKeepsDeliveryOrder(t *testing.T) {
	t.Parallel()

	carried := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	markers := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	blob := serializedAccount(1, 1, common.Hash{})
	for i := 0; i < 3000; i++ {
		a := make([]byte, 20)
		a[0], a[7], a[19] = byte(i), byte(i>>8), byte(i*7)
		u, err := NewCarriedAccountUpdate(blob)
		require.NoError(t, err)
		carried.TouchPlainKeyDirect(string(a), u)
		markers.TouchPlainKey(string(a), blob, markers.TouchAccount)
	}

	var carriedOrder, markerOrder []string
	require.NoError(t, carried.HashSort(context.Background(), nil, func(hk, pk []byte, _ *Update) error {
		carriedOrder = append(carriedOrder, string(hk))
		return nil
	}))
	require.NoError(t, markers.HashSort(context.Background(), nil, func(hk, pk []byte, _ *Update) error {
		markerOrder = append(markerOrder, string(hk))
		return nil
	}))
	require.Equal(t, markerOrder, carriedOrder)
}

func touchCarriedFromState(t testing.TB, ut *Updates, ms *MockState, keys [][]byte, accountKeyLen int) {
	t.Helper()
	for _, k := range keys {
		var u *Update
		var err error
		if len(k) == accountKeyLen {
			u, err = ms.Account(k)
		} else {
			u, err = ms.Storage(k)
		}
		require.NoError(t, err)
		ut.TouchPlainKeyDirect(string(k), u)
	}
}

// TestProcess_CarriedMatchesReread drives the same blocks through the re-read
// baseline (marker touches) and a carried arm whose updates are derived from the
// post-apply state — the roots must match at every block. Block 2 is the
// balance-only sibling scenario where PARTIAL carried updates are known to
// diverge; full carried updates must not. Block 3 covers delete and re-create.
func TestProcess_CarriedMatchesReread(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	msBase := NewMockState(t)
	msCarried := NewMockState(t)
	trieBase := NewHexPatriciaHashed(1, msBase, DefaultTrieConfig())
	trieCarried := NewHexPatriciaHashed(1, msCarried, DefaultTrieConfig())

	processBoth := func(plainKeys [][]byte, updates []Update) {
		t.Helper()
		require.NoError(t, msBase.applyPlainUpdates(plainKeys, updates))
		require.NoError(t, msCarried.applyPlainUpdates(plainKeys, updates))

		base := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
		defer base.Close()
		rootBase, err := trieBase.Process(ctx, base, "", nil, WarmupConfig{})
		require.NoError(t, err)

		carried := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
		defer carried.Close()
		touchCarriedFromState(t, carried, msCarried, plainKeys, 1)
		rootCarried, err := trieCarried.Process(ctx, carried, "", nil, WarmupConfig{})
		require.NoError(t, err)

		require.Equal(t, rootBase, rootCarried)
	}

	k1, u1 := NewUpdateBuilder().
		Balance("00", 100).Nonce("00", 1).
		Balance("01", 200).Nonce("01", 2).
		Balance("02", 300).Nonce("02", 3).
		Storage("03", "01", "abcd").
		Build()
	processBoth(k1, u1)

	k2, u2 := NewUpdateBuilder().Balance("00", 150).Build()
	processBoth(k2, u2)

	k3, u3 := NewUpdateBuilder().
		Delete("01").
		Balance("02", 300).Nonce("02", 4).
		Storage("03", "01", "beef").
		Build()
	processBoth(k3, u3)

	k4, u4 := NewUpdateBuilder().Balance("01", 500).Build()
	processBoth(k4, u4)

	requireBranchParity(t, msBase, msCarried)
}

// TestNewCarriedAccountUpdate_MalformedValue pins that truncated account bytes
// degrade to an error (re-read fallback) instead of panicking the writer.
func TestNewCarriedAccountUpdate_MalformedValue(t *testing.T) {
	t.Parallel()

	u, err := NewCarriedAccountUpdate([]byte{0x01})
	require.Error(t, err)
	require.Nil(t, u)
}
