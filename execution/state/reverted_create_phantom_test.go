package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// A tx that CREATEs a contract (nonce=1 + code) and then reverts must leave no
// versioned write behind — FinalizedWrites (the parallel write-set the commitment
// consumes) must not emit a phantom account. Serial MakeWriteSet drops it via the
// dirties reconciliation; the noMaterialize path relies on journal reverts.
func finalizedWritesRules() *chain.Rules {
	return &chain.Rules{ChainID: uint256.NewInt(1)}
}

func TestRevertedFreshCreate_NoPhantom(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xda, 0x11})
	code := []byte("deployed-bytecode")
	ibs := NewWithVersionMap(&emptyReader{}, NewVersionMap(nil))
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.CreateAccount(addr, true))
	require.NoError(t, ibs.SetNonce(addr, 1, 0))
	require.NoError(t, ibs.SetCode(addr, code, 0))
	ibs.RevertToSnapshot(snap, nil)

	writes := ibs.FinalizedWrites(finalizedWritesRules())
	_, hasNonce := writes.GetNonce(addr)
	_, hasCode := writes.GetCode(addr)
	_, hasCodeHash := writes.GetCodeHash(addr)
	require.False(t, hasNonce, "reverted fresh create must not leave a NoncePath write")
	require.False(t, hasCode, "reverted fresh create must not leave a CodePath write")
	require.False(t, hasCodeHash, "reverted fresh create must not leave a CodeHashPath write")
}

func TestRevertedRecreate_NoPhantom(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xda, 0x22})
	code := []byte("deployed-bytecode")
	// The address already exists (a prior tx touched/funded it) so CreateAccount
	// takes the resetObject path, whose revert restores account-record cells but
	// relies on field entries for nonce/code.
	pre := accounts.NewAccount()
	pre.Balance = *uint256.NewInt(0)
	reader := &fieldReader{addr: addr, account: &pre}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.CreateAccount(addr, true))
	require.NoError(t, ibs.SetNonce(addr, 1, 0))
	require.NoError(t, ibs.SetCode(addr, code, 0))
	ch, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, accounts.InternCodeHash(crypto.Keccak256Hash(code)), ch)
	ibs.RevertToSnapshot(snap, nil)

	writes := ibs.FinalizedWrites(finalizedWritesRules())
	_, hasNonce := writes.GetNonce(addr)
	_, hasCode := writes.GetCode(addr)
	nonceVal := uint64(0)
	if vw, ok := writes.GetNonce(addr); ok {
		nonceVal = vw.Val
	}
	require.False(t, hasNonce, "reverted recreate must not leave a NoncePath write (got nonce=%d)", nonceVal)
	require.False(t, hasCode, "reverted recreate must not leave a CodePath write")
}
