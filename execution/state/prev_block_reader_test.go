package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// One long-lived reader serves every block: SetBlock re-layers the prev blocks
// for the task's block, and committing a block drops it back to the raw base —
// the per-task, live behavior a reused worker reader needs.
func TestPrevBlockReader_SwitchesPerBlock(t *testing.T) {
	t.Parallel()
	addr := getAddress(1)
	base := &fakeBaseReader{accts: map[accounts.Address]*accounts.Account{addr: acctBal(5)}}
	reg := NewPrevBlockList()
	reg.PushHead(10, 10, mapWithBalance(addr, 100))

	o := NewPrevBlockReader(base, reg)

	// A task in block 11 sees block 10's finalized (uncommitted) balance.
	o.SetBlock(11)
	acc, err := o.ReadAccountData(addr)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), acc.Balance.Uint64(), "block 11 sees block 10")

	// A task in block 10 itself must NOT include block 10 — falls to raw base.
	o.SetBlock(10)
	acc, err = o.ReadAccountData(addr)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), acc.Balance.Uint64(), "block 10 reads its own base, not itself")

	// Once block 10 commits to sd.mem, it leaves the list tail: block 11 now reads
	// the raw base (which, in production, sd.mem now holds 100 — here the fake base
	// stays 5, proving the prev block is gone).
	reg.RemoveTail()
	o.SetBlock(11)
	acc, err = o.ReadAccountData(addr)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), acc.Balance.Uint64(), "committed block dropped from the prev-block list")
}
