package execmodule

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types"
)

func makeTx(nonce uint64) types.Transaction {
	return types.NewTransaction(nonce, common.Address{1}, uint256.NewInt(1), 21000, uint256.NewInt(1), nil)
}

// openGen adds a generation with a stand-in SharedDomains. The frontier only ever compares the pointer
// (and hands it back to the caller), so a zero-value one is enough to exercise the bookkeeping.
func openGen(f *preExecFrontier, number uint64, hash common.Hash) *execctx.SharedDomains {
	sd := &execctx.SharedDomains{}
	f.Open(sd, hash, number)
	return sd
}

func TestPreExecFrontier_CheckUpdateNoState(t *testing.T) {
	f := newPreExecFrontier()
	assert.False(t, f.CheckUpdate(1, []types.Transaction{makeTx(0)}).IsUpdate)
}

func TestPreExecFrontier_CheckUpdatePrefixMatch(t *testing.T) {
	f := newPreExecFrontier()
	sd := openGen(f, 5, common.Hash{5})
	tx0, tx1, tx2 := makeTx(0), makeTx(1), makeTx(2)
	f.RecordTxHashes([]types.Transaction{tx0, tx1})

	got := f.CheckUpdate(5, []types.Transaction{tx0, tx1, tx2})
	require.True(t, got.IsUpdate, "a prefix extension of the active block is an update")
	assert.Equal(t, 2, got.PrefixLen, "the two already-executed txs are the prefix")
	assert.Same(t, sd, got.SD, "the update carries the active generation's SharedDomains")
}

// An EMPTY in-progress block is reusable: the atomic open creates the successor as an empty block at the
// close, and the first content round carries forward into it with PrefixLen 0.
func TestPreExecFrontier_CheckUpdateEmptyBlockIsReusable(t *testing.T) {
	f := newPreExecFrontier()
	openGen(f, 5, common.Hash{5})

	got := f.CheckUpdate(5, []types.Transaction{makeTx(0)})
	require.True(t, got.IsUpdate)
	assert.Zero(t, got.PrefixLen)
}

func TestPreExecFrontier_CheckUpdateRejects(t *testing.T) {
	tx0, tx1, tx2 := makeTx(0), makeTx(1), makeTx(2)
	for _, tc := range []struct {
		name   string
		number uint64
		txs    []types.Transaction
	}{
		{"different block number", 6, []types.Transaction{tx0, tx1}},
		{"prefix mismatch (reordered)", 5, []types.Transaction{tx0, tx2}},
		{"shorter than what we executed", 5, []types.Transaction{tx0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPreExecFrontier()
			openGen(f, 5, common.Hash{5})
			f.RecordTxHashes([]types.Transaction{tx0, tx1})
			assert.False(t, f.CheckUpdate(tc.number, tc.txs).IsUpdate)
		})
	}
}

// The producer runs ahead of consensus, so a block opening must chain onto its IMMEDIATE predecessor.
// Chaining to a grandparent copies a txNum index missing the parent's entry, and the block cannot open.
func TestPreExecFrontier_ParentForPicksImmediatePredecessor(t *testing.T) {
	f := newPreExecFrontier()
	openGen(f, 4, common.Hash{4})
	sd5 := openGen(f, 5, common.Hash{5})

	assert.Same(t, sd5, f.ParentFor(6), "block 6 chains onto block 5")
	assert.Nil(t, f.ParentFor(4), "no live ancestor below block 4")
}

func TestPreExecFrontier_ParentForFallsBackBelow(t *testing.T) {
	f := newPreExecFrontier()
	sd4 := openGen(f, 4, common.Hash{4})

	assert.Same(t, sd4, f.ParentFor(9), "with no immediate predecessor, the newest generation below is the parent")
}

// The validation side asks these two questions, and only these two.
func TestPreExecFrontier_LiveAndGen(t *testing.T) {
	f := newPreExecFrontier()
	sd := openGen(f, 5, common.Hash{5})

	assert.True(t, f.Live(common.Hash{5}, 5))
	assert.False(t, f.Live(common.Hash{9}, 5), "wrong hash is not live")
	assert.False(t, f.Live(common.Hash{5}, 6), "wrong number is not live")
	assert.True(t, f.Owns(sd))
	assert.False(t, f.Owns(&execctx.SharedDomains{}))

	require.NoError(t, f.SealActive(common.Hash{5}, common.Hash{55}, 5))
	gotSD, _ := f.Gen(common.Hash{55}, 5)
	assert.Same(t, sd, gotSD)
}

// Only a SEALED generation may cross into validation space. An in-progress one carries a placeholder
// header whose state root has not been computed, so promoting it would canonicalise a block that was
// never sealed — and would short-circuit the producer's own close, which runs against that same hash.
func TestPreExecFrontier_GenRefusesUnsealed(t *testing.T) {
	f := newPreExecFrontier()
	openGen(f, 5, common.Hash{5})

	sd, _ := f.Gen(common.Hash{5}, 5)
	assert.Nil(t, sd, "an in-progress generation is not promotable")
	assert.True(t, f.Live(common.Hash{5}, 5), "but it is still a live ancestor for its successor")

	require.NoError(t, f.SealActive(common.Hash{5}, common.Hash{5}, 5))
	sd, _ = f.Gen(common.Hash{5}, 5)
	assert.NotNil(t, sd, "once sealed it is promotable")
}

// The in-progress header re-hashes every round as its body grows, so the recorded hash must track it —
// otherwise newPayload cannot match the block it gets back against the generation that built it.
func TestPreExecFrontier_SetActiveHeadRetracksHash(t *testing.T) {
	f := newPreExecFrontier()
	sd := openGen(f, 5, common.Hash{5})
	f.SetActiveHead(common.Hash{55}, 5)

	assert.False(t, f.Live(common.Hash{5}, 5), "the stale hash no longer resolves")
	assert.True(t, f.Live(common.Hash{55}, 5), "the current hash does")
	_, _, active := f.Active()
	assert.Same(t, sd, active)

	f.SetActiveHead(common.Hash{99}, 4)
	assert.True(t, f.Live(common.Hash{55}, 5), "a mismatched number leaves the active generation alone")
}

func TestPreExecFrontier_ActiveAndDepth(t *testing.T) {
	f := newPreExecFrontier()
	assert.Equal(t, 0, f.Depth())
	_, _, sd := f.Active()
	assert.Nil(t, sd)

	openGen(f, 4, common.Hash{4})
	sd5 := openGen(f, 5, common.Hash{5})

	hash, number, active := f.Active()
	assert.Equal(t, common.Hash{5}, hash)
	assert.Equal(t, uint64(5), number)
	assert.Same(t, sd5, active, "the newest generation is the in-progress block")
	assert.Equal(t, 2, f.Depth(), "depth above 1 means the producer is running ahead")
}

func TestPreExecFrontier_NotifyCommittedIsMonotonic(t *testing.T) {
	f := newPreExecFrontier()
	f.NotifyCommitted(7)
	f.NotifyCommitted(3)
	assert.Equal(t, uint64(7), f.CommittedHeight(), "committed height never goes backwards")
}
