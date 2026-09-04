package execmodule

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// dropGen drives the close path every caller funnels through — Open's replace, Abandon, RetireBelow.
// (RetireBelow itself flushes first, which a stand-in SharedDomains cannot do, so the tests below
// reach closeGen through Abandon and directly.)
func dropGen(f *preExecFrontier, g *preExecGen) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gens = f.gens[:0]
	f.closeGen(g)
}

// A generation dropped while a query is reading it must NOT have its SharedDomains closed. RPC
// answering `pending` hands out the live SD, and retirement runs concurrently behind the producer —
// without the lease the reader is left holding freed state.
func TestPreExecFrontier_DropDefersCloseWhilePinned(t *testing.T) {
	f := newPreExecFrontier()
	openGen(f, 1, common.Hash{1})
	openGen(f, 2, common.Hash{2})

	sd, hash, number, release, ok := f.PinActive()
	require.True(t, ok)
	require.NotNil(t, sd)
	assert.Equal(t, common.Hash{2}, hash)
	assert.Equal(t, uint64(2), number)

	pinned := f.gens[len(f.gens)-1]
	dropGen(f, pinned)

	assert.Empty(t, f.gens, "dropped generations leave the chain")
	require.Len(t, f.draining, 1, "the pinned generation is held for its reader")
	assert.NotNil(t, pinned.sd, "a pinned generation is not closed")
	assert.True(t, pinned.dropped)

	release()
	assert.Nil(t, pinned.sd, "the last release closes it")
	assert.Empty(t, f.draining)
}

// An unpinned generation closes immediately — the lease must not keep state alive that nobody holds.
func TestPreExecFrontier_DropClosesWhenUnpinned(t *testing.T) {
	f := newPreExecFrontier()
	openGen(f, 1, common.Hash{1})
	g := f.gens[0]

	dropGen(f, g)

	assert.Empty(t, f.gens)
	assert.Empty(t, f.draining)
	assert.Nil(t, g.sd)
}

// Releasing twice must not close a generation a second reader still holds.
func TestPreExecFrontier_ReleaseIsIdempotentAndRefCounted(t *testing.T) {
	f := newPreExecFrontier()
	openGen(f, 1, common.Hash{1})

	_, _, _, releaseA, ok := f.PinActive()
	require.True(t, ok)
	_, _, _, releaseB, ok := f.PinActive()
	require.True(t, ok)

	g := f.gens[0]
	dropGen(f, g)
	require.NotNil(t, g.sd)

	releaseA()
	releaseA() // a repeated release must not drop the other reader's pin
	assert.NotNil(t, g.sd, "still held by the second reader")

	releaseB()
	assert.Nil(t, g.sd)
}

// Abandoning the active generation while it is being read defers the close the same way: the abandon
// path discards a block the producer is dropping, but a query may still be reading it.
func TestPreExecFrontier_AbandonDefersCloseWhilePinned(t *testing.T) {
	f := newPreExecFrontier()
	openGen(f, 1, common.Hash{1})

	_, _, _, release, ok := f.PinActive()
	require.True(t, ok)
	g := f.gens[0]

	f.Abandon()
	assert.Empty(t, f.gens)
	assert.NotNil(t, g.sd, "abandon must not close a generation being read")

	release()
	assert.Nil(t, g.sd)
}

// Re-opening the same block replaces the active generation; a reader of the displaced one keeps it.
func TestPreExecFrontier_ReopenDefersCloseWhilePinned(t *testing.T) {
	f := newPreExecFrontier()
	openGen(f, 1, common.Hash{1})

	_, _, _, release, ok := f.PinActive()
	require.True(t, ok)
	old := f.gens[0]

	openGen(f, 1, common.Hash{9})
	require.Len(t, f.gens, 1)
	assert.NotSame(t, old, f.gens[0], "the active generation was replaced")
	assert.NotNil(t, old.sd, "the displaced generation is still being read")

	release()
	assert.Nil(t, old.sd)
}

// With no live generation there is nothing pre-confirmed to read, and the caller must be told so
// rather than handed a nil SharedDomains to use.
func TestPreExecFrontier_PinActiveEmpty(t *testing.T) {
	f := newPreExecFrontier()
	sd, _, _, release, ok := f.PinActive()
	assert.False(t, ok)
	assert.Nil(t, sd)
	assert.Nil(t, release)
}
