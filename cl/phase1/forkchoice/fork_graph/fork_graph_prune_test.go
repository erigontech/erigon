package fork_graph

import (
	"testing"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

// Prune must never leave the justified checkpoint unrebuildable.
//
// Rebuilding a state is a replay forward from the nearest DUMPED state at or below it. If pruning
// removes the last such dump, the checkpoint state can never be computed again — and because
// justification cannot then advance, pruning never advances either, so the chain is stuck for good
// rather than merely for a while. A live 2s-slot chain lost ~75% of its proposals for five hours to
// exactly this: the first dump landed at slot 268, four slots ABOVE a justified checkpoint at 264,
// with pruning already cut to 231.
//
// The setup below is that chain in miniature: blocks everywhere, the only usable dump ABOVE the
// checkpoint, and a prune slot that would cut the ground out from under it.
func newGraph(t *testing.T) *forkGraphDisk {
	t.Helper()
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))
	fg := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(),
		beacon_router_configuration.RouterConfiguration{}, beaconevents.NewEventEmitter())
	return fg.(*forkGraphDisk)
}

// addBlock puts a block at `slot` into the graph, optionally with a dumped state beside it.
func addBlock(t *testing.T, g *forkGraphDisk, slot uint64, withDump bool) common.Hash {
	t.Helper()
	root := common.Hash{}
	root[0], root[1] = byte(slot), byte(slot>>8)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	block.Block.Slot = slot
	g.blocks.Store(root, block)
	if withDump {
		f, err := g.fs.Create(getBeaconStateFilename(root))
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
	return root
}

func TestPruneKeepsTheJustifiedCheckpointRebuildable(t *testing.T) {
	g := newGraph(t)

	// Blocks at every slot from 230 to 270; the ONLY dumped state is at 268.
	for slot := uint64(230); slot <= 270; slot++ {
		addBlock(t, g, slot, slot == 268)
	}
	checkpoint := uint64(264) // justified — above every dump we hold

	require.NoError(t, g.Prune(231, checkpoint))

	// Nothing may be dropped: with no dump at or below 264 there is nothing to replay from, so the
	// blocks themselves are the only way back and all of them have to stay.
	for slot := uint64(230); slot <= 270; slot++ {
		root := common.Hash{}
		root[0], root[1] = byte(slot), byte(slot>>8)
		_, ok := g.blocks.Load(root)
		require.Truef(t, ok, "block at slot %d was pruned while the checkpoint had no state to replay from", slot)
	}
}

func TestPruneStopsAtTheDumpTheCheckpointNeeds(t *testing.T) {
	g := newGraph(t)

	// Now a dump DOES exist below the checkpoint, at 240. Everything from 240 up is needed to
	// replay to 264; everything below it is genuinely spare.
	for slot := uint64(230); slot <= 270; slot++ {
		addBlock(t, g, slot, slot == 240 || slot == 268)
	}
	checkpoint := uint64(264)

	require.NoError(t, g.Prune(260, checkpoint))

	held := func(slot uint64) bool {
		root := common.Hash{}
		root[0], root[1] = byte(slot), byte(slot>>8)
		_, ok := g.blocks.Load(root)
		return ok
	}
	// Asked to cut at 260, it must stop at 240 — the replay anchor — not honour the request.
	require.False(t, held(239), "slot 239 is below the replay anchor and should have been pruned")
	require.True(t, held(240), "the replay anchor itself was pruned")
	require.True(t, held(250), "a block needed to replay from 240 up to the checkpoint was pruned")
	require.True(t, held(264), "the checkpoint block was pruned")
}
