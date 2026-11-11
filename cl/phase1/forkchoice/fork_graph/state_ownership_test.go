// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package fork_graph

import (
	"fmt"
	"sync"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

type countingStateOpenFs struct {
	afero.Fs
	mu    sync.Mutex
	opens int
}

func (fs *countingStateOpenFs) Open(name string) (afero.File, error) {
	file, err := fs.Fs.Open(name)
	if err == nil {
		fs.mu.Lock()
		fs.opens++
		fs.mu.Unlock()
	}
	return file, err
}

func (fs *countingStateOpenFs) count() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.opens
}

func TestForkGraphOwnsAnchorState(t *testing.T) {
	anchor := state.New(&clparams.MainnetBeaconConfig)
	graph, err := NewForkGraphDisk(anchor, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	root := graph.AnchorRoot()

	current := graph.CurrentStateLocked(root)
	require.NotNil(t, current)
	require.NotSame(t, anchor, current)
	require.NotZero(t, anchor.PeekPreviousStateRoot())
	require.Equal(t, current.PeekPreviousStateRoot(), anchor.PeekPreviousStateRoot())
	require.NoError(t, anchor.SetSlot(1))
	require.Zero(t, current.Slot())

	replayed, err := graph.ReplayState(root)
	require.NoError(t, err)
	require.NotNil(t, replayed)
	require.NotSame(t, current, replayed)
}

func TestReplayStateCachesCopyByRoot(t *testing.T) {
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))
	fs := &countingStateOpenFs{Fs: afero.NewMemMapFs()}
	graphInterface, err := NewForkGraphDisk(anchorState, nil, fs, beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	graph := graphInterface.(*forkGraphDisk)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	require.NoError(t, utils.DecodeSSZSnappy(block, block1, int(clparams.Phase0Version)))
	_, result, err := graph.AddChainSegment(block, true)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	graph.currentState = nil

	first, err := graph.ReplayState(root)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, 1, fs.count())
	require.NoError(t, first.SetSlot(99))

	second, err := graph.ReplayState(root)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, 1, fs.count())
	require.NotSame(t, first, second)
	require.Equal(t, block.Block.Slot, second.Slot())
}

func TestReplayStatePruneDropsCachedRoot(t *testing.T) {
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))
	graphInterface, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	graph := graphInterface.(*forkGraphDisk)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	require.NoError(t, utils.DecodeSSZSnappy(block, block1, int(clparams.Phase0Version)))
	_, result, err := graph.AddChainSegment(block, true)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	graph.currentState = nil
	stateAtRoot, err := graph.ReplayState(root)
	require.NoError(t, err)
	require.NotNil(t, stateAtRoot)

	newerRoot := common.Hash{0xff}
	newerBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	newerBlock.Block.Slot = block.Block.Slot + 10
	graph.blocks.Store(newerRoot, newerBlock)
	require.NoError(t, graph.DumpBeaconStateOnDisk(newerRoot, stateAtRoot, true))
	require.NoError(t, graph.Prune(block.Block.Slot+1))
	graph.replayedStates.mu.Lock()
	_, cached := graph.replayedStates.states[root]
	graph.replayedStates.mu.Unlock()
	require.False(t, cached)

	pruned, err := graph.ReplayState(root)
	require.NoError(t, err)
	require.Nil(t, pruned)
}

func TestReplayStateRacingPruneIsCompleteOrUnavailable(t *testing.T) {
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	require.NoError(t, utils.DecodeSSZSnappy(block, block1, int(clparams.Phase0Version)))
	graphInterface, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	graph := graphInterface.(*forkGraphDisk)
	_, status, err := graph.AddChainSegment(block, true)
	require.NoError(t, err)
	require.Equal(t, Success, status)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	current := graph.CurrentStateLocked(root)
	require.NotNil(t, current)
	require.NoError(t, graph.DumpBeaconStateOnDisk(root, current, true))

	newerRoot := common.Hash{0xff}
	newerBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	newerBlock.Block.Slot = block.Block.Slot + 10
	graph.blocks.Store(newerRoot, newerBlock)
	require.NoError(t, afero.WriteFile(graph.fs, getBeaconStateFilename(newerRoot), []byte{1}, 0o644))

	start := make(chan struct{})
	errs := make(chan error, 16)
	var readers sync.WaitGroup
	for range 16 {
		readers.Go(func() {
			<-start
			replayed, replayErr := graph.ReplayState(root)
			if replayErr == nil && replayed != nil {
				var replayedRoot common.Hash
				replayedRoot, replayErr = replayed.HashSSZ()
				if replayErr == nil && replayedRoot != block.Block.StateRoot {
					replayErr = fmt.Errorf("replayed state root %x, want %x", replayedRoot, block.Block.StateRoot)
				}
			}
			errs <- replayErr
		})
	}
	close(start)
	require.NoError(t, graph.Prune(block.Block.Slot+1))
	readers.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestReplayStateIgnoresBlockWithoutHeader(t *testing.T) {
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	require.NoError(t, utils.DecodeSSZSnappy(block, block1, int(clparams.Phase0Version)))
	graphInterface, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	graph := graphInterface.(*forkGraphDisk)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	root := common.Hash(blockRoot)
	_, result, err := graph.AddChainSegment(block, true)
	require.NoError(t, err)
	require.Equal(t, Success, result)
	graph.headers.Delete(root)
	_, ok := graph.GetHeader(root)
	require.False(t, ok)

	replayed, err := graph.ReplayState(root)
	require.NoError(t, err)
	require.Nil(t, replayed)
}
