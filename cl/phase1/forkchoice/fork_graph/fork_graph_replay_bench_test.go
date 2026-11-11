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
	"testing"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/spf13/afero"
)

func BenchmarkReplayStateNonHead(b *testing.B) {
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	if err := utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)); err != nil {
		b.Fatal(err)
	}
	graphInterface, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	if err != nil {
		b.Fatal(err)
	}
	graph := graphInterface.(*forkGraphDisk)
	blockA := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	blockB := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	for _, fixture := range []struct {
		block *cltypes.SignedBeaconBlock
		data  []byte
	}{{blockA, block1}, {blockB, block2}} {
		if err := utils.DecodeSSZSnappy(fixture.block, fixture.data, int(clparams.Phase0Version)); err != nil {
			b.Fatal(err)
		}
		if _, result, err := graph.AddChainSegment(fixture.block, true); err != nil || result != Success {
			b.Fatalf("import block: result %v, error %v", result, err)
		}
	}
	root, err := blockB.Block.HashSSZ()
	if err != nil {
		b.Fatal(err)
	}
	graph.currentState = nil
	b.ResetTimer()
	for b.Loop() {
		if _, err := graph.ReplayState(common.Hash(root)); err != nil {
			b.Fatal(err)
		}
	}
}
