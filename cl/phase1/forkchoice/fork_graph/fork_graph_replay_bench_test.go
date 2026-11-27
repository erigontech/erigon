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

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/common"
	"github.com/spf13/afero"
)

func BenchmarkReplayStateNonHead(b *testing.B) {
	for _, mode := range []string{"signature-checks", "read-replay", "cached-read"} {
		b.Run(mode, func(b *testing.B) {
			graph, root := replayBenchmarkGraph(b)
			b.ResetTimer()
			for b.Loop() {
				var err error
				switch mode {
				case "signature-checks":
					_, err = graph.replayState(root, true)
				case "read-replay":
					graph.replayedStates.clear()
					_, err = graph.ReplayState(root)
				case "cached-read":
					_, err = graph.ReplayState(root)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func replayBenchmarkGraph(b *testing.B) (*forkGraphDisk, common.Hash) {
	b.Helper()
	blocks, anchorState, _ := tests.GetCapellaRandom()
	targetIndex := -1
	for index := 0; index < len(blocks)-1; index++ {
		block := blocks[index]
		if block.Block.Body.ProposerSlashings.Len() > 0 {
			targetIndex = index
			break
		}
	}
	if targetIndex == -1 {
		b.Fatal("fixture has no non-head block with proposer slashings")
	}
	graphInterface, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	if err != nil {
		b.Fatal(err)
	}
	graph := graphInterface.(*forkGraphDisk)
	for _, block := range blocks[:targetIndex+2] {
		if _, result, err := graph.AddChainSegment(block, true); err != nil || result != Success {
			b.Fatalf("import block: result %v, error %v", result, err)
		}
	}
	target := blocks[targetIndex]
	if target.Block.Body.ProposerSlashings.Len() == 0 {
		b.Fatal("fixture target block has no proposer slashings")
	}
	root, err := target.Block.HashSSZ()
	if err != nil {
		b.Fatal(err)
	}
	if graph.currentStateBlockRoot == common.Hash(root) {
		b.Fatal("fixture target is the current head")
	}
	return graph, common.Hash(root)
}
