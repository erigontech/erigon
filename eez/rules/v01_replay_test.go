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

package rules_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/networks"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// TestV01Replay executes the first blocks of 699169 and 916969, their
// history updates, native deliveries and outbound pairs included, to the
// state and receipts roots their headers carry.
func TestV01Replay(t *testing.T) {
	for _, spec := range []chainspec.Spec{networks.Chiado699169, networks.Chiado916969} {
		t.Run(spec.Name, func(t *testing.T) {
			fixture, err := testfixtures.LoadRollupXBlocks(spec.Config.ChainID.Uint64())
			require.NoError(t, err)
			m, _ := derivertest.NewL2Of(t, spec)
			blocks := make([]*types.Block, 0, len(fixture.Raw)-1)
			for _, raw := range fixture.Raw[1:] {
				b := new(types.Block)
				require.NoError(t, rlp.DecodeBytes(raw, b))
				blocks = append(blocks, b)
			}
			const step = 50
			for i := 0; i < len(blocks); i += step {
				chunk := blocks[i:min(i+step, len(blocks))]
				headers := make([]*types.Header, len(chunk))
				for j, b := range chunk {
					headers[j] = b.Header()
				}
				err := m.InsertChain(&blockgen.ChainPack{Blocks: chunk, Headers: headers, TopBlock: chunk[len(chunk)-1]})
				require.NoError(t, err, fmt.Sprintf("blocks %d-%d", chunk[0].NumberU64(), chunk[len(chunk)-1].NumberU64()))
			}
			require.Equal(t, fixture.Head, m.Current(nil).Hash())
		})
	}
}
