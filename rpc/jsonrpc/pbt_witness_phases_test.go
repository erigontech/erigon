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

package jsonrpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/commitment/trie"
)

func TestPBinWitnessSkipsCollapseDetection(t *testing.T) {
	var paths [][]byte
	var err error
	require.NotPanics(t, func() {
		paths, err = detectCollapseSiblings(context.Background(), nil, nil, nil, nil, 0, 0, 0, 0, common.Hash{}, nil, witnessModeLegacy, true)
	})
	require.NoError(t, err)
	require.Empty(t, paths)
}

func TestPBinWitnessOmitsEmptyStorageNode(t *testing.T) {
	accountLeaf := hexutil.Bytes(append([]byte{0xf8, 0x44}, trie.EmptyRoot[:]...))
	nodes := []hexutil.Bytes{accountLeaf}
	require.Equal(t, nodes, appendLegacyEmptyStorageNode(nodes, witnessModeLegacy, true))
}
