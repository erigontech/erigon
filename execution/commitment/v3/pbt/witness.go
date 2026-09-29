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

package pbt

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297/witness"
)

func (t *Trie) Witness(ctx context.Context, expectedRoot common.Hash, input witness.PBinDriverInput) ([][]byte, [][]byte, common.Hash, error) {
	if ctx == nil {
		return nil, nil, common.Hash{}, fmt.Errorf("pbin witness: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, common.Hash{}, err
	}
	if t == nil || t.ctx == nil {
		return nil, nil, common.Hash{}, fmt.Errorf("pbin witness: nil Patricia context")
	}
	resolver := NewPBinWitnessResolver(t.ctx)
	model, err := witness.NewPBinTree(expectedRoot, resolver.Resolve)
	if err != nil {
		return nil, nil, common.Hash{}, err
	}
	postRoot, resolved, err := model.Apply(input)
	if err != nil {
		return nil, nil, common.Hash{}, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, common.Hash{}, err
	}
	slices.SortStableFunc(resolved, func(a, b witness.PBinResolvedNode) int {
		return bytes.Compare(a.Path, b.Path)
	})
	paths := make([][]byte, len(resolved))
	blobs := make([][]byte, len(resolved))
	for index, node := range resolved {
		paths[index] = slices.Clone(node.Path)
		blobs[index] = slices.Clone(node.Blob)
	}
	return paths, blobs, postRoot, nil
}
