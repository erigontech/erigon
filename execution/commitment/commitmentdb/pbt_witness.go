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

package commitmentdb

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297/witness"
)

type pbinWitnessTrie interface {
	Witness(context.Context, common.Hash, witness.PBinDriverInput) ([][]byte, [][]byte, common.Hash, error)
}

func (sdc *SharedDomainsCommitmentContext) PBinWitness(ctx context.Context, expectedRoot common.Hash, input witness.PBinDriverInput) ([][]byte, [][]byte, common.Hash, error) {
	wt, ok := sdc.patriciaTrie.(pbinWitnessTrie)
	if !ok {
		return nil, nil, common.Hash{}, fmt.Errorf("commitment trie %s cannot build PBT witnesses", sdc.variant)
	}
	return wt.Witness(ctx, expectedRoot, input)
}
