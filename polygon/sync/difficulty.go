// Copyright 2024 The Erigon Authors
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

package sync

import (
	"context"

	lru "github.com/hashicorp/golang-lru/arc/v2"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

type DifficultyCalculator struct {
	borConfig            *borcfg.BorConfig
	signaturesCache      *lru.ARCCache[common.Hash, common.Address]
	blockProducersReader blockProducersReader
}

func (calc *DifficultyCalculator) HeaderDifficulty(ctx context.Context, header *types.Header) (uint64, error) {
	signer, err := bor.Ecrecover(header, calc.signaturesCache, calc.borConfig)
	if err != nil {
		return 0, err
	}

	return calc.signerDifficulty(ctx, signer, header.Number.Uint64())
}

func (calc *DifficultyCalculator) signerDifficulty(
	ctx context.Context,
	signer common.Address,
	headerNum uint64,
) (uint64, error) {
	producers, err := calc.blockProducersReader.Producers(ctx, headerNum)
	if err != nil {
		return 0, err
	}

	return producers.Difficulty(signer)
}
