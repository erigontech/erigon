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
	lru "github.com/hashicorp/golang-lru/arc/v2"

	"github.com/erigontech/erigon/erigon-lib/chain"
	"github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

const InMemorySignatures = 4096 // Number of recent block signatures to keep in memory

type CanonicalChainBuilderFactory func(root *types.Header) *CanonicalChainBuilder

func NewCanonicalChainBuilderFactory(
	chainConfig *chain.Config,
	borConfig *borcfg.BorConfig,
	blockProducersReader blockProducersReader,
) CanonicalChainBuilderFactory {
	signaturesCache, err := lru.NewARC[common.Hash, common.Address](InMemorySignatures)
	if err != nil {
		panic(err)
	}

	difficultyCalculator := &DifficultyCalculator{
		borConfig:            borConfig,
		signaturesCache:      signaturesCache,
		blockProducersReader: blockProducersReader,
	}

	headerTimeValidator := &HeaderTimeValidator{
		borConfig:            borConfig,
		signaturesCache:      signaturesCache,
		blockProducersReader: blockProducersReader,
	}

	headerValidator := &HeaderValidator{
		chainConfig:         chainConfig,
		borConfig:           borConfig,
		headerTimeValidator: headerTimeValidator,
	}

	return func(root *types.Header) *CanonicalChainBuilder {
		return NewCanonicalChainBuilder(root, difficultyCalculator, headerValidator)
	}
}
