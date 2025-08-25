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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/jellydator/ttlcache/v3"
)

const InMemorySignatures = 4096 // Number of recent block signatures to keep in memory

type CanonicalChainBuilderFactory func(root *types.Header) *CanonicalChainBuilder

func NewCanonicalChainBuilderFactory(
	chainConfig *chain.Config,
	borConfig *borcfg.BorConfig,
	blockProducersTracker blockProducersTracker,
	signaturesCache *lru.ARCCache[common.Hash, common.Address],
	logger log.Logger,
) CanonicalChainBuilderFactory {
	recentVerifiedHeaders := ttlcache.New[common.Hash, *types.Header](
		ttlcache.WithTTL[common.Hash, *types.Header](VeBlopBlockTimeout),
		ttlcache.WithCapacity[common.Hash, *types.Header](DefaultRecentHeadersCapacity),
		ttlcache.WithDisableTouchOnHit[common.Hash, *types.Header](),
	)
	difficultyCalculator := &DifficultyCalculator{
		borConfig:            borConfig,
		signaturesCache:      signaturesCache,
		blockProducersReader: blockProducersTracker,
	}

	headerTimeValidator := &HeaderTimeValidator{
		borConfig:             borConfig,
		signaturesCache:       signaturesCache,
		recentVerifiedHeaders: recentVerifiedHeaders,
		blockProducersTracker: blockProducersTracker,
		logger:                logger,
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
