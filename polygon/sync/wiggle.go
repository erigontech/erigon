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
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	lru "github.com/hashicorp/golang-lru/arc/v2"
)

type wiggleCalc struct {
	borConfig            *borcfg.BorConfig
	signaturesCache      *lru.ARCCache[common.Hash, common.Address]
	blockProducersReader blockProducersReader
}

func NewWiggleCalculator(borConfig *borcfg.BorConfig, signaturesCache *lru.ARCCache[common.Hash, common.Address], blockProducersReader blockProducersReader) *wiggleCalc {
	return &wiggleCalc{
		borConfig:            borConfig,
		signaturesCache:      signaturesCache,
		blockProducersReader: blockProducersReader,
	}
}

func (calc *wiggleCalc) CalculateWiggle(ctx context.Context, header *types.Header) (time.Duration, error) {
	signer, err := bor.Ecrecover(header, calc.signaturesCache, calc.borConfig)
	if err != nil {
		return 0, err
	}

	producers, err := calc.blockProducersReader.Producers(ctx, header.Number.Uint64())
	if err != nil {
		return 0, err
	}

	succession, err := producers.GetSignerSuccessionNumber(signer, header.Number.Uint64())
	if err != nil {
		return 0, err
	}

	wiggle := time.Duration(succession) * time.Duration(calc.borConfig.CalculateBackupMultiplier(header.Number.Uint64())) * time.Second
	return wiggle, nil
}
