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
	"fmt"

	lru "github.com/hashicorp/golang-lru/arc/v2"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bor/valset"
)

type DifficultyCalculator interface {
	HeaderDifficulty(header *types.Header) (uint64, error)
}

type difficultyCalculator struct {
	borConfig           *borcfg.BorConfig
	spans               *SpansCache
	validatorSetFactory func(headerNum uint64) validatorSetInterface
	signaturesCache     *lru.ARCCache[libcommon.Hash, libcommon.Address]
}

func NewDifficultyCalculator(
	borConfig *borcfg.BorConfig,
	spans *SpansCache,
	validatorSetFactory func(headerNum uint64) validatorSetInterface,
	signaturesCache *lru.ARCCache[libcommon.Hash, libcommon.Address],
) DifficultyCalculator {
	if signaturesCache == nil {
		var err error
		signaturesCache, err = lru.NewARC[libcommon.Hash, libcommon.Address](InMemorySignatures)
		if err != nil {
			panic(err)
		}
	}

	calc := difficultyCalculator{
		borConfig:           borConfig,
		spans:               spans,
		validatorSetFactory: validatorSetFactory,
		signaturesCache:     signaturesCache,
	}

	if validatorSetFactory == nil {
		calc.validatorSetFactory = calc.makeValidatorSet
	}

	return &calc
}

func (calc *difficultyCalculator) makeValidatorSet(headerNum uint64) validatorSetInterface {
	span := calc.spans.SpanAt(headerNum)
	if span == nil {
		return nil
	}
	return valset.NewValidatorSet(span.ValidatorSet.Validators)
}

func (calc *difficultyCalculator) HeaderDifficulty(header *types.Header) (uint64, error) {
	signer, err := bor.Ecrecover(header, calc.signaturesCache, calc.borConfig)
	if err != nil {
		return 0, err
	}
	return calc.signerDifficulty(signer, header.Number.Uint64())
}

func (calc *difficultyCalculator) signerDifficulty(signer libcommon.Address, headerNum uint64) (uint64, error) {
	validatorSet := calc.validatorSetFactory(headerNum)
	if validatorSet == nil {
		return 0, fmt.Errorf("difficultyCalculator.signerDifficulty: no span at %d", headerNum)
	}

	sprintNum := calc.borConfig.CalculateSprintNumber(headerNum)
	if sprintNum > 0 {
		validatorSet.IncrementProposerPriority(int(sprintNum))
	}

	return validatorSet.Difficulty(signer)
}
