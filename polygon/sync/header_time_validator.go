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
	"time"

	lru "github.com/hashicorp/golang-lru/arc/v2"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bor/valset"
)

type HeaderTimeValidator interface {
	ValidateHeaderTime(header *types.Header, now time.Time, parent *types.Header) error
}

type headerTimeValidator struct {
	borConfig           *borcfg.BorConfig
	spans               *SpansCache
	validatorSetFactory func(headerNum uint64) validatorSetInterface
	signaturesCache     *lru.ARCCache[libcommon.Hash, libcommon.Address]
}

func NewHeaderTimeValidator(
	borConfig *borcfg.BorConfig,
	spans *SpansCache,
	validatorSetFactory func(headerNum uint64) validatorSetInterface,
	signaturesCache *lru.ARCCache[libcommon.Hash, libcommon.Address],
) HeaderTimeValidator {
	if signaturesCache == nil {
		var err error
		signaturesCache, err = lru.NewARC[libcommon.Hash, libcommon.Address](InMemorySignatures)
		if err != nil {
			panic(err)
		}
	}

	htv := headerTimeValidator{
		borConfig:           borConfig,
		spans:               spans,
		validatorSetFactory: validatorSetFactory,
		signaturesCache:     signaturesCache,
	}

	if validatorSetFactory == nil {
		htv.validatorSetFactory = htv.makeValidatorSet
	}

	return &htv
}

func (htv *headerTimeValidator) makeValidatorSet(headerNum uint64) validatorSetInterface {
	span := htv.spans.SpanAt(headerNum)
	if span == nil {
		return nil
	}
	return valset.NewValidatorSet(span.ValidatorSet.Validators)
}

func (htv *headerTimeValidator) ValidateHeaderTime(header *types.Header, now time.Time, parent *types.Header) error {
	headerNum := header.Number.Uint64()
	validatorSet := htv.validatorSetFactory(headerNum)
	if validatorSet == nil {
		return fmt.Errorf("headerTimeValidator.ValidateHeaderTime: no span at %d", headerNum)
	}

	sprintNum := htv.borConfig.CalculateSprintNumber(headerNum)
	if sprintNum > 0 {
		validatorSet.IncrementProposerPriority(int(sprintNum))
	}

	return bor.ValidateHeaderTime(header, now, parent, validatorSet, htv.borConfig, htv.signaturesCache)
}
