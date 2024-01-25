package sync

import (
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/arc/v2"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/bor/valset"
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
		signaturesCache, err = lru.NewARC[libcommon.Hash, libcommon.Address](stagedsync.InMemorySignatures)
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
