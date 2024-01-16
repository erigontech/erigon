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

type headerTimeValidatorImpl struct {
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

	impl := headerTimeValidatorImpl{
		borConfig:           borConfig,
		spans:               spans,
		validatorSetFactory: validatorSetFactory,
		signaturesCache:     signaturesCache,
	}

	if validatorSetFactory == nil {
		impl.validatorSetFactory = impl.makeValidatorSet
	}

	return &impl
}

func (impl *headerTimeValidatorImpl) makeValidatorSet(headerNum uint64) validatorSetInterface {
	span := impl.spans.SpanAt(headerNum)
	if span == nil {
		return nil
	}
	return valset.NewValidatorSet(span.ValidatorSet.Validators)
}

func (impl *headerTimeValidatorImpl) ValidateHeaderTime(header *types.Header, now time.Time, parent *types.Header) error {
	headerNum := header.Number.Uint64()
	validatorSet := impl.validatorSetFactory(headerNum)
	if validatorSet == nil {
		return fmt.Errorf("headerTimeValidatorImpl.ValidateHeaderTime: no span at %d", headerNum)
	}

	sprintNum := impl.borConfig.CalculateSprintNumber(headerNum)
	if sprintNum > 0 {
		validatorSet.IncrementProposerPriority(int(sprintNum))
	}

	return bor.ValidateHeaderTime(header, now, parent, validatorSet, impl.borConfig, impl.signaturesCache)
}
