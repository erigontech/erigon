package sync

import (
	"fmt"

	lru "github.com/hashicorp/golang-lru/arc/v2"

	"github.com/ledgerwatch/erigon/eth/stagedsync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/bor/valset"
)

type DifficultyCalculator interface {
	HeaderDifficulty(header *types.Header) (uint64, error)
}

type difficultyCalculatorImpl struct {
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
		signaturesCache, err = lru.NewARC[libcommon.Hash, libcommon.Address](stagedsync.InMemorySignatures)
		if err != nil {
			panic(err)
		}
	}

	impl := difficultyCalculatorImpl{
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

func (impl *difficultyCalculatorImpl) makeValidatorSet(headerNum uint64) validatorSetInterface {
	span := impl.spans.SpanAt(headerNum)
	if span == nil {
		return nil
	}
	return valset.NewValidatorSet(span.ValidatorSet.Validators)
}

func (impl *difficultyCalculatorImpl) HeaderDifficulty(header *types.Header) (uint64, error) {
	signer, err := bor.Ecrecover(header, impl.signaturesCache, impl.borConfig)
	if err != nil {
		return 0, err
	}
	return impl.signerDifficulty(signer, header.Number.Uint64())
}

func (impl *difficultyCalculatorImpl) signerDifficulty(signer libcommon.Address, headerNum uint64) (uint64, error) {
	validatorSet := impl.validatorSetFactory(headerNum)
	if validatorSet == nil {
		return 0, fmt.Errorf("difficultyCalculatorImpl.signerDifficulty: no span at %d", headerNum)
	}

	sprintNum := impl.borConfig.CalculateSprintNumber(headerNum)
	if sprintNum > 0 {
		validatorSet.IncrementProposerPriority(int(sprintNum))
	}

	return validatorSet.Difficulty(signer)
}
