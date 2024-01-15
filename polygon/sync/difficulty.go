package sync

import (
	lru "github.com/hashicorp/golang-lru/arc/v2"

	"github.com/ledgerwatch/erigon/eth/stagedsync"

	heimdallspan "github.com/ledgerwatch/erigon/polygon/heimdall/span"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/bor/valset"
)

type DifficultyCalculator interface {
	HeaderDifficulty(header *types.Header) (uint64, error)
	SetSpan(span *heimdallspan.HeimdallSpan)
}

type difficultyCalculatorImpl struct {
	borConfig           *borcfg.BorConfig
	span                *heimdallspan.HeimdallSpan
	validatorSetFactory func() validatorSetInterface
	signaturesCache     *lru.ARCCache[libcommon.Hash, libcommon.Address]
}

func NewDifficultyCalculator(
	borConfig *borcfg.BorConfig,
	span *heimdallspan.HeimdallSpan,
	validatorSetFactory func() validatorSetInterface,
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
		span:                span,
		validatorSetFactory: validatorSetFactory,
		signaturesCache:     signaturesCache,
	}

	if validatorSetFactory == nil {
		impl.validatorSetFactory = impl.makeValidatorSet
	}

	return &impl
}

func (impl *difficultyCalculatorImpl) makeValidatorSet() validatorSetInterface {
	return valset.NewValidatorSet(impl.span.ValidatorSet.Validators)
}

func (impl *difficultyCalculatorImpl) SetSpan(span *heimdallspan.HeimdallSpan) {
	impl.span = span
}

func (impl *difficultyCalculatorImpl) HeaderDifficulty(header *types.Header) (uint64, error) {
	signer, err := bor.Ecrecover(header, impl.signaturesCache, impl.borConfig)
	if err != nil {
		return 0, err
	}
	return impl.signerDifficulty(signer, header.Number.Uint64())
}

func (impl *difficultyCalculatorImpl) signerDifficulty(signer libcommon.Address, headerNum uint64) (uint64, error) {
	validatorSet := impl.validatorSetFactory()

	sprintNum := impl.borConfig.CalculateSprintNumber(headerNum)
	if sprintNum > 0 {
		validatorSet.IncrementProposerPriority(int(sprintNum))
	}

	return validatorSet.Difficulty(signer)
}
