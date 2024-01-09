package sync

import (
	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/eth/stagedsync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/bor/borcfg"
	heimdallspan "github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/core/types"
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

	log log.Logger
}

// valset.ValidatorSet abstraction for unit tests
type validatorSetInterface interface {
	IncrementProposerPriority(times int, logger log.Logger)
	Difficulty(signer libcommon.Address) (uint64, error)
}

func NewDifficultyCalculator(
	borConfig *borcfg.BorConfig,
	span *heimdallspan.HeimdallSpan,
	validatorSetFactory func() validatorSetInterface,
	log log.Logger,
) DifficultyCalculator {
	signaturesCache, err := lru.NewARC[libcommon.Hash, libcommon.Address](stagedsync.InMemorySignatures)
	if err != nil {
		panic(err)
	}
	impl := difficultyCalculatorImpl{
		borConfig:           borConfig,
		span:                span,
		validatorSetFactory: validatorSetFactory,
		signaturesCache:     signaturesCache,

		log: log,
	}

	if validatorSetFactory == nil {
		impl.validatorSetFactory = impl.makeValidatorSet
	}

	return &impl
}

func (impl *difficultyCalculatorImpl) makeValidatorSet() validatorSetInterface {
	return valset.NewValidatorSet(impl.span.ValidatorSet.Validators, impl.log)
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
		validatorSet.IncrementProposerPriority(int(sprintNum), impl.log)
	}

	return validatorSet.Difficulty(signer)
}
