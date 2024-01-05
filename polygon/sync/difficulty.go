package sync

import (
	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/log/v3"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/bor/borcfg"
	heimdallspan "github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/core/types"
)

type DifficultyCalculator interface {
	HeaderDifficulty(header *types.Header) (uint64, error)
}

type difficultyCalculatorImpl struct {
	borConfig       *borcfg.BorConfig
	span            *heimdallspan.HeimdallSpan
	signaturesCache *lru.ARCCache[libcommon.Hash, libcommon.Address]

	log log.Logger
}

func NewDifficultyCalculator(
	borConfig *borcfg.BorConfig,
	span *heimdallspan.HeimdallSpan,
	log log.Logger,
) DifficultyCalculator {
	signaturesCache, err := lru.NewARC[libcommon.Hash, libcommon.Address](stagedsync.InMemorySignatures)
	if err != nil {
		panic(err)
	}
	return &difficultyCalculatorImpl{
		borConfig:       borConfig,
		span:            span,
		signaturesCache: signaturesCache,

		log: log,
	}
}

func (impl *difficultyCalculatorImpl) HeaderDifficulty(header *types.Header) (uint64, error) {
	signer, err := bor.Ecrecover(header, impl.signaturesCache, impl.borConfig)
	if err != nil {
		return 0, err
	}

	validatorSet := valset.NewValidatorSet(impl.span.ValidatorSet.Validators, log.New())

	sprintCount := impl.borConfig.CalculateSprintNumber(header.Number.Uint64())
	if sprintCount > 0 {
		validatorSet.IncrementProposerPriority(int(sprintCount), impl.log)
	}

	return validatorSet.Difficulty(signer)
}
