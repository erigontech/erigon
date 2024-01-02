package sync

import (
	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus/bor"
	heimdallspan "github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/core/types"
)

type DifficultyCalculator interface {
	HeaderDifficulty(header *types.Header) (uint64, error)
}

type difficultyCalculatorImpl struct {
	borConfig       *chain.BorConfig
	span            *heimdallspan.HeimdallSpan
	signaturesCache *lru.ARCCache[libcommon.Hash, libcommon.Address]
}

func NewDifficultyCalculator(
	borConfig *chain.BorConfig,
	span *heimdallspan.HeimdallSpan,
) DifficultyCalculator {
	signaturesCache, err := lru.NewARC[libcommon.Hash, libcommon.Address](4096)
	if err != nil {
		panic(err)
	}
	return &difficultyCalculatorImpl{
		borConfig:       borConfig,
		span:            span,
		signaturesCache: signaturesCache,
	}
}

func (impl *difficultyCalculatorImpl) HeaderDifficulty(header *types.Header) (uint64, error) {
	signer, err := bor.Ecrecover(header, impl.signaturesCache, impl.borConfig)
	if err != nil {
		return 0, err
	}

	validatorSet := valset.NewValidatorSet(impl.span.ValidatorSet.Validators, log.New())

	sprintCount := impl.borConfig.CalculateSprintCount(0, header.Number.Uint64())
	if sprintCount > 0 {
		validatorSet.IncrementProposerPriority(sprintCount, log.New())
	}

	return validatorSet.Difficulty(signer)
}
