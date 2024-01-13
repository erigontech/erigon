package sync

import (
	"time"

	lru "github.com/hashicorp/golang-lru/arc/v2"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/bor/valset"
	heimdallspan "github.com/ledgerwatch/erigon/polygon/heimdall/span"
)

type HeaderTimeValidator interface {
	ValidateHeaderTime(header *types.Header, now time.Time, parent *types.Header) error
	SetSpan(span *heimdallspan.HeimdallSpan)
}

type headerTimeValidatorImpl struct {
	borConfig           *borcfg.BorConfig
	span                *heimdallspan.HeimdallSpan
	validatorSetFactory func() validatorSetInterface
	signaturesCache     *lru.ARCCache[libcommon.Hash, libcommon.Address]
}

func NewHeaderTimeValidator(
	borConfig *borcfg.BorConfig,
	span *heimdallspan.HeimdallSpan,
	validatorSetFactory func() validatorSetInterface,
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
		span:                span,
		validatorSetFactory: validatorSetFactory,
		signaturesCache:     signaturesCache,
	}

	if validatorSetFactory == nil {
		impl.validatorSetFactory = impl.makeValidatorSet
	}

	return &impl
}

func (impl *headerTimeValidatorImpl) makeValidatorSet() validatorSetInterface {
	return valset.NewValidatorSet(impl.span.ValidatorSet.Validators)
}

func (impl *headerTimeValidatorImpl) SetSpan(span *heimdallspan.HeimdallSpan) {
	impl.span = span
}

func (impl *headerTimeValidatorImpl) ValidateHeaderTime(header *types.Header, now time.Time, parent *types.Header) error {
	validatorSet := impl.validatorSetFactory()

	sprintNum := impl.borConfig.CalculateSprintNumber(header.Number.Uint64())
	if sprintNum > 0 {
		validatorSet.IncrementProposerPriority(int(sprintNum))
	}

	return bor.ValidateHeaderTime(header, now, parent, validatorSet, impl.borConfig, impl.signaturesCache)
}
