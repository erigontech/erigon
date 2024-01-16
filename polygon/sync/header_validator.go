package sync

import (
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
)

type HeaderValidator interface {
	ValidateHeader(header *types.Header, parent *types.Header, now time.Time) error
}

type headerValidatorImpl struct {
	chainConfig         *chain.Config
	borConfig           *borcfg.BorConfig
	headerTimeValidator HeaderTimeValidator
}

func NewHeaderValidator(
	chainConfig *chain.Config,
	borConfig *borcfg.BorConfig,
	headerTimeValidator HeaderTimeValidator,
) HeaderValidator {
	return &headerValidatorImpl{
		chainConfig:         chainConfig,
		borConfig:           borConfig,
		headerTimeValidator: headerTimeValidator,
	}
}

func (impl *headerValidatorImpl) ValidateHeader(header *types.Header, parent *types.Header, now time.Time) error {
	if err := bor.ValidateHeaderUnusedFields(header); err != nil {
		return err
	}

	if err := bor.ValidateHeaderGas(header, parent, impl.chainConfig); err != nil {
		return err
	}

	if err := bor.ValidateHeaderExtraLength(header.Extra); err != nil {
		return err
	}
	if err := bor.ValidateHeaderSprintValidators(header, impl.borConfig); err != nil {
		return err
	}

	if impl.headerTimeValidator != nil {
		if err := impl.headerTimeValidator.ValidateHeaderTime(header, now, parent); err != nil {
			return err
		}
	}

	return nil
}
