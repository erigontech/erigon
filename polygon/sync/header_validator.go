package sync

import (
	"time"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

type HeaderValidator interface {
	ValidateHeader(header *types.Header, parent *types.Header, now time.Time) error
}

type headerValidator struct {
	chainConfig         *chain.Config
	borConfig           *borcfg.BorConfig
	headerTimeValidator HeaderTimeValidator
}

func NewHeaderValidator(
	chainConfig *chain.Config,
	borConfig *borcfg.BorConfig,
	headerTimeValidator HeaderTimeValidator,
) HeaderValidator {
	return &headerValidator{
		chainConfig:         chainConfig,
		borConfig:           borConfig,
		headerTimeValidator: headerTimeValidator,
	}
}

func (hv *headerValidator) ValidateHeader(header *types.Header, parent *types.Header, now time.Time) error {
	if err := bor.ValidateHeaderUnusedFields(header); err != nil {
		return err
	}

	if err := bor.ValidateHeaderGas(header, parent, hv.chainConfig); err != nil {
		return err
	}

	if err := bor.ValidateHeaderExtraLength(header.Extra); err != nil {
		return err
	}
	if err := bor.ValidateHeaderSprintValidators(header, hv.borConfig); err != nil {
		return err
	}

	if hv.headerTimeValidator != nil {
		if err := hv.headerTimeValidator.ValidateHeaderTime(header, now, parent); err != nil {
			return err
		}
	}

	return nil
}
