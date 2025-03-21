package sync

import (
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/polygon/bor"
)

// valset.ValidatorSet abstraction for unit tests
type validatorSetInterface interface {
	bor.ValidateHeaderTimeSignerSuccessionNumber
	IncrementProposerPriority(times int)
	Difficulty(signer libcommon.Address) (uint64, error)
}
