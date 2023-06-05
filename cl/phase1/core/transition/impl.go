package transition

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

type impl struct {
	FullValidation bool
}

func (I *impl) VerifyTransition(s *state.BeaconState, currentBlock *cltypes.BeaconBlock) error {
	if !I.FullValidation {
		return nil
	}
	expectedStateRoot, err := s.HashSSZ()
	if err != nil {
		return fmt.Errorf("unable to generate state root: %v", err)
	}
	if expectedStateRoot != currentBlock.StateRoot {
		return fmt.Errorf("expected state root differs from received state root")
	}
	return nil
}
