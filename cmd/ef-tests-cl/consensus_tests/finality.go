package consensustests

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
)

func finalityTestFunction(context testContext) error {
	testState, err := decodeStateFromFile(context, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	expectedState, err := decodeStateFromFile(context, "post.ssz_snappy")
	if err != nil {
		return err
	}
	blocks, err := testBlocks(context)
	if err != nil {
		return err
	}
	transistor := transition.New(testState, &clparams.MainnetBeaconConfig, nil, false)
	startSlot := testState.Slot()
	for _, block := range blocks {
		if err := transistor.TransitionState(block); err != nil {
			return fmt.Errorf("cannot transition state: %s. slot=%d. start_slot=%d", err, block.Block.Slot, startSlot)
		}
	}
	expectedRoot, err := expectedState.HashSSZ()
	if err != nil {
		return err
	}
	haveRoot, err := testState.HashSSZ()
	if err != nil {
		return err
	}
	if haveRoot != expectedRoot {
		return fmt.Errorf("mismatching state roots")
	}
	return nil
}
