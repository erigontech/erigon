package consensustests

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
)

func testSanityFunction(context testContext) error {
	testState, err := decodeStateFromFile(context, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	var expectedError bool
	expectedState, err := decodeStateFromFile(context, "post.ssz_snappy")
	if os.IsNotExist(err) {
		expectedError = true
		err = nil
	}
	if err != nil {
		return err
	}
	blocks, err := testBlocks(context)
	if err != nil {
		return err
	}
	for _, block := range blocks {
		err = transition.TransitionState(testState, block, true)
		if err != nil {
			break
		}
	}
	// Deal with transition error
	if expectedError && err == nil {
		return fmt.Errorf("expected error")
	}
	if err != nil {
		if expectedError {
			return nil
		}
		return err
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

func testSanityFunctionSlot(context testContext) error {
	testState, err := decodeStateFromFile(context, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	expectedState, err := decodeStateFromFile(context, "post.ssz_snappy")
	if err != nil {
		return err
	}

	if err := transition.ProcessSlots(testState, expectedState.Slot()); err != nil {
		return err
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
