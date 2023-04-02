package consensustests

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/beacon_changeset"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
)

func testSanityFunction(context testContext) error {
	testState, err := decodeStateFromFile(context, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	testState.HashSSZ()
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
	startSlot := testState.Slot()

	changes := []*beacon_changeset.ChangeSet{}
	forwardChanges := []*beacon_changeset.ChangeSet{}
	var block *cltypes.SignedBeaconBlock
	for _, block = range blocks {
		testState.StartCollectingReverseChangeSet()
		testState.StartCollectingForwardChangeSet()
		err = transition.TransitionState(testState, block, true)
		if err != nil {
			break
		}
		changes = append(changes, testState.StopCollectingReverseChangeSet())
		forwardChanges = append(forwardChanges, testState.StopCollectingForwardChangeSet())
	}
	_ = forwardChanges

	// Deal with transition error
	if expectedError && err == nil {
		return fmt.Errorf("expected error")
	}
	if err != nil {
		if expectedError {
			return nil
		}
		return fmt.Errorf("cannot transition state: %s. slot=%d. start_slot=%d", err, block.Block.Slot, startSlot)
	}
	finalRoot, err := expectedState.HashSSZ()
	if err != nil {
		return err
	}
	haveRoot, err := testState.HashSSZ()
	if err != nil {
		return err
	}
	if haveRoot != finalRoot {
		return fmt.Errorf("mismatching state roots")
	}
	if context.version == clparams.Phase0Version {
		return nil
	}
	// Now do the unwind
	initialState, err := decodeStateFromFile(context, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	_ = initialState
	for i := len(changes) - 1; i >= 0; i-- {
		testState.RevertWithChangeset(changes[i])
	}
	expectedRoot, err := initialState.HashSSZ()
	if err != nil {
		return err
	}

	haveRoot, err = testState.HashSSZ()
	if err != nil {
		return err
	}

	if haveRoot != expectedRoot {
		return fmt.Errorf("mismatching state roots with unwind")
	}
	// Execute them back (ensure cache is good.)
	for _, block = range blocks {
		err = transition.TransitionState(testState, block, true)
		if err != nil {
			break
		}
	}
	if err != nil {
		return err
	}
	// Send it back again
	for i := len(changes) - 1; i >= 0; i-- {
		testState.RevertWithChangeset(changes[i])
	}
	haveRoot, err = testState.HashSSZ()
	if err != nil {
		return err
	}
	if haveRoot != expectedRoot {
		return fmt.Errorf("mismatching state roots with unwind")
	}
	// Test the forward changes
	for _, change := range forwardChanges {
		testState.RevertWithChangeset(change)
	}
	haveRoot, err = testState.HashSSZ()
	if err != nil {
		return err
	}
	expectedState.HashSSZ()
	if haveRoot != finalRoot {
		return fmt.Errorf("mismatching state roots for forward changesets")
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
