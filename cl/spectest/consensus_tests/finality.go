package consensus_tests

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cl/transition/machine"
	"io/fs"
	"testing"

	"github.com/ledgerwatch/erigon/spectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var FinalityFinality = spectest.HandlerFunc(func(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {

	testState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PreSsz)
	require.NoError(t, err)

	expectedState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PostSsz)
	require.NoError(t, err)

	blocks, err := spectest.ReadBlocks(root, c.Version())
	if err != nil {
		return err
	}
	startSlot := testState.Slot()
	for _, block := range blocks {
		if err := machine.TransitionState(c.Machine, testState, block); err != nil {
			require.NoError(t, fmt.Errorf("cannot transition state: %w. slot=%d. start_slot=%d", err, block.Block.Slot, startSlot))
		}
	}
	expectedRoot, err := testState.HashSSZ()
	assert.NoError(t, err)

	haveRoot, err := expectedState.HashSSZ()
	assert.NoError(t, err)

	assert.EqualValues(t, haveRoot, expectedRoot, "state root")

	return nil
})
