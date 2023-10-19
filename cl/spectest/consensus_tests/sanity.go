package consensus_tests

import (
	"github.com/ledgerwatch/erigon/cl/transition/machine"
	spectest2 "github.com/ledgerwatch/erigon/spectest"
	"io/fs"
	"os"
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var SanitySlots = spectest2.HandlerFunc(func(t *testing.T, root fs.FS, c spectest2.TestCase) (err error) {
	// TODO: this is unused, why?
	var slots int
	err = spectest2.ReadMeta(root, "slots.yaml", &slots)
	require.NoError(t, err)

	testState, err := spectest2.ReadBeaconState(root, c.Version(), spectest2.PreSsz)
	require.NoError(t, err)

	expectedState, err := spectest2.ReadBeaconState(root, c.Version(), spectest2.PostSsz)
	require.NoError(t, err)

	err = c.Machine.ProcessSlots(testState, expectedState.Slot())
	require.NoError(t, err)

	expectedRoot, err := expectedState.HashSSZ()
	require.NoError(t, err)

	haveRoot, err := testState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
	return nil
})

var SanityBlocks = spectest2.HandlerFunc(func(t *testing.T, root fs.FS, c spectest2.TestCase) (err error) {
	var meta struct {
		Description            string `yaml:"description"`
		BlsSetting             int    `yaml:"bls_settings"`
		RevealDeadlinesSetting int    `yaml:"reveal_deadlines_setting"`
		BlocksCount            int    `yaml:"blocks_count"`
	}

	err = spectest2.ReadMeta(root, "meta.yaml", &meta)
	require.NoError(t, err)

	testState, err := spectest2.ReadBeaconState(root, c.Version(), spectest2.PreSsz)
	require.NoError(t, err)

	var expectedError bool
	expectedState, err := spectest2.ReadBeaconState(root, c.Version(), spectest2.PostSsz)
	if os.IsNotExist(err) {
		expectedError = true
	} else {
		require.NoError(t, err)
	}

	blocks, err := spectest2.ReadBlocks(root, c.Version())
	require.NoError(t, err)

	var block *cltypes.SignedBeaconBlock
	for _, block = range blocks {
		err = machine.TransitionState(c.Machine, testState, block)
		if err != nil {
			break
		}
	}
	// Deal with transition error
	if expectedError {
		require.Error(t, err)
		return nil
	} else {
		require.NoError(t, err)
	}

	finalRoot, err := expectedState.HashSSZ()
	require.NoError(t, err)
	haveRoot, err := testState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, finalRoot, haveRoot)

	return nil
})
