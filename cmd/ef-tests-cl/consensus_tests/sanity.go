package consensus_tests

import (
	"io/fs"
	"os"
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/ef-tests-cl/spectest"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var SanitySlots = spectest.HandlerFunc(func(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	// TODO: this is unused, why?
	var slots int
	err = spectest.ReadMeta(root, "slots.yaml", &slots)
	require.NoError(t, err)

	testState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PreSsz)
	require.NoError(t, err)

	expectedState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PostSsz)
	require.NoError(t, err)

	err = transition.ProcessSlots(testState, expectedState.Slot())
	require.NoError(t, err)

	expectedRoot, err := expectedState.HashSSZ()
	require.NoError(t, err)

	haveRoot, err := testState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
	return nil
})

var SanityBlocks = spectest.HandlerFunc(func(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	var meta struct {
		Description            string `yaml:"description"`
		BlsSetting             int    `yaml:"bls_settings"`
		RevealDeadlinesSetting int    `yaml:"reveal_deadlines_setting"`
		BlocksCount            int    `yaml:"blocks_count"`
	}

	err = spectest.ReadMeta(root, "meta.yaml", &meta)
	require.NoError(t, err)

	testState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PreSsz)
	require.NoError(t, err)

	var expectedError bool
	expectedState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PostSsz)
	if os.IsNotExist(err) {
		expectedError = true
	}

	blocks, err := spectest.ReadBlocks(root, c.Version())
	require.NoError(t, err)

	var block *cltypes.SignedBeaconBlock
	for _, block = range blocks {
		err = transition.TransitionState(testState, block, true)
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
