package consensus_tests

import (
	"fmt"
	"github.com/ledgerwatch/erigon/spectest"
	"io/fs"
	"os"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ForksFork = spectest.HandlerFunc(func(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {

	preState, err := spectest.ReadBeaconState(root, c.Version()-1, spectest.PreSsz)
	require.NoError(t, err)

	postState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PostSsz)
	expectedError := os.IsNotExist(err)
	if !expectedError {
		require.NoError(t, err)
	}
	switch preState.Version() {
	case clparams.Phase0Version:
		err = preState.UpgradeToAltair()
	case clparams.AltairVersion:
		err = preState.UpgradeToBellatrix()
	case clparams.BellatrixVersion:
		err = preState.UpgradeToCapella()
	case clparams.CapellaVersion:
		err = preState.UpgradeToDeneb()
	default:
		err = spectest.ErrHandlerNotImplemented(fmt.Sprintf("block state %v", preState.Version()))
	}
	if expectedError {
		assert.Error(t, err)
	}

	haveRoot, err := preState.HashSSZ()
	assert.NoError(t, err)

	expectedRoot, err := postState.HashSSZ()
	assert.NoError(t, err)
	assert.EqualValues(t, haveRoot, expectedRoot, "state root")

	return nil
})
