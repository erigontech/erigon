package consensustests

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon/cl/clparams"
)

func forkTest(context testContext) error {
	prevContext := context
	prevContext.version--
	preState, err := decodeStateFromFile(prevContext, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	postState, err := decodeStateFromFile(context, "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}

	preState.StartCollectingReverseChangeSet()
	if preState.Version() == clparams.Phase0Version {
		if err := preState.UpgradeToAltair(); err != nil {
			return err
		}
	} else if preState.Version() == clparams.AltairVersion {
		if err := preState.UpgradeToBellatrix(); err != nil {
			return err
		}
	} else if preState.Version() == clparams.BellatrixVersion {
		if err := preState.UpgradeToCapella(); err != nil {
			return err
		}
	}
	change := preState.StopCollectingReverseChangeSet()

	if expectedError {
		return fmt.Errorf("expected error")
	}
	root, err := preState.HashSSZ()
	if err != nil {
		return err
	}
	expectedRoot, err := postState.HashSSZ()
	if err != nil {
		return err
	}
	if root != expectedRoot {
		return fmt.Errorf("mismatching state roots")
	}
	if context.version == clparams.AltairVersion {
		return nil
	}
	// now do unwind
	initialState, err := decodeStateFromFile(prevContext, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	preState.RevertWithChangeset(change)
	root, err = preState.HashSSZ()
	if err != nil {
		return err
	}
	expectedRoot, err = initialState.HashSSZ()
	if err != nil {
		return err
	}
	if root != expectedRoot {
		return fmt.Errorf("mismatching state roots with unwind")
	}
	return nil
}
