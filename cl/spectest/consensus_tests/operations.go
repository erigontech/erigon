package consensus_tests

import (
	"fmt"
	"io/fs"
	"os"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/spectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	attestationFileName      = "attestation.ssz_snappy"
	attesterSlashingFileName = "attester_slashing.ssz_snappy"
	proposerSlashingFileName = "proposer_slashing.ssz_snappy"
	blockFileName            = "block.ssz_snappy"
	depositFileName          = "deposit.ssz_snappy"
	syncAggregateFileName    = "sync_aggregate.ssz_snappy"
	voluntaryExitFileName    = "voluntary_exit.ssz_snappy"
	executionPayloadFileName = "execution_payload.ssz_snappy"
	addressChangeFileName    = "address_change.ssz_snappy"
)

func operationAttestationHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	att := &solid.Attestation{}
	if err := spectest.ReadSszOld(root, att, c.Version(), attestationFileName); err != nil {
		return err
	}
	if err := c.Machine.ProcessAttestations(preState, solid.NewDynamicListSSZFromList([]*solid.Attestation{att}, 128)); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return fmt.Errorf("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, haveRoot, expectedRoot)
	return nil
}

func operationAttesterSlashingHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	att := &cltypes.AttesterSlashing{}
	if err := spectest.ReadSszOld(root, att, c.Version(), attesterSlashingFileName); err != nil {
		return err
	}
	if err := c.Machine.ProcessAttesterSlashing(preState, att); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return fmt.Errorf("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, haveRoot, expectedRoot)
	return nil
}

func operationProposerSlashingHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	att := &cltypes.ProposerSlashing{}
	if err := spectest.ReadSszOld(root, att, c.Version(), proposerSlashingFileName); err != nil {
		return err
	}
	if err := c.Machine.ProcessProposerSlashing(preState, att); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return fmt.Errorf("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, haveRoot, expectedRoot)
	return nil
}

func operationBlockHeaderHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	block := cltypes.NewBeaconBlock(&clparams.MainnetBeaconConfig)
	if err := spectest.ReadSszOld(root, block, c.Version(), blockFileName); err != nil {
		return err
	}
	if err := c.Machine.ProcessBlockHeader(preState, block); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return fmt.Errorf("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, haveRoot, expectedRoot)
	return nil
}

func operationDepositHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	deposit := &cltypes.Deposit{}
	if err := spectest.ReadSszOld(root, deposit, c.Version(), depositFileName); err != nil {
		return err
	}
	if err := c.Machine.ProcessDeposit(preState, deposit); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return fmt.Errorf("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, haveRoot, expectedRoot)
	return nil
}

func operationSyncAggregateHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	agg := &cltypes.SyncAggregate{}
	if err := spectest.ReadSszOld(root, agg, c.Version(), syncAggregateFileName); err != nil {
		return err
	}
	if err := c.Machine.ProcessSyncAggregate(preState, agg); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return fmt.Errorf("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, haveRoot, expectedRoot)
	return nil
}

func operationVoluntaryExitHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	vo := &cltypes.SignedVoluntaryExit{}
	if err := spectest.ReadSszOld(root, vo, c.Version(), voluntaryExitFileName); err != nil {
		return err
	}
	if err := c.Machine.ProcessVoluntaryExit(preState, vo); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return fmt.Errorf("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, haveRoot, expectedRoot)
	return nil
}

func operationWithdrawalHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	executionPayload := cltypes.NewEth1Block(c.Version(), &clparams.MainnetBeaconConfig)
	if err := spectest.ReadSszOld(root, executionPayload, c.Version(), executionPayloadFileName); err != nil {
		return err
	}
	if err := c.Machine.ProcessWithdrawals(preState, executionPayload.Withdrawals); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return fmt.Errorf("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, haveRoot, expectedRoot)
	return nil
}

func operationSignedBlsChangeHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	change := &cltypes.SignedBLSToExecutionChange{}
	if err := spectest.ReadSszOld(root, change, c.Version(), addressChangeFileName); err != nil {
		return err
	}
	if err := c.Machine.ProcessBlsToExecutionChange(preState, change); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return fmt.Errorf("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)

	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, haveRoot, expectedRoot)
	return nil
}
