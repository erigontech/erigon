package consensustests

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
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

func operationAttestationHandler(context testContext) error {
	preState, err := decodeStateFromFile(context, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	postState, err := decodeStateFromFile(context, "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	att := &cltypes.Attestation{}
	if err := decodeSSZObjectFromFile(att, context.version, attestationFileName); err != nil {
		return err
	}
	if err := transition.ProcessAttestations(preState, []*cltypes.Attestation{att}, true); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
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
	return nil
}

func operationAttesterSlashingHandler(context testContext) error {
	preState, err := decodeStateFromFile(context, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	postState, err := decodeStateFromFile(context, "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	att := &cltypes.AttesterSlashing{}
	if err := decodeSSZObjectFromFile(att, context.version, attesterSlashingFileName); err != nil {
		return err
	}
	if err := transition.ProcessAttesterSlashing(preState, att); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
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
	return nil
}

func operationProposerSlashingHandler(context testContext) error {
	preState, err := decodeStateFromFile(context, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	postState, err := decodeStateFromFile(context, "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	att := &cltypes.ProposerSlashing{}
	if err := decodeSSZObjectFromFile(att, context.version, proposerSlashingFileName); err != nil {
		return err
	}
	if err := transition.ProcessProposerSlashing(preState, att); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
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
	return nil
}

func operationBlockHeaderHandler(context testContext) error {
	preState, err := decodeStateFromFile(context, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	postState, err := decodeStateFromFile(context, "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	block := &cltypes.BeaconBlock{}
	if err := decodeSSZObjectFromFile(block, context.version, blockFileName); err != nil {
		return err
	}
	if err := transition.ProcessBlockHeader(preState, block, true); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
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
	return nil
}

func operationDepositHandler(context testContext) error {
	preState, err := decodeStateFromFile(context, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	postState, err := decodeStateFromFile(context, "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	deposit := &cltypes.Deposit{}
	if err := decodeSSZObjectFromFile(deposit, context.version, depositFileName); err != nil {
		return err
	}
	if err := transition.ProcessDeposit(preState, deposit, true); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
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
	return nil
}

func operationSyncAggregateHandler(context testContext) error {
	preState, err := decodeStateFromFile(context, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	postState, err := decodeStateFromFile(context, "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	agg := &cltypes.SyncAggregate{}
	if err := decodeSSZObjectFromFile(agg, context.version, syncAggregateFileName); err != nil {
		return err
	}
	if err := transition.ProcessSyncAggregate(preState, agg, true); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
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
	return nil
}

func operationVoluntaryExitHandler(context testContext) error {
	preState, err := decodeStateFromFile(context, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	postState, err := decodeStateFromFile(context, "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	vo := &cltypes.SignedVoluntaryExit{}
	if err := decodeSSZObjectFromFile(vo, context.version, voluntaryExitFileName); err != nil {
		return err
	}
	if err := transition.ProcessVoluntaryExit(preState, vo, true); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
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
	return nil
}

func operationWithdrawalHandler(context testContext) error {
	preState, err := decodeStateFromFile(context, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	postState, err := decodeStateFromFile(context, "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	executionPayload := &cltypes.Eth1Block{}
	if err := decodeSSZObjectFromFile(executionPayload, context.version, executionPayloadFileName); err != nil {
		return err
	}
	if err := transition.ProcessWithdrawals(preState, executionPayload.Withdrawals, true); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
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
	return nil
}

func operationSignedBlsChangeHandler(context testContext) error {
	preState, err := decodeStateFromFile(context, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	postState, err := decodeStateFromFile(context, "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	change := &cltypes.SignedBLSToExecutionChange{}
	if err := decodeSSZObjectFromFile(change, context.version, addressChangeFileName); err != nil {
		return err
	}
	if err := transition.ProcessBlsToExecutionChange(preState, change, true); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
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
	return nil
}
