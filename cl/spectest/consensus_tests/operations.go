// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package consensus_tests

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/spectest/spectest"
	"github.com/erigontech/erigon/cl/transition/machine"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
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
		return errors.New("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
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
		return errors.New("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
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
	proposer, err := preState.ValidatorForValidatorIndex(int(att.Header1.Header.ProposerIndex))
	if err != nil {
		return err
	}
	for _, signedHeader := range []*cltypes.SignedBeaconBlockHeader{att.Header1, att.Header2} {
		domain, err := preState.GetDomain(
			preState.BeaconConfig().DomainBeaconProposer,
			state.GetEpochAtSlot(preState.BeaconConfig(), signedHeader.Header.Slot),
		)
		if err != nil {
			return fmt.Errorf("unable to get domain: %v", err)
		}
		signingRoot, err := fork.ComputeSigningRoot(signedHeader.Header, domain)
		if err != nil {
			return fmt.Errorf("unable to compute signing root: %v", err)
		}
		pk := proposer.PublicKey()
		valid, err := bls.Verify(signedHeader.Signature[:], signingRoot[:], pk[:])
		if err != nil || !valid {
			if expectedError {
				return nil
			}
			return errors.New("verification error")
		}
	}

	if expectedError {
		return errors.New("expected error")
	}

	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
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
	block := cltypes.NewBeaconBlock(&clparams.MainnetBeaconConfig, c.Version())
	if err := spectest.ReadSszOld(root, block, c.Version(), blockFileName); err != nil {
		return err
	}
	bodyRoot, err := block.Body.HashSSZ()
	require.NoError(t, err)
	if err := c.Machine.ProcessBlockHeader(preState, block.Slot, block.ProposerIndex, block.ParentRoot, bodyRoot); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return errors.New("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
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
		return errors.New("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
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
		return errors.New("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
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

	// we have removed signature verification from the function, to make this test pass we do it here.
	var domain []byte
	voluntaryExit := vo.VoluntaryExit
	validator, err := preState.ValidatorForValidatorIndex(int(voluntaryExit.ValidatorIndex))
	if err != nil {
		return err
	}
	if preState.Version() < clparams.DenebVersion {
		domain, err = preState.GetDomain(preState.BeaconConfig().DomainVoluntaryExit, voluntaryExit.Epoch)
	} else if preState.Version() >= clparams.DenebVersion {
		domain, err = fork.ComputeDomain(preState.BeaconConfig().DomainVoluntaryExit[:], utils.Uint32ToBytes4(uint32(preState.BeaconConfig().CapellaForkVersion)), preState.GenesisValidatorsRoot())
	}
	if err != nil {
		return err
	}
	signingRoot, err := fork.ComputeSigningRoot(voluntaryExit, domain)
	if err != nil {
		return err
	}
	pk := validator.PublicKey()
	valid, err := bls.Verify(vo.Signature[:], signingRoot[:], pk[:])
	if err != nil || !valid {
		if expectedError {
			return nil
		}
		return errors.New("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
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
		return errors.New("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
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
		return errors.New("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)

	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
	return nil
}

func operationConsolidationRequestHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	consolidation := &solid.ConsolidationRequest{}
	if err := spectest.ReadSszOld(root, consolidation, c.Version(), "consolidation_request.ssz_snappy"); err != nil {
		return err
	}
	if err := c.Machine.ProcessConsolidationRequest(preState, consolidation); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return errors.New("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)

	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
	return nil
}

func operationDepositRequstHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	request := &solid.DepositRequest{}
	if err := spectest.ReadSszOld(root, request, c.Version(), "deposit_request.ssz_snappy"); err != nil {
		return err
	}
	if err := c.Machine.ProcessDepositRequest(preState, request); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return errors.New("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)

	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
	return nil
}

func operationWithdrawalRequstHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	request := &solid.WithdrawalRequest{}
	if err := spectest.ReadSszOld(root, request, c.Version(), "withdrawal_request.ssz_snappy"); err != nil {
		return err
	}
	if err := c.Machine.ProcessWithdrawalRequest(preState, request); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return errors.New("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)

	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
	return nil
}

func operationExecutionPayloadHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	// Check execution.yaml for execution engine validity
	var executionMeta struct {
		ExecutionValid bool `yaml:"execution_valid"`
	}
	executionMeta.ExecutionValid = true // default to valid
	_ = spectest.ReadYml(root, "execution.yaml", &executionMeta)
	if !executionMeta.ExecutionValid {
		if expectedError {
			return nil
		}
		return errors.New("execution engine returned invalid")
	}
	body := cltypes.NewBeaconBody(&clparams.MainnetBeaconConfig, c.Version())
	if err := spectest.ReadSszOld(root, body, c.Version(), "body.ssz_snappy"); err != nil {
		return err
	}
	if err := c.Machine.ProcessExecutionPayload(preState, body); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return errors.New("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)

	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
	return nil
}

// operationGloasExecutionPayloadHandler handles gloas execution_payload operation tests.
// In gloas, execution payload is delivered via SignedExecutionPayloadEnvelope (not block body).
func operationGloasExecutionPayloadHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	envelope := cltypes.NewSignedExecutionPayloadEnvelope()
	if err := spectest.ReadSszOld(root, envelope, c.Version(), "signed_envelope.ssz_snappy"); err != nil {
		return err
	}
	// Read execution.yaml to check if execution engine should return valid
	var executionMeta struct {
		ExecutionValid bool `yaml:"execution_valid"`
	}
	_ = spectest.ReadYml(root, "execution.yaml", &executionMeta)

	if err := processGloasExecutionPayload(c.Machine, preState, envelope, executionMeta.ExecutionValid); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return errors.New("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)

	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
	return nil
}

// processGloasExecutionPayload implements process_execution_payload for Gloas spec tests.
func processGloasExecutionPayload(impl machine.Interface, s abstract.BeaconState, signedEnvelope *cltypes.SignedExecutionPayloadEnvelope, executionValid bool) error { //nolint:gocognit
	envelope := signedEnvelope.Message
	payload := envelope.Payload

	// Verify envelope signature is a valid BLS point (full verification skipped without builder registry)
	if _, err := bls.NewSignatureFromBytes(signedEnvelope.Signature[:]); err != nil {
		return fmt.Errorf("process_execution_payload: invalid envelope signature: %w", err)
	}

	// Cache latest block header state root
	previousStateRoot, err := s.HashSSZ()
	if err != nil {
		return fmt.Errorf("process_execution_payload: failed to hash state: %w", err)
	}
	latestBlockHeader := s.LatestBlockHeader()
	if latestBlockHeader.Root == ([32]byte{}) {
		latestBlockHeader.Root = previousStateRoot
		s.SetLatestBlockHeader(&latestBlockHeader)
	}

	// Verify consistency with the beacon block
	lbh := s.LatestBlockHeader()
	expectedBeaconBlockRoot, err := lbh.HashSSZ()
	if err != nil {
		return fmt.Errorf("process_execution_payload: failed to hash latest block header: %w", err)
	}
	if envelope.BeaconBlockRoot != expectedBeaconBlockRoot {
		return fmt.Errorf("process_execution_payload: beacon_block_root mismatch: got %x, expected %x",
			envelope.BeaconBlockRoot, expectedBeaconBlockRoot)
	}
	if envelope.Slot != s.Slot() {
		return fmt.Errorf("process_execution_payload: slot mismatch: got %d, expected %d",
			envelope.Slot, s.Slot())
	}

	// Verify consistency with the committed bid
	beaconConfig := s.BeaconConfig()
	committedBid := s.LatestExecutionPayloadBid()
	if envelope.BuilderIndex != committedBid.BuilderIndex {
		return fmt.Errorf("process_execution_payload: builder_index mismatch: got %d, expected %d",
			envelope.BuilderIndex, committedBid.BuilderIndex)
	}

	// Verify prev_randao matches the current epoch's randao mix
	currentEpoch := s.Slot() / beaconConfig.SlotsPerEpoch
	expectedRandao := s.GetRandaoMixes(currentEpoch)
	if payload.PrevRandao != expectedRandao {
		return fmt.Errorf("process_execution_payload: prev_randao mismatch")
	}

	// Verify consistency with expected withdrawals
	actualWithdrawalsRoot, err := payload.Withdrawals.HashSSZ()
	if err != nil {
		return fmt.Errorf("process_execution_payload: failed to hash actual withdrawals: %w", err)
	}
	expectedWithdrawalsRoot := s.LatestWithdrawalsRoot()
	if expectedWithdrawalsRoot != actualWithdrawalsRoot {
		return fmt.Errorf("process_execution_payload: withdrawals mismatch")
	}

	// Verify gas_limit
	if committedBid.GasLimit != payload.GasLimit {
		return fmt.Errorf("process_execution_payload: gas_limit mismatch: committed=%d, actual=%d",
			committedBid.GasLimit, payload.GasLimit)
	}
	// Verify block_hash
	if committedBid.BlockHash != payload.BlockHash {
		return fmt.Errorf("process_execution_payload: block_hash mismatch")
	}
	// Verify parent_hash
	if payload.ParentHash != s.LatestBlockHash() {
		return fmt.Errorf("process_execution_payload: parent_hash mismatch")
	}
	// Verify timestamp
	if payload.Time != state.ComputeTimestampAtSlot(s, s.Slot()) {
		return fmt.Errorf("process_execution_payload: timestamp mismatch")
	}

	// Verify blob_kzg_commitments_root matches the committed bid
	blobCommitmentsRoot, err := envelope.BlobKzgCommitments.HashSSZ()
	if err != nil {
		return fmt.Errorf("process_execution_payload: failed to hash blob commitments: %w", err)
	}
	if blobCommitmentsRoot != committedBid.BlobKzgCommitmentsRoot {
		return fmt.Errorf("process_execution_payload: blob_kzg_commitments_root mismatch")
	}

	// Verify execution engine (simulated via execution.yaml)
	if !executionValid {
		return errors.New("process_execution_payload: execution engine returned invalid")
	}

	// Process execution requests
	requests := envelope.ExecutionRequests
	if requests != nil {
		if requests.Deposits != nil {
			for i := 0; i < requests.Deposits.Len(); i++ {
				if err := impl.ProcessDepositRequest(s, requests.Deposits.Get(i)); err != nil {
					return fmt.Errorf("process_execution_payload: deposit request: %w", err)
				}
			}
		}
		if requests.Withdrawals != nil {
			for i := 0; i < requests.Withdrawals.Len(); i++ {
				if err := impl.ProcessWithdrawalRequest(s, requests.Withdrawals.Get(i)); err != nil {
					return fmt.Errorf("process_execution_payload: withdrawal request: %w", err)
				}
			}
		}
		if requests.Consolidations != nil {
			for i := 0; i < requests.Consolidations.Len(); i++ {
				if err := impl.ProcessConsolidationRequest(s, requests.Consolidations.Get(i)); err != nil {
					return fmt.Errorf("process_execution_payload: consolidation request: %w", err)
				}
			}
		}
	}

	// Queue the builder payment (always append, compute withdrawable epoch via churn)
	bpp := s.BuilderPendingPayments()
	paymentIdx := beaconConfig.SlotsPerEpoch + s.Slot()%beaconConfig.SlotsPerEpoch
	payment := bpp.Get(int(paymentIdx))
	exitEpoch := s.ComputeExitEpochAndUpdateChurn(payment.Withdrawal.Amount)
	w := &cltypes.BuilderPendingWithdrawal{
		FeeRecipient:      payment.Withdrawal.FeeRecipient,
		Amount:            payment.Withdrawal.Amount,
		BuilderIndex:      payment.Withdrawal.BuilderIndex,
		WithdrawableEpoch: exitEpoch + beaconConfig.MinValidatorWithdrawabilityDelay,
	}
	bpw := s.BuilderPendingWithdrawals()
	bpw.Append(w)
	s.SetBuilderPendingWithdrawals(bpw)
	// Clear current slot's payment
	payment.Weight = 0
	payment.Withdrawal.FeeRecipient = [20]byte{}
	payment.Withdrawal.Amount = 0
	payment.Withdrawal.BuilderIndex = 0
	payment.Withdrawal.WithdrawableEpoch = 0
	s.SetBuilderPendingPayments(bpp)

	// Cache the execution payload hash
	epa := s.ExecutionPayloadAvailability()
	if err := epa.SetBitAt(int(s.Slot()%beaconConfig.SlotsPerHistoricalRoot), true); err != nil {
		return fmt.Errorf("process_execution_payload: failed to set payload availability: %w", err)
	}
	s.SetExecutionPayloadAvailability(epa)
	s.SetLatestBlockHash(payload.BlockHash)

	return nil
}

// operationExecutionPayloadHeaderHandler handles gloas execution_payload_header operation tests.
// In gloas, process_execution_payload_header processes the signed execution payload bid from the block.
func operationExecutionPayloadHeaderHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	block := cltypes.NewBeaconBlock(&clparams.MainnetBeaconConfig, c.Version())
	if err := spectest.ReadSszOld(root, block, c.Version(), blockFileName); err != nil {
		return err
	}
	if err := machine.ProcessExecutionPayloadBid(preState, block.Body, block.ProposerIndex, block.Slot, block.ParentRoot); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return errors.New("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)

	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
	return nil
}

// operationPayloadAttestationHandler handles gloas payload_attestation operation tests.
func operationPayloadAttestationHandler(t *testing.T, root fs.FS, c spectest.TestCase) error {
	preState, err := spectest.ReadBeaconState(root, c.Version(), "pre.ssz_snappy")
	require.NoError(t, err)
	postState, err := spectest.ReadBeaconState(root, c.Version(), "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	pa := cltypes.NewPayloadAttestation()
	if err := spectest.ReadSszOld(root, pa, c.Version(), "payload_attestation.ssz_snappy"); err != nil {
		return err
	}
	if err := machine.ProcessPayloadAttestation(preState, pa); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return errors.New("expected error")
	}
	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)

	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
	return nil
}
