package transition

import (
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

// processBlock takes a block and transition said block. Important: it assumes execution payload is correct.
func (s *StateTransistor) processBlock(signedBlock *cltypes.SignedBeaconBlock) error {
	block := signedBlock.Block
	if signedBlock.Version() != s.state.Version() {
		return fmt.Errorf("wrong state version for block at slot %d", block.Slot)
	}
	if err := s.ProcessBlockHeader(block); err != nil {
		return fmt.Errorf("ProcessBlockHeader: %s", err)
	}
	if s.state.Version() >= clparams.BellatrixVersion {
		// Set execution header accordingly to state.
		s.state.SetLatestExecutionPayloadHeader(block.Body.ExecutionPayload.Header)
	}
	if err := s.ProcessRandao(block.Body.RandaoReveal); err != nil {
		return fmt.Errorf("ProcessRandao: %s", err)
	}
	if err := s.ProcessEth1Data(block.Body.Eth1Data); err != nil {
		return fmt.Errorf("ProcessEth1Data: %s", err)
	}
	// Do operationns
	if err := s.processOperations(block.Body); err != nil {
		return fmt.Errorf("processOperations: %s", err)
	}
	// Process altair data
	if s.state.Version() >= clparams.AltairVersion {
		if err := s.ProcessSyncAggregate(block.Body.SyncAggregate); err != nil {
			return fmt.Errorf("ProcessSyncAggregate: %s", err)
		}
	}
	return nil
}

func (s *StateTransistor) processOperations(blockBody *cltypes.BeaconBody) error {
	if len(blockBody.Deposits) != int(s.maximumDeposits()) {
		return errors.New("outstanding deposits do not match maximum deposits")
	}
	// Process each proposer slashing
	for _, slashing := range blockBody.ProposerSlashings {
		if err := s.ProcessProposerSlashing(slashing); err != nil {
			return fmt.Errorf("ProcessProposerSlashing: %s", err)
		}
	}
	// Process each attester slashing
	for _, slashing := range blockBody.AttesterSlashings {
		if err := s.ProcessAttesterSlashing(slashing); err != nil {
			return fmt.Errorf("ProcessAttesterSlashing: %s", err)
		}
	}
	// Process each attestations
	for _, att := range blockBody.Attestations {
		if err := s.ProcessAttestation(att); err != nil {
			return fmt.Errorf("ProcessAttestation: %s", err)
		}
	}
	// Process each deposit
	for _, dep := range blockBody.Deposits {
		if err := s.ProcessDeposit(dep); err != nil {
			return fmt.Errorf("ProcessDeposit: %s", err)
		}
	}
	// Process each voluntary exit.
	for _, exit := range blockBody.VoluntaryExits {
		if err := s.ProcessVoluntaryExit(exit); err != nil {
			return fmt.Errorf("ProcessVoluntaryExit: %s", err)
		}
	}
	return nil
}

func (s *StateTransistor) maximumDeposits() (maxDeposits uint64) {
	maxDeposits = s.state.Eth1Data().DepositCount - s.state.Eth1DepositIndex()
	if maxDeposits > s.beaconConfig.MaxDeposits {
		maxDeposits = s.beaconConfig.MaxDeposits
	}
	return
}
