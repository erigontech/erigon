// Package machine is the interface for eth2 state transition
package machine

import (
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/core/types"
)

type Interface interface {
	BlockValidator
	BlockProcessor
	SlotProcessor
}

type BlockProcessor interface {
	BlockHeaderProcessor
	BlockOperationProcessor
}

type BlockValidator interface {
	VerifyBlockSignature(s *state.BeaconState, block *cltypes.SignedBeaconBlock) error
	VerifyTransition(s *state.BeaconState, block *cltypes.BeaconBlock) error
}

type SlotProcessor interface {
	ProcessSlots(s *state.BeaconState, slot uint64) error
}

type BlockHeaderProcessor interface {
	ProcessBlockHeader(s *state.BeaconState, block *cltypes.BeaconBlock) error
	ProcessWithdrawals(s *state.BeaconState, withdrawals *solid.ListSSZ[*types.Withdrawal]) error
	ProcessExecutionPayload(s *state.BeaconState, payload *cltypes.Eth1Block) error
	ProcessRandao(s *state.BeaconState, randao [96]byte, proposerIndex uint64) error
	ProcessEth1Data(state *state.BeaconState, eth1Data *cltypes.Eth1Data) error
	ProcessSyncAggregate(s *state.BeaconState, sync *cltypes.SyncAggregate) error
	VerifyKzgCommitmentsAgainstTransactions(transactions *solid.TransactionsSSZ, kzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment]) (bool, error)
}

type BlockOperationProcessor interface {
	ProcessProposerSlashing(s *state.BeaconState, propSlashing *cltypes.ProposerSlashing) error
	ProcessAttesterSlashing(s *state.BeaconState, attSlashing *cltypes.AttesterSlashing) error
	ProcessAttestations(s *state.BeaconState, attestations *solid.ListSSZ[*solid.Attestation]) error
	ProcessDeposit(s *state.BeaconState, deposit *cltypes.Deposit) error
	ProcessVoluntaryExit(s *state.BeaconState, signedVoluntaryExit *cltypes.SignedVoluntaryExit) error
	ProcessBlsToExecutionChange(state *state.BeaconState, signedChange *cltypes.SignedBLSToExecutionChange) error
}
