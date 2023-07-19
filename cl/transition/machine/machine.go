// Package machine is the interface for eth2 state transition
package machine

import (
	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
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
	VerifyBlockSignature(s abstract.BeaconState, block *cltypes.SignedBeaconBlock) error
	VerifyTransition(s abstract.BeaconState, block *cltypes.BeaconBlock) error
}

type SlotProcessor interface {
	ProcessSlots(s abstract.BeaconState, slot uint64) error
}

type BlockHeaderProcessor interface {
	ProcessBlockHeader(s abstract.BeaconState, block *cltypes.BeaconBlock) error
	ProcessWithdrawals(s abstract.BeaconState, withdrawals *solid.ListSSZ[*types.Withdrawal]) error
	ProcessExecutionPayload(s abstract.BeaconState, payload *cltypes.Eth1Block) error
	ProcessRandao(s abstract.BeaconState, randao [96]byte, proposerIndex uint64) error
	ProcessEth1Data(state abstract.BeaconState, eth1Data *cltypes.Eth1Data) error
	ProcessSyncAggregate(s abstract.BeaconState, sync *cltypes.SyncAggregate) error
	VerifyKzgCommitmentsAgainstTransactions(transactions *solid.TransactionsSSZ, kzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment]) (bool, error)
}

type BlockOperationProcessor interface {
	ProcessProposerSlashing(s abstract.BeaconState, propSlashing *cltypes.ProposerSlashing) error
	ProcessAttesterSlashing(s abstract.BeaconState, attSlashing *cltypes.AttesterSlashing) error
	ProcessAttestations(s abstract.BeaconState, attestations *solid.ListSSZ[*solid.Attestation]) error
	ProcessDeposit(s abstract.BeaconState, deposit *cltypes.Deposit) error
	ProcessVoluntaryExit(s abstract.BeaconState, signedVoluntaryExit *cltypes.SignedVoluntaryExit) error
	ProcessBlsToExecutionChange(state abstract.BeaconState, signedChange *cltypes.SignedBLSToExecutionChange) error
}
