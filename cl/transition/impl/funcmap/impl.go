package funcmap

import (
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/transition/machine"
	"github.com/ledgerwatch/erigon/core/types"
)

var _ machine.Interface = (*Impl)(nil)

type Impl struct {
	FnVerifyBlockSignature                    func(s *state.BeaconState, block *cltypes.SignedBeaconBlock) error
	FnVerifyTransition                        func(s *state.BeaconState, block *cltypes.BeaconBlock) error
	FnProcessSlots                            func(s *state.BeaconState, slot uint64) error
	FnProcessBlockHeader                      func(s *state.BeaconState, block *cltypes.BeaconBlock) error
	FnProcessWithdrawals                      func(s *state.BeaconState, withdrawals *solid.ListSSZ[*types.Withdrawal]) error
	FnProcessExecutionPayload                 func(s *state.BeaconState, payload *cltypes.Eth1Block) error
	FnProcessRandao                           func(s *state.BeaconState, randao [96]byte, proposerIndex uint64) error
	FnProcessEth1Data                         func(state *state.BeaconState, eth1Data *cltypes.Eth1Data) error
	FnProcessSyncAggregate                    func(s *state.BeaconState, sync *cltypes.SyncAggregate) error
	FnVerifyKzgCommitmentsAgainstTransactions func(transactions *solid.TransactionsSSZ, kzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment]) (bool, error)
	FnProcessProposerSlashing                 func(s *state.BeaconState, propSlashing *cltypes.ProposerSlashing) error
	FnProcessAttesterSlashing                 func(s *state.BeaconState, attSlashing *cltypes.AttesterSlashing) error
	FnProcessAttestations                     func(s *state.BeaconState, attestations *solid.ListSSZ[*solid.Attestation]) error
	FnProcessDeposit                          func(s *state.BeaconState, deposit *cltypes.Deposit) error
	FnProcessVoluntaryExit                    func(s *state.BeaconState, signedVoluntaryExit *cltypes.SignedVoluntaryExit) error
	FnProcessBlsToExecutionChange             func(state *state.BeaconState, signedChange *cltypes.SignedBLSToExecutionChange) error
}

func (i Impl) VerifyBlockSignature(s *state.BeaconState, block *cltypes.SignedBeaconBlock) error {
	return i.FnVerifyBlockSignature(s, block)
}

func (i Impl) VerifyTransition(s *state.BeaconState, block *cltypes.BeaconBlock) error {
	return i.FnVerifyTransition(s, block)
}

func (i Impl) ProcessBlockHeader(s *state.BeaconState, block *cltypes.BeaconBlock) error {
	return i.FnProcessBlockHeader(s, block)
}

func (i Impl) ProcessWithdrawals(s *state.BeaconState, withdrawals *solid.ListSSZ[*types.Withdrawal]) error {
	return i.FnProcessWithdrawals(s, withdrawals)
}

func (i Impl) ProcessExecutionPayload(s *state.BeaconState, payload *cltypes.Eth1Block) error {
	return i.FnProcessExecutionPayload(s, payload)
}

func (i Impl) ProcessRandao(s *state.BeaconState, randao [96]byte, proposerIndex uint64) error {
	return i.FnProcessRandao(s, randao, proposerIndex)
}

func (i Impl) ProcessEth1Data(state *state.BeaconState, eth1Data *cltypes.Eth1Data) error {
	return i.FnProcessEth1Data(state, eth1Data)
}

func (i Impl) ProcessSyncAggregate(s *state.BeaconState, sync *cltypes.SyncAggregate) error {
	return i.FnProcessSyncAggregate(s, sync)
}

func (i Impl) VerifyKzgCommitmentsAgainstTransactions(transactions *solid.TransactionsSSZ, kzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment]) (bool, error) {
	return i.FnVerifyKzgCommitmentsAgainstTransactions(transactions, kzgCommitments)
}

func (i Impl) ProcessProposerSlashing(s *state.BeaconState, propSlashing *cltypes.ProposerSlashing) error {
	return i.FnProcessProposerSlashing(s, propSlashing)
}

func (i Impl) ProcessAttesterSlashing(s *state.BeaconState, attSlashing *cltypes.AttesterSlashing) error {
	return i.FnProcessAttesterSlashing(s, attSlashing)
}

func (i Impl) ProcessAttestations(s *state.BeaconState, attestations *solid.ListSSZ[*solid.Attestation]) error {
	return i.FnProcessAttestations(s, attestations)
}

func (i Impl) ProcessDeposit(s *state.BeaconState, deposit *cltypes.Deposit) error {
	return i.FnProcessDeposit(s, deposit)
}

func (i Impl) ProcessVoluntaryExit(s *state.BeaconState, signedVoluntaryExit *cltypes.SignedVoluntaryExit) error {
	return i.FnProcessVoluntaryExit(s, signedVoluntaryExit)
}

func (i Impl) ProcessBlsToExecutionChange(state *state.BeaconState, signedChange *cltypes.SignedBLSToExecutionChange) error {
	return i.FnProcessBlsToExecutionChange(state, signedChange)
}

func (i Impl) ProcessSlots(s *state.BeaconState, slot uint64) error {
	return i.FnProcessSlots(s, slot)
}
