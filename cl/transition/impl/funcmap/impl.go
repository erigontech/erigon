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

package funcmap

/*
import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/transition/machine"
)

var _ machine.Interface = (*Impl)(nil)

type Impl struct {
	FnVerifyBlockSignature        func(s abstract.BeaconState, block *cltypes.SignedBeaconBlock) error
	FnVerifyTransition            func(s abstract.BeaconState, block *cltypes.BeaconBlock) error
	FnProcessSlots                func(s abstract.BeaconState, slot uint64) error
	FnProcessBlockHeader          func(s abstract.BeaconState, slot, proposerIndex uint64, parentRoot common.Hash, bodyRoot [32]byte) error
	FnProcessWithdrawals          func(s abstract.BeaconState, withdrawals *solid.ListSSZ[*cltypes.Withdrawal]) error
	FnProcessExecutionPayload     func(s abstract.BeaconState, parentHash, prevRandao common.Hash, time uint64, payloadHeader *cltypes.Eth1Header) error
	FnProcessRandao               func(s abstract.BeaconState, randao [96]byte, proposerIndex uint64) error
	FnProcessEth1Data             func(state abstract.BeaconState, eth1Data *cltypes.Eth1Data) error
	FnProcessSyncAggregate        func(s abstract.BeaconState, sync *cltypes.SyncAggregate) error
	FnProcessProposerSlashing     func(s abstract.BeaconState, propSlashing *cltypes.ProposerSlashing) error
	FnProcessAttesterSlashing     func(s abstract.BeaconState, attSlashing *cltypes.AttesterSlashing) error
	FnProcessAttestations         func(s abstract.BeaconState, attestations *solid.ListSSZ[*solid.Attestation]) error
	FnProcessDeposit              func(s abstract.BeaconState, deposit *cltypes.Deposit) error
	FnProcessVoluntaryExit        func(s abstract.BeaconState, signedVoluntaryExit *cltypes.SignedVoluntaryExit) error
	FnProcessBlsToExecutionChange func(state abstract.BeaconState, signedChange *cltypes.SignedBLSToExecutionChange) error
	FnFullValidate                func() bool
}

func (i Impl) FullValidate() bool {
	return i.FnFullValidate()
}

func (i Impl) VerifyBlockSignature(s abstract.BeaconState, block *cltypes.SignedBeaconBlock) error {
	return i.FnVerifyBlockSignature(s, block)
}

func (i Impl) VerifyTransition(s abstract.BeaconState, block *cltypes.BeaconBlock) error {
	return i.FnVerifyTransition(s, block)
}

func (i Impl) ProcessBlockHeader(s abstract.BeaconState, slot, proposerIndex uint64, parentRoot common.Hash, bodyRoot [32]byte) error {
	return i.FnProcessBlockHeader(s, slot, proposerIndex, parentRoot, bodyRoot)
}

func (i Impl) ProcessWithdrawals(s abstract.BeaconState, withdrawals *solid.ListSSZ[*cltypes.Withdrawal]) error {
	return i.FnProcessWithdrawals(s, withdrawals)
}

func (i Impl) ProcessExecutionPayload(s abstract.BeaconState, parentHash, prevRandao common.Hash, time uint64, payloadHeader *cltypes.Eth1Header) error {
	return i.FnProcessExecutionPayload(s, parentHash, prevRandao, time, payloadHeader)
}

func (i Impl) ProcessRandao(s abstract.BeaconState, randao [96]byte, proposerIndex uint64) error {
	return i.FnProcessRandao(s, randao, proposerIndex)
}

func (i Impl) ProcessEth1Data(state abstract.BeaconState, eth1Data *cltypes.Eth1Data) error {
	return i.FnProcessEth1Data(state, eth1Data)
}

func (i Impl) ProcessSyncAggregate(s abstract.BeaconState, sync *cltypes.SyncAggregate) error {
	return i.FnProcessSyncAggregate(s, sync)
}

func (i Impl) ProcessProposerSlashing(s abstract.BeaconState, propSlashing *cltypes.ProposerSlashing) error {
	return i.FnProcessProposerSlashing(s, propSlashing)
}

func (i Impl) ProcessAttesterSlashing(s abstract.BeaconState, attSlashing *cltypes.AttesterSlashing) error {
	return i.FnProcessAttesterSlashing(s, attSlashing)
}

func (i Impl) ProcessAttestations(s abstract.BeaconState, attestations *solid.ListSSZ[*solid.Attestation]) error {
	return i.FnProcessAttestations(s, attestations)
}

func (i Impl) ProcessDeposit(s abstract.BeaconState, deposit *cltypes.Deposit) error {
	return i.FnProcessDeposit(s, deposit)
}

func (i Impl) ProcessVoluntaryExit(s abstract.BeaconState, signedVoluntaryExit *cltypes.SignedVoluntaryExit) error {
	return i.FnProcessVoluntaryExit(s, signedVoluntaryExit)
}

func (i Impl) ProcessBlsToExecutionChange(state abstract.BeaconState, signedChange *cltypes.SignedBLSToExecutionChange) error {
	return i.FnProcessBlsToExecutionChange(state, signedChange)
}

func (i Impl) ProcessSlots(s abstract.BeaconState, slot uint64) error {
	return i.FnProcessSlots(s, slot)
}
*/
