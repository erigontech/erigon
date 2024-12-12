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

// Package machine is the interface for eth2 state transition
package machine

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
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
	ProcessBlockHeader(s abstract.BeaconState, slot, proposerIndex uint64, parentRoot common.Hash, bodyRoot [32]byte) error
	ProcessWithdrawals(s abstract.BeaconState, withdrawals *solid.ListSSZ[*cltypes.Withdrawal]) error
	ProcessExecutionPayload(s abstract.BeaconState, body cltypes.GenericBeaconBody) error
	ProcessRandao(s abstract.BeaconState, randao [96]byte, proposerIndex uint64) error
	ProcessEth1Data(state abstract.BeaconState, eth1Data *cltypes.Eth1Data) error
	ProcessSyncAggregate(s abstract.BeaconState, sync *cltypes.SyncAggregate) error
}

type BlockOperationProcessor interface {
	ProcessProposerSlashing(s abstract.BeaconState, propSlashing *cltypes.ProposerSlashing) error
	ProcessAttesterSlashing(s abstract.BeaconState, attSlashing *cltypes.AttesterSlashing) error
	ProcessAttestations(s abstract.BeaconState, attestations *solid.ListSSZ[*solid.Attestation]) error
	ProcessDeposit(s abstract.BeaconState, deposit *cltypes.Deposit) error
	ProcessVoluntaryExit(s abstract.BeaconState, signedVoluntaryExit *cltypes.SignedVoluntaryExit) error
	ProcessBlsToExecutionChange(state abstract.BeaconState, signedChange *cltypes.SignedBLSToExecutionChange) error
	ProcessDepositRequest(s abstract.BeaconState, depositRequest *solid.DepositRequest) error
	ProcessWithdrawalRequest(s abstract.BeaconState, withdrawalRequest *solid.WithdrawalRequest) error
	ProcessConsolidationRequest(s abstract.BeaconState, consolidationRequest *solid.ConsolidationRequest) error
	FullValidate() bool
}
