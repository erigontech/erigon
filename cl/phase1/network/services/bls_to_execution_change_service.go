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

package services

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils"
)

type blsToExecutionChangeService struct {
	operationsPool    pool.OperationsPool
	emitters          *beaconevents.Emitters
	syncedDataManager synced_data.SyncedData
	beaconCfg         *clparams.BeaconChainConfig
}

func NewBLSToExecutionChangeService(
	operationsPool pool.OperationsPool,
	emitters *beaconevents.Emitters,
	syncedDataManager synced_data.SyncedData,
	beaconCfg *clparams.BeaconChainConfig,
) BLSToExecutionChangeService {
	return &blsToExecutionChangeService{
		operationsPool:    operationsPool,
		emitters:          emitters,
		syncedDataManager: syncedDataManager,
		beaconCfg:         beaconCfg,
	}
}

func (s *blsToExecutionChangeService) ProcessMessage(ctx context.Context, subnet *uint64, msg *cltypes.SignedBLSToExecutionChange) error {
	// https://github.com/ethereum/consensus-specs/blob/dev/specs/capella/p2p-interface.md#bls_to_execution_change
	defer s.emitters.Publish("bls_to_execution_change", msg)
	// [IGNORE] The signed_bls_to_execution_change is the first valid signed bls to execution change received
	// for the validator with index signed_bls_to_execution_change.message.validator_index.
	if s.operationsPool.BLSToExecutionChangesPool.Has(msg.Signature) {
		return ErrIgnore
	}
	change := msg.Message
	stateReader := s.syncedDataManager.HeadStateReader()
	if stateReader == nil {
		return ErrIgnore
	}
	stateMutator := s.syncedDataManager.HeadStateMutator()
	if stateMutator == nil {
		return ErrIgnore
	}

	// [IGNORE] current_epoch >= CAPELLA_FORK_EPOCH, where current_epoch is defined by the current wall-clock time.
	if !(stateReader.Version() >= clparams.CapellaVersion) {
		return ErrIgnore
	}
	// ref: https://github.com/ethereum/consensus-specs/blob/dev/specs/capella/beacon-chain.md#new-process_bls_to_execution_change
	// assert address_change.validator_index < len(state.validators)
	validator, err := stateReader.ValidatorForValidatorIndex(int(change.ValidatorIndex))
	if err != nil {
		return fmt.Errorf("unable to retrieve validator: %v", err)
	}
	wc := validator.WithdrawalCredentials()

	// assert validator.withdrawal_credentials[:1] == BLS_WITHDRAWAL_PREFIX
	if wc[0] != byte(s.beaconCfg.BLSWithdrawalPrefixByte) {
		return errors.New("invalid withdrawal credentials prefix")
	}

	// assert validator.withdrawal_credentials[1:] == hash(address_change.from_bls_pubkey)[1:]
	// Perform full validation if requested.
	// Check the validator's withdrawal credentials against the provided message.
	hashedFrom := utils.Sha256(change.From[:])
	if !bytes.Equal(hashedFrom[1:], wc[1:]) {
		return errors.New("invalid withdrawal credentials hash")
	}

	// assert bls.Verify(address_change.from_bls_pubkey, signing_root, signed_address_change.signature)
	genesisValidatorRoot := stateReader.GenesisValidatorsRoot()
	domain, err := fork.ComputeDomain(s.beaconCfg.DomainBLSToExecutionChange[:], utils.Uint32ToBytes4(uint32(s.beaconCfg.GenesisForkVersion)), genesisValidatorRoot)
	if err != nil {
		return err
	}
	signedRoot, err := computeSigningRoot(change, domain)
	if err != nil {
		return err
	}
	valid, err := blsVerify(msg.Signature[:], signedRoot[:], change.From[:])
	if err != nil {
		return err
	}
	if !valid {
		return errors.New("invalid signature")
	}

	// validator.withdrawal_credentials = (
	//    ETH1_ADDRESS_WITHDRAWAL_PREFIX
	//    + b'\x00' * 11
	//    + address_change.to_execution_address
	// )
	newWc := libcommon.Hash{}
	newWc[0] = byte(s.beaconCfg.ETH1AddressWithdrawalPrefixByte)
	copy(newWc[1:], make([]byte, 11))
	copy(newWc[12:], change.To[:])
	stateMutator.SetWithdrawalCredentialForValidatorAtIndex(int(change.ValidatorIndex), newWc)

	s.operationsPool.BLSToExecutionChangesPool.Insert(msg.Signature, msg)
	return nil
}
