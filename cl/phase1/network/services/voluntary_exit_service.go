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
	"context"
	"fmt"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/pkg/errors"
)

type voluntaryExitService struct {
	operationsPool    pool.OperationsPool
	emitters          *beaconevents.Emitters
	syncedDataManager synced_data.SyncedData
	beaconCfg         *clparams.BeaconChainConfig
	ethClock          eth_clock.EthereumClock
}

func NewVoluntaryExitService(
	operationsPool pool.OperationsPool,
	emitters *beaconevents.Emitters,
	syncedDataManager synced_data.SyncedData,
	beaconCfg *clparams.BeaconChainConfig,
	ethClock eth_clock.EthereumClock,
) VoluntaryExitService {
	return &voluntaryExitService{
		operationsPool:    operationsPool,
		emitters:          emitters,
		syncedDataManager: syncedDataManager,
		beaconCfg:         beaconCfg,
		ethClock:          ethClock,
	}
}

func (s *voluntaryExitService) ProcessMessage(ctx context.Context, subnet *uint64, msg *cltypes.SignedVoluntaryExit) error {
	// ref: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#voluntary_exit
	voluntaryExit := msg.VoluntaryExit
	defer s.emitters.Publish("voluntary_exit", voluntaryExit)

	// [IGNORE] The voluntary exit is the first valid voluntary exit received for the validator with index signed_voluntary_exit.message.validator_index.
	if s.operationsPool.VoluntaryExitsPool.Has(voluntaryExit.ValidatorIndex) {
		return ErrIgnore
	}

	// ref: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#voluntary-exits
	// def process_voluntary_exit(state: BeaconState, signed_voluntary_exit: SignedVoluntaryExit) -> None:
	state := s.syncedDataManager.HeadStateReader()
	if state == nil {
		return ErrIgnore
	}
	val, err := state.ValidatorForValidatorIndex(int(voluntaryExit.ValidatorIndex))
	if err != nil {
		return ErrIgnore
	}
	curEpoch := s.ethClock.GetCurrentEpoch()

	// Verify the validator is active
	// assert is_active_validator(validator, get_current_epoch(state))
	if !val.Active(curEpoch) {
		return errors.New("validator is not active")
	}

	// Verify exit has not been initiated
	// assert validator.exit_epoch == FAR_FUTURE_EPOCH
	if !(val.ExitEpoch() == s.beaconCfg.FarFutureEpoch) {
		return fmt.Errorf("verify exit has not been initiated. exitEpoch: %d, farFutureEpoch: %d", val.ExitEpoch(), s.beaconCfg.FarFutureEpoch)
	}

	// Exits must specify an epoch when they become valid; they are not valid before then
	// assert get_current_epoch(state) >= voluntary_exit.epoch
	if !(curEpoch >= voluntaryExit.Epoch) {
		return errors.New("exits must specify an epoch when they become valid; they are not valid before then")
	}

	// Verify the validator has been active long enough
	// assert get_current_epoch(state) >= validator.activation_epoch + SHARD_COMMITTEE_PERIOD
	if !(curEpoch >= val.ActivationEpoch()+s.beaconCfg.ShardCommitteePeriod) {
		return errors.New("verify the validator has been active long enough")
	}

	// Verify signature
	// domain = get_domain(state, DOMAIN_VOLUNTARY_EXIT, voluntary_exit.epoch)
	// signing_root = compute_signing_root(voluntary_exit, domain)
	// assert bls.Verify(validator.pubkey, signing_root, signed_voluntary_exit.signature)
	pk := val.PublicKey()
	domainType := s.beaconCfg.DomainVoluntaryExit
	var domain []byte
	if state.Version() < clparams.DenebVersion {
		domain, err = state.GetDomain(domainType, voluntaryExit.Epoch)
	} else if state.Version() >= clparams.DenebVersion {
		domain, err = fork.ComputeDomain(domainType[:], utils.Uint32ToBytes4(uint32(s.beaconCfg.CapellaForkVersion)), state.GenesisValidatorsRoot())
	}
	if err != nil {
		return err
	}
	signingRoot, err := computeSigningRoot(voluntaryExit, domain)
	if err != nil {
		return err
	}
	if valid, err := blsVerify(msg.Signature[:], signingRoot[:], pk[:]); err != nil {
		return err
	} else if !valid {
		return errors.New("ProcessVoluntaryExit: BLS verification failed")
	}

	s.operationsPool.VoluntaryExitsPool.Insert(voluntaryExit.ValidatorIndex, msg)

	return nil
}
