package services

import (
	"context"
	"fmt"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
	"github.com/pkg/errors"
)

type voluntaryExitService struct {
	operationsPool    pool.OperationsPool
	emitters          *beaconevents.Emitters
	syncedDataManager *synced_data.SyncedDataManager
	beaconCfg         *clparams.BeaconChainConfig
	ethClock          eth_clock.EthereumClock
}

func NewVoluntaryExitService(
	operationsPool pool.OperationsPool,
	emitters *beaconevents.Emitters,
	syncedDataManager *synced_data.SyncedDataManager,
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
	state := s.syncedDataManager.HeadState()
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
		return fmt.Errorf("validator is not active")
	}

	// Verify exit has not been initiated
	// assert validator.exit_epoch == FAR_FUTURE_EPOCH
	if !(val.ExitEpoch() == s.beaconCfg.FarFutureEpoch) {
		return fmt.Errorf("verify exit has not been initiated. exitEpoch: %d, farFutureEpoch: %d", val.ExitEpoch(), s.beaconCfg.FarFutureEpoch)
	}

	// Exits must specify an epoch when they become valid; they are not valid before then
	// assert get_current_epoch(state) >= voluntary_exit.epoch
	if !(curEpoch >= voluntaryExit.Epoch) {
		return fmt.Errorf("exits must specify an epoch when they become valid; they are not valid before then")
	}

	// Verify the validator has been active long enough
	// assert get_current_epoch(state) >= validator.activation_epoch + SHARD_COMMITTEE_PERIOD
	if !(curEpoch >= val.ActivationEpoch()+s.beaconCfg.ShardCommitteePeriod) {
		return fmt.Errorf("verify the validator has been active long enough")
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
		domain, err = fork.ComputeDomain(domainType[:], utils.Uint32ToBytes4(uint32(state.BeaconConfig().CapellaForkVersion)), state.GenesisValidatorsRoot())
	}
	if err != nil {
		return err
	}
	signingRoot, err := fork.ComputeSigningRoot(voluntaryExit, domain)
	if err != nil {
		return err
	}
	if valid, err := bls.Verify(msg.Signature[:], signingRoot[:], pk[:]); err != nil {
		return err
	} else if !valid {
		return errors.New("ProcessVoluntaryExit: BLS verification failed")
	}

	s.operationsPool.VoluntaryExitsPool.Insert(voluntaryExit.ValidatorIndex, msg)

	return nil
}
