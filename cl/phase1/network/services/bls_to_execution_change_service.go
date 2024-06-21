package services

import (
	"bytes"
	"context"
	"fmt"

	"github.com/Giulio2002/bls"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/ledgerwatch/erigon/cl/utils"
)

type blsToExecutionChangeService struct {
	operationsPool    pool.OperationsPool
	emitters          *beaconevents.Emitters
	syncedDataManager *synced_data.SyncedDataManager
	beaconCfg         *clparams.BeaconChainConfig
}

func NewBLSToExecutionChangeService(
	operationsPool pool.OperationsPool,
	emitters *beaconevents.Emitters,
	syncedDataManager *synced_data.SyncedDataManager,
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
	state := s.syncedDataManager.HeadState()
	if state == nil {
		return ErrIgnore
	}

	// [IGNORE] current_epoch >= CAPELLA_FORK_EPOCH, where current_epoch is defined by the current wall-clock time.
	if !(state.Version() >= clparams.CapellaVersion) {
		return ErrIgnore
	}
	// ref: https://github.com/ethereum/consensus-specs/blob/dev/specs/capella/beacon-chain.md#new-process_bls_to_execution_change
	// assert address_change.validator_index < len(state.validators)
	validator, err := state.ValidatorForValidatorIndex(int(change.ValidatorIndex))
	if err != nil {
		return fmt.Errorf("unable to retrieve state: %v", err)
	}
	wc := validator.WithdrawalCredentials()

	// assert validator.withdrawal_credentials[:1] == BLS_WITHDRAWAL_PREFIX
	if wc[0] != byte(s.beaconCfg.BLSWithdrawalPrefixByte) {
		return fmt.Errorf("invalid withdrawal credentials prefix")
	}

	// assert validator.withdrawal_credentials[1:] == hash(address_change.from_bls_pubkey)[1:]
	// Perform full validation if requested.
	// Check the validator's withdrawal credentials against the provided message.
	hashedFrom := utils.Sha256(change.From[:])
	if !bytes.Equal(hashedFrom[1:], wc[1:]) {
		return fmt.Errorf("invalid withdrawal credentials")
	}

	// assert bls.Verify(address_change.from_bls_pubkey, signing_root, signed_address_change.signature)
	genesisValidatorRoot := state.GenesisValidatorsRoot()
	domain, err := fork.ComputeDomain(s.beaconCfg.DomainBLSToExecutionChange[:], utils.Uint32ToBytes4(uint32(s.beaconCfg.GenesisForkVersion)), genesisValidatorRoot)
	if err != nil {
		return err
	}
	signedRoot, err := fork.ComputeSigningRoot(change, domain)
	if err != nil {
		return err
	}
	valid, err := bls.Verify(msg.Signature[:], signedRoot[:], change.From[:])
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("invalid signature")
	}

	// validator.withdrawal_credentials = (
	//    ETH1_ADDRESS_WITHDRAWAL_PREFIX
	//    + b'\x00' * 11
	//    + address_change.to_execution_address
	// )
	newWc := libcommon.Hash{}
	newWc[0] = byte(s.beaconCfg.ETH1AddressWithdrawalPrefixByte)
	copy(wc[1:], make([]byte, 11))
	copy(wc[12:], change.To[:])
	state.SetWithdrawalCredentialForValidatorAtIndex(int(change.ValidatorIndex), newWc)

	s.operationsPool.BLSToExecutionChangesPool.Insert(msg.Signature, msg)
	return nil
}
