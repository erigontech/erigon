package transition

import (
	"bytes"
	"fmt"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"

	"github.com/Giulio2002/bls"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
)

// ProcessBlsToExecutionChange processes a BLSToExecutionChange message by updating a validator's withdrawal credentials.
func ProcessBlsToExecutionChange(state *state.BeaconState, signedChange *cltypes.SignedBLSToExecutionChange, fullValidation bool) error {
	change := signedChange.Message

	beaconConfig := state.BeaconConfig()
	validator, err := state.ValidatorForValidatorIndex(int(change.ValidatorIndex))
	if err != nil {
		return err
	}

	// Perform full validation if requested.
	wc := validator.WithdrawalCredentials()
	if fullValidation {
		// Check the validator's withdrawal credentials prefix.
		if wc[0] != beaconConfig.BLSWithdrawalPrefixByte {
			return fmt.Errorf("invalid withdrawal credentials prefix")
		}

		// Check the validator's withdrawal credentials against the provided message.
		hashedFrom := utils.Keccak256(change.From[:])
		if !bytes.Equal(hashedFrom[1:], wc[1:]) {
			return fmt.Errorf("invalid withdrawal credentials")
		}

		// Compute the signing domain and verify the message signature.
		domain, err := fork.ComputeDomain(beaconConfig.DomainBLSToExecutionChange[:], utils.Uint32ToBytes4(beaconConfig.GenesisForkVersion), state.GenesisValidatorsRoot())
		if err != nil {
			return err
		}
		signedRoot, err := fork.ComputeSigningRoot(change, domain)
		if err != nil {
			return err
		}
		valid, err := bls.Verify(signedChange.Signature[:], signedRoot[:], change.From[:])
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("invalid signature")
		}
	}
	credentials := wc
	// Reset the validator's withdrawal credentials.
	credentials[0] = beaconConfig.ETH1AddressWithdrawalPrefixByte
	copy(credentials[1:], make([]byte, 11))
	copy(credentials[12:], change.To[:])

	// Update the state with the modified validator.
	state.SetWithdrawalCredentialForValidatorAtIndex(int(change.ValidatorIndex), credentials)
	return nil
}
