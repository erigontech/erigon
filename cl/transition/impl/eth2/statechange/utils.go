package statechange

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
)

func IsValidDepositSignature(
	depositData *cltypes.DepositData,
	cfg *clparams.BeaconChainConfig) (bool, error) {
	// Agnostic domain.
	domain, err := fork.ComputeDomain(
		cfg.DomainDeposit[:],
		utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)),
		[32]byte{},
	)
	if err != nil {
		return false, err
	}
	depositMessageRoot, err := depositData.MessageHash()
	if err != nil {
		return false, err
	}
	signedRoot := utils.Sha256(depositMessageRoot[:], domain)
	// Perform BLS verification and if successful noice.
	valid, err := bls.Verify(depositData.Signature[:], signedRoot[:], depositData.PubKey[:])
	if err != nil || !valid {
		// ignore err here
		log.Debug("Validator BLS verification failed", "valid", valid, "err", err)
		return false, nil
	}
	return true, nil
}

func AddValidatorToRegistry(
	s abstract.BeaconState,
	pubkey [48]byte,
	withdrawalCredentials common.Hash,
	amount uint64,
) {
	// Append validator
	s.AddValidator(state.GetValidatorFromDeposit(s, pubkey, withdrawalCredentials, amount), amount)
	if s.Version() >= clparams.AltairVersion {
		// Altair forward
		s.AddCurrentEpochParticipationFlags(cltypes.ParticipationFlags(0))
		s.AddPreviousEpochParticipationFlags(cltypes.ParticipationFlags(0))
		s.AddInactivityScore(0)
	}
}
