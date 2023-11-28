package state_accessors

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

// class Validator(Container):
//
//	pubkey: BLSPubkey
//	withdrawal_credentials: Bytes32  # Commitment to pubkey for withdrawals
//	effective_balance: Gwei  # Balance at stake
//	slashed: boolean
//	# Status epochs
//	activation_eligibility_epoch: Epoch  # When criteria for activation were met
//	activation_epoch: Epoch
//	exit_epoch: Epoch
//	withdrawable_epoch: Epoch  # When validator can withdraw funds

type StaticValidator struct {
	withdrawalCredentials []staticValidatorField[libcommon.Hash]
	slashed               []staticValidatorField[bool]
	activationEligibility []staticValidatorField[uint64]
	activationEpoch       []staticValidatorField[uint64]
	exitEpoch             []staticValidatorField[uint64]
	withdrawableEpoch     []staticValidatorField[uint64]
}

func NewStaticValidatorFromValidator(v solid.Validator, slot uint64) *StaticValidator {
	return &StaticValidator{
		withdrawalCredentials: []staticValidatorField[libcommon.Hash]{{slot, v.WithdrawalCredentials()}},
		slashed:               []staticValidatorField[bool]{{slot, v.Slashed()}},
		activationEligibility: []staticValidatorField[uint64]{{slot, v.ActivationEligibilityEpoch()}},
		activationEpoch:       []staticValidatorField[uint64]{{slot, v.ActivationEpoch()}},
		exitEpoch:             []staticValidatorField[uint64]{{slot, v.ExitEpoch()}},
		withdrawableEpoch:     []staticValidatorField[uint64]{{slot, v.WithdrawableEpoch()}},
	}
}

func (s *StaticValidator) AddWithdrawalCredentials(slot uint64, withdrawalCredentials libcommon.Hash) {
	s.withdrawalCredentials = append(s.withdrawalCredentials, staticValidatorField[libcommon.Hash]{slot, withdrawalCredentials})
}

func (s *StaticValidator) AddSlashed(slot uint64, slashed bool) {
	s.slashed = append(s.slashed, staticValidatorField[bool]{slot, slashed})
}

func (s *StaticValidator) AddActivationEligibility(slot uint64, activationEligibility uint64) {
	s.activationEligibility = append(s.activationEligibility, staticValidatorField[uint64]{slot, activationEligibility})
}

func (s *StaticValidator) AddActivationEpoch(slot uint64, activationEpoch uint64) {
	s.activationEpoch = append(s.activationEpoch, staticValidatorField[uint64]{slot, activationEpoch})
}

func (s *StaticValidator) AddExitEpoch(slot uint64, exitEpoch uint64) {
	s.exitEpoch = append(s.exitEpoch, staticValidatorField[uint64]{slot, exitEpoch})
}

func (s *StaticValidator) AddWithdrawableEpoch(slot uint64, withdrawableEpoch uint64) {
	s.withdrawableEpoch = append(s.withdrawableEpoch, staticValidatorField[uint64]{slot, withdrawableEpoch})
}

func (s *StaticValidator) WithdrawalCredentials(slot uint64) libcommon.Hash {
	currIndex := 0
	for i, v := range s.withdrawalCredentials {
		if v.slot > slot {
			break
		}
		currIndex = i
	}
	return s.withdrawalCredentials[currIndex].field
}

func (s *StaticValidator) Slashed(slot uint64) bool {
	currIndex := 0
	for i, v := range s.slashed {
		if v.slot > slot {
			break
		}
		currIndex = i
	}
	return s.slashed[currIndex].field
}

func (s *StaticValidator) ActivationEligibilityEpoch(slot uint64) uint64 {
	currIndex := 0
	for i, v := range s.activationEligibility {
		if v.slot > slot {
			break
		}
		currIndex = i
	}
	return s.activationEligibility[currIndex].field
}

func (s *StaticValidator) ActivationEpoch(slot uint64) uint64 {
	currIndex := 0
	for i, v := range s.activationEpoch {
		if v.slot > slot {
			break
		}
		currIndex = i
	}
	return s.activationEpoch[currIndex].field
}

func (s *StaticValidator) ExitEpoch(slot uint64) uint64 {
	currIndex := 0
	for i, v := range s.exitEpoch {
		if v.slot > slot {
			break
		}
		currIndex = i
	}
	return s.exitEpoch[currIndex].field
}

func (s *StaticValidator) WithdrawableEpoch(slot uint64) uint64 {
	currIndex := 0
	for i, v := range s.withdrawableEpoch {
		if v.slot > slot {
			break
		}
		currIndex = i
	}
	return s.withdrawableEpoch[currIndex].field
}

func (s *StaticValidator) ToValidator(v solid.Validator, slot uint64) {
	v.SetWithdrawalCredentials(s.WithdrawalCredentials(slot))
	v.SetSlashed(s.Slashed(slot))
	v.SetActivationEligibilityEpoch(s.ActivationEligibilityEpoch(slot))
	v.SetActivationEpoch(s.ActivationEpoch(slot))
	v.SetExitEpoch(s.ExitEpoch(slot))
	v.SetWithdrawableEpoch(s.WithdrawableEpoch(slot))
}

type staticValidatorField[V any] struct {
	slot  uint64
	field V
}
