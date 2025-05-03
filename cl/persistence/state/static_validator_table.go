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

package state_accessors

import (
	"errors"
	"io"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/cbor"
	"github.com/erigontech/erigon/cl/cltypes/solid"
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
//
// StaticValidator is designed to track changes in a validator's attributes over time.
// It keeps track of attributes such as withdrawal credentials, slashed status, and various epochs
// that typically change at most twice during the validator's lifespan.
type StaticValidator struct {
	publicKeys            []staticValidatorField[common.Bytes48] // Tracks changes in public keys.
	withdrawalCredentials []staticValidatorField[common.Hash]    // Tracks changes in withdrawal credentials.
	slashed               []staticValidatorField[bool]           // Tracks changes in slashed status.
	activationEligibility []staticValidatorField[uint64]         // Tracks changes in activation eligibility epoch.
	activationEpoch       []staticValidatorField[uint64]         // Tracks changes in activation epoch.
	exitEpoch             []staticValidatorField[uint64]         // Tracks changes in exit epoch.
	withdrawableEpoch     []staticValidatorField[uint64]         // Tracks changes in withdrawable epoch.
}

// NewStaticValidatorFromValidator creates a new StaticValidator from a given Validator and Slot,
// initializing the fields with the current state of the Validator at the given Slot.
func NewStaticValidatorFromValidator(v solid.Validator, slot uint64) *StaticValidator {
	return &StaticValidator{
		// Initializes each field with the current state of the validator.
		publicKeys:            []staticValidatorField[common.Bytes48]{{slot, v.PublicKey()}},
		withdrawalCredentials: []staticValidatorField[common.Hash]{{slot, v.WithdrawalCredentials()}},
		slashed:               []staticValidatorField[bool]{{slot, v.Slashed()}},
		activationEligibility: []staticValidatorField[uint64]{{slot, v.ActivationEligibilityEpoch()}},
		activationEpoch:       []staticValidatorField[uint64]{{slot, v.ActivationEpoch()}},
		exitEpoch:             []staticValidatorField[uint64]{{slot, v.ExitEpoch()}},
		withdrawableEpoch:     []staticValidatorField[uint64]{{slot, v.WithdrawableEpoch()}},
	}
}

// AddWithdrawalCredentials adds a new withdrawal credential entry to the validator.
// This method is used to track changes in withdrawal credentials over time.
func (s *StaticValidator) AddWithdrawalCredentials(slot uint64, withdrawalCredentials common.Hash) {
	s.withdrawalCredentials = append(s.withdrawalCredentials, staticValidatorField[common.Hash]{slot, withdrawalCredentials})
}

// cborStaticValidator is a struct used for CBOR serialization of StaticValidator data.
type cborStaticValidator struct {
	PublicKeys            []staticValidatorField[common.Bytes48]
	WithdrawalCredentials []staticValidatorField[common.Hash]
	Slashed               []staticValidatorField[bool]
	ActivationEligibility []staticValidatorField[uint64]
	ActivationEpoch       []staticValidatorField[uint64]
	ExitEpoch             []staticValidatorField[uint64]
	WithdrawableEpoch     []staticValidatorField[uint64]
}

// Serialize encodes the StaticValidator data into CBOR format and writes it to the given writer.
func (s *StaticValidator) WriteTo(w io.Writer) error {
	return cbor.Marshal(w, cborStaticValidator{
		PublicKeys:            s.publicKeys,
		WithdrawalCredentials: s.withdrawalCredentials,
		Slashed:               s.slashed,
		ActivationEligibility: s.activationEligibility,
		ActivationEpoch:       s.activationEpoch,
		ExitEpoch:             s.exitEpoch,
		WithdrawableEpoch:     s.withdrawableEpoch,
	})
}

// Deserialize decodes CBOR data from the given reader and updates the StaticValidator fields.
func (s *StaticValidator) ReadFrom(r io.Reader) error {
	tmp := &cborStaticValidator{}
	if err := cbor.Unmarshal(&tmp, r); err != nil {
		return err
	}
	s.withdrawalCredentials = tmp.WithdrawalCredentials
	s.slashed = tmp.Slashed
	s.activationEligibility = tmp.ActivationEligibility
	s.activationEpoch = tmp.ActivationEpoch
	s.exitEpoch = tmp.ExitEpoch
	s.withdrawableEpoch = tmp.WithdrawableEpoch
	s.publicKeys = tmp.PublicKeys
	return nil
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

func (s *StaticValidator) WithdrawalCredentials(slot uint64) common.Hash {
	currIndex := 0
	for i, v := range s.withdrawalCredentials {
		if v.Slot > slot {
			break
		}
		currIndex = i
	}
	return s.withdrawalCredentials[currIndex].Field
}

func (s *StaticValidator) Slashed(slot uint64) bool {
	currIndex := 0
	for i, v := range s.slashed {
		if v.Slot > slot {
			break
		}
		currIndex = i
	}
	return s.slashed[currIndex].Field
}

func (s *StaticValidator) ActivationEligibilityEpoch(slot uint64) uint64 {
	currIndex := 0
	for i, v := range s.activationEligibility {
		if v.Slot > slot {
			break
		}
		currIndex = i
	}
	return s.activationEligibility[currIndex].Field
}

func (s *StaticValidator) ActivationEpoch(slot uint64) uint64 {
	currIndex := 0
	for i, v := range s.activationEpoch {
		if v.Slot > slot {
			break
		}
		currIndex = i
	}
	return s.activationEpoch[currIndex].Field
}

func (s *StaticValidator) ExitEpoch(slot uint64) uint64 {
	currIndex := 0
	for i, v := range s.exitEpoch {
		if v.Slot > slot {
			break
		}
		currIndex = i
	}
	return s.exitEpoch[currIndex].Field
}

func (s *StaticValidator) WithdrawableEpoch(slot uint64) uint64 {
	currIndex := 0
	for i, v := range s.withdrawableEpoch {
		if v.Slot > slot {
			break
		}
		currIndex = i
	}
	return s.withdrawableEpoch[currIndex].Field
}

func (s *StaticValidator) PublicKey(slot uint64) common.Bytes48 {
	currIndex := 0
	for i, v := range s.publicKeys {
		if v.Slot > slot {
			break
		}
		currIndex = i
	}
	return s.publicKeys[currIndex].Field
}

func (s *StaticValidator) ToValidator(v solid.Validator, slot uint64) {
	v.SetPublicKey(s.PublicKey(slot))
	v.SetWithdrawalCredentials(s.WithdrawalCredentials(slot))
	v.SetSlashed(s.Slashed(slot))
	v.SetActivationEligibilityEpoch(s.ActivationEligibilityEpoch(slot))
	v.SetActivationEpoch(s.ActivationEpoch(slot))
	v.SetExitEpoch(s.ExitEpoch(slot))
	v.SetWithdrawableEpoch(s.WithdrawableEpoch(slot))
}

func (s *StaticValidator) Reset(slot uint64) {
	for i := 0; i < len(s.publicKeys); i++ {
		if s.publicKeys[i].Slot > slot {
			s.publicKeys = s.publicKeys[:i]
			break
		}
	}
	for i := 0; i < len(s.withdrawalCredentials); i++ {
		if s.withdrawalCredentials[i].Slot > slot {
			s.withdrawalCredentials = s.withdrawalCredentials[:i]
			break
		}
	}
	for i := 0; i < len(s.slashed); i++ {
		if s.slashed[i].Slot > slot {
			s.slashed = s.slashed[:i]
			break
		}
	}
	for i := 0; i < len(s.activationEligibility); i++ {
		if s.activationEligibility[i].Slot > slot {
			s.activationEligibility = s.activationEligibility[:i]
			break
		}
	}
	for i := 0; i < len(s.activationEpoch); i++ {
		if s.activationEpoch[i].Slot > slot {
			s.activationEpoch = s.activationEpoch[:i]
			break
		}
	}
	for i := 0; i < len(s.exitEpoch); i++ {
		if s.exitEpoch[i].Slot > slot {
			s.exitEpoch = s.exitEpoch[:i]
			break
		}
	}
	for i := 0; i < len(s.withdrawableEpoch); i++ {
		if s.withdrawableEpoch[i].Slot > slot {
			s.withdrawableEpoch = s.withdrawableEpoch[:i]
			break
		}
	}
}

type staticValidatorField[V any] struct {
	Slot  uint64
	Field V
}

// StaticValidatorTable is a structure to manage a collection of StaticValidators.
// It is used for tracking multiple validators and their state changes.
type StaticValidatorTable struct {
	validatorTable []*StaticValidator
	slot           uint64
	sync           sync.RWMutex // Mutex for safe concurrent access.
}

// NewStaticValidatorTable creates a new instance of StaticValidatorTable.
func NewStaticValidatorTable() *StaticValidatorTable {
	return &StaticValidatorTable{
		validatorTable: make([]*StaticValidator, 0, 2400), // Preallocating memory for efficiency.
	}
}

func (s *StaticValidatorTable) AddValidator(v solid.Validator, validatorIndex, slot uint64) error {
	s.sync.Lock()
	defer s.sync.Unlock()
	if slot <= s.slot && s.slot != 0 {
		return nil
	}
	s.validatorTable = append(s.validatorTable, NewStaticValidatorFromValidator(v, slot))
	if validatorIndex != uint64(len(s.validatorTable))-1 {
		return errors.New("validator index mismatch")
	}
	return nil
}

func (s *StaticValidatorTable) AddWithdrawalCredentials(validatorIndex, slot uint64, withdrawalCredentials common.Hash) error {
	s.sync.Lock()
	defer s.sync.Unlock()
	if slot <= s.slot && s.slot != 0 {
		return nil
	}
	if validatorIndex >= uint64(len(s.validatorTable)) {
		return errors.New("validator index mismatch")
	}
	s.validatorTable[validatorIndex].AddWithdrawalCredentials(slot, withdrawalCredentials)
	return nil
}

func (s *StaticValidatorTable) AddSlashed(validatorIndex, slot uint64, slashed bool) error {
	s.sync.Lock()
	defer s.sync.Unlock()
	if slot <= s.slot && s.slot != 0 {
		return nil
	}
	if validatorIndex >= uint64(len(s.validatorTable)) {
		return errors.New("validator index mismatch")
	}
	s.validatorTable[validatorIndex].AddSlashed(slot, slashed)
	return nil
}

func (s *StaticValidatorTable) AddActivationEligibility(validatorIndex, slot uint64, activationEligibility uint64) error {
	s.sync.Lock()
	defer s.sync.Unlock()
	if slot <= s.slot && s.slot != 0 {
		return nil
	}
	if validatorIndex >= uint64(len(s.validatorTable)) {
		return errors.New("validator index mismatch")
	}
	s.validatorTable[validatorIndex].AddActivationEligibility(slot, activationEligibility)
	return nil
}

func (s *StaticValidatorTable) AddActivationEpoch(validatorIndex, slot uint64, activationEpoch uint64) error {
	s.sync.Lock()
	defer s.sync.Unlock()
	if slot <= s.slot && s.slot != 0 {
		return nil
	}
	if validatorIndex >= uint64(len(s.validatorTable)) {
		return errors.New("validator index mismatch")
	}
	s.validatorTable[validatorIndex].AddActivationEpoch(slot, activationEpoch)
	return nil
}

func (s *StaticValidatorTable) AddExitEpoch(validatorIndex, slot uint64, exitEpoch uint64) error {
	s.sync.Lock()
	defer s.sync.Unlock()
	if slot <= s.slot && s.slot != 0 {
		return nil
	}
	if validatorIndex >= uint64(len(s.validatorTable)) {
		return errors.New("validator index mismatch")
	}
	s.validatorTable[validatorIndex].AddExitEpoch(slot, exitEpoch)
	return nil
}

func (s *StaticValidatorTable) AddWithdrawableEpoch(validatorIndex, slot uint64, withdrawableEpoch uint64) error {
	s.sync.Lock()
	defer s.sync.Unlock()
	if slot <= s.slot && s.slot != 0 {
		return nil
	}
	if validatorIndex >= uint64(len(s.validatorTable)) {
		return errors.New("validator index mismatch")
	}
	s.validatorTable[validatorIndex].AddWithdrawableEpoch(slot, withdrawableEpoch)
	return nil
}

func (s *StaticValidatorTable) GetInPlace(validatorIndex uint64, slot uint64, v solid.Validator) {
	s.sync.RLock()
	defer s.sync.RUnlock()
	s.validatorTable[validatorIndex].ToValidator(v, slot)
}

func (s *StaticValidatorTable) ForEach(fn func(validatorIndex uint64, validator *StaticValidator) bool) {
	s.sync.RLock()
	defer s.sync.RUnlock()
	for i, v := range s.validatorTable {
		if !fn(uint64(i), v) {
			break
		}
	}
}

func (s *StaticValidatorTable) WithdrawalCredentials(validatorIndex uint64, slot uint64) common.Hash {
	s.sync.RLock()
	defer s.sync.RUnlock()
	return s.validatorTable[validatorIndex].WithdrawalCredentials(slot)
}

func (s *StaticValidatorTable) Slashed(validatorIndex uint64, slot uint64) bool {
	s.sync.RLock()
	defer s.sync.RUnlock()
	return s.validatorTable[validatorIndex].Slashed(slot)
}

func (s *StaticValidatorTable) ActivationEligibilityEpoch(validatorIndex uint64, slot uint64) uint64 {
	s.sync.RLock()
	defer s.sync.RUnlock()
	return s.validatorTable[validatorIndex].ActivationEligibilityEpoch(slot)
}

func (s *StaticValidatorTable) ActivationEpoch(validatorIndex uint64, slot uint64) uint64 {
	s.sync.RLock()
	defer s.sync.RUnlock()
	return s.validatorTable[validatorIndex].ActivationEpoch(slot)
}

func (s *StaticValidatorTable) ExitEpoch(validatorIndex uint64, slot uint64) uint64 {
	s.sync.RLock()
	defer s.sync.RUnlock()
	return s.validatorTable[validatorIndex].ExitEpoch(slot)
}

func (s *StaticValidatorTable) WithdrawableEpoch(validatorIndex uint64, slot uint64) uint64 {
	s.sync.RLock()
	defer s.sync.RUnlock()
	return s.validatorTable[validatorIndex].WithdrawableEpoch(slot)
}

func (s *StaticValidatorTable) GetStaticValidator(validatorIndex uint64) *StaticValidator {
	s.sync.RLock()
	defer s.sync.RUnlock()
	return s.validatorTable[validatorIndex]
}

// This is for versioning
func (s *StaticValidatorTable) SetSlot(slot uint64) {
	s.sync.Lock()
	defer s.sync.Unlock()
	s.resetTable(slot)
	s.slot = slot
}

func (s *StaticValidatorTable) resetTable(slot uint64) {
	for i, v := range s.validatorTable {
		v.Reset(slot)
		// if we remove all public keys, we can remove all subsequent fields
		if len(v.publicKeys) == 0 {
			s.validatorTable = s.validatorTable[:i]
			break
		}
	}
}

func (s *StaticValidatorTable) Slot() uint64 {
	s.sync.RLock()
	defer s.sync.RUnlock()
	return s.slot
}
