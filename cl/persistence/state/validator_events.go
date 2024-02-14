package state_accessors

import (
	"encoding/binary"
	"errors"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

// THIS IS EXPERMIENTAL, IT MAY CHANGE

var ErrUnknownEvent = errors.New("unknown event")

type stateEvent int64

const (
	addValidator stateEvent = iota
	changeExitEpoch
	changeWithdrawableEpoch
	changeWithdrawalCredentials
	changeActivationEpoch
	changeActivationEligibilityEpoch
	changeSlashed
)

type StateEvents struct {
	buf []byte
}

func NewStateEvents() *StateEvents {
	return &StateEvents{}
}

func (se *StateEvents) AddValidator(validatorIndex uint64, validator solid.Validator) {
	se.buf = append(se.buf, byte(addValidator))
	se.buf = binary.BigEndian.AppendUint64(se.buf, validatorIndex)
	se.buf = append(se.buf, validator...)
}

func (se *StateEvents) ChangeExitEpoch(validatorIndex uint64, exitEpoch uint64) {
	se.buf = append(se.buf, byte(changeExitEpoch))
	se.buf = binary.BigEndian.AppendUint64(se.buf, validatorIndex)
	se.buf = binary.BigEndian.AppendUint64(se.buf, exitEpoch)
}

func (se *StateEvents) ChangeWithdrawableEpoch(validatorIndex uint64, withdrawableEpoch uint64) {
	se.buf = append(se.buf, byte(changeWithdrawableEpoch))
	se.buf = binary.BigEndian.AppendUint64(se.buf, validatorIndex)
	se.buf = binary.BigEndian.AppendUint64(se.buf, withdrawableEpoch)
}

func (se *StateEvents) ChangeWithdrawalCredentials(validatorIndex uint64, withdrawalCredentials libcommon.Hash) {
	se.buf = append(se.buf, byte(changeWithdrawalCredentials))
	se.buf = binary.BigEndian.AppendUint64(se.buf, validatorIndex)
	se.buf = append(se.buf, withdrawalCredentials[:]...)
}

func (se *StateEvents) ChangeActivationEpoch(validatorIndex uint64, activationEpoch uint64) {
	se.buf = append(se.buf, byte(changeActivationEpoch))
	se.buf = binary.BigEndian.AppendUint64(se.buf, validatorIndex)
	se.buf = binary.BigEndian.AppendUint64(se.buf, activationEpoch)
}

func (se *StateEvents) ChangeActivationEligibilityEpoch(validatorIndex uint64, activationEligibilityEpoch uint64) {
	se.buf = append(se.buf, byte(changeActivationEligibilityEpoch))
	se.buf = binary.BigEndian.AppendUint64(se.buf, validatorIndex)
	se.buf = binary.BigEndian.AppendUint64(se.buf, activationEligibilityEpoch)
}

func (se *StateEvents) ChangeSlashed(validatorIndex uint64, slashed bool) {
	se.buf = append(se.buf, byte(changeSlashed))
	se.buf = binary.BigEndian.AppendUint64(se.buf, validatorIndex)
	se.buf = append(se.buf, byte(0))
	if slashed {
		se.buf[len(se.buf)-1] = byte(1)
	}
}

func (se *StateEvents) CopyBytes() []byte {
	return libcommon.Copy(se.buf)
}

func (se *StateEvents) Reset() {
	se.buf = se.buf[:0]
}

// ReplayEvents replays the events in the buffer and will return the err on handler failure
func ReplayEvents(onAddValidator func(validatorIndex uint64, validator solid.Validator) error,
	onChangeExitEpoch func(validatorIndex uint64, exitEpoch uint64) error,
	onChangeWithdrawableEpoch func(validatorIndex uint64, withdrawableEpoch uint64) error,
	onChangeWithdrawalCredentials func(validatorIndex uint64, withdrawalCredentials libcommon.Hash) error,
	onChangeActivationEpoch func(validatorIndex uint64, activationEpoch uint64) error,
	onChangeActivationEligibilityEpoch func(validatorIndex uint64, activationEligibilityEpoch uint64) error,
	onChangeSlashed func(validatorIndex uint64, slashed bool) error,
	e *StateEvents) error {
	buf := e.buf
	for len(buf) > 0 {
		event := stateEvent(buf[0])
		buf = buf[1:]
		switch event {
		case addValidator:
			validatorIndex := binary.BigEndian.Uint64(buf)
			buf = buf[8:]
			validator := solid.Validator(buf[:121])
			buf = buf[121:]
			if err := onAddValidator(validatorIndex, validator); err != nil {
				return err
			}
		case changeExitEpoch:
			validatorIndex := binary.BigEndian.Uint64(buf)
			buf = buf[8:]
			exitEpoch := binary.BigEndian.Uint64(buf)
			buf = buf[8:]
			if err := onChangeExitEpoch(validatorIndex, exitEpoch); err != nil {
				return err
			}
		case changeWithdrawableEpoch:
			validatorIndex := binary.BigEndian.Uint64(buf)
			buf = buf[8:]
			withdrawableEpoch := binary.BigEndian.Uint64(buf)
			buf = buf[8:]
			if err := onChangeWithdrawableEpoch(validatorIndex, withdrawableEpoch); err != nil {
				return err
			}
		case changeWithdrawalCredentials:
			validatorIndex := binary.BigEndian.Uint64(buf)
			buf = buf[8:]
			var withdrawalCredentials [32]byte
			copy(withdrawalCredentials[:], buf)
			buf = buf[32:]
			if err := onChangeWithdrawalCredentials(validatorIndex, withdrawalCredentials); err != nil {
				return err
			}
		case changeActivationEpoch:
			validatorIndex := binary.BigEndian.Uint64(buf)
			buf = buf[8:]
			activationEpoch := binary.BigEndian.Uint64(buf)
			buf = buf[8:]
			if err := onChangeActivationEpoch(validatorIndex, activationEpoch); err != nil {
				return err
			}
		case changeActivationEligibilityEpoch:
			validatorIndex := binary.BigEndian.Uint64(buf)
			buf = buf[8:]
			activationEligibilityEpoch := binary.BigEndian.Uint64(buf)
			buf = buf[8:]
			if err := onChangeActivationEligibilityEpoch(validatorIndex, activationEligibilityEpoch); err != nil {
				return err
			}
		case changeSlashed:
			validatorIndex := binary.BigEndian.Uint64(buf)
			buf = buf[8:]
			slashed := buf[0] == 1
			buf = buf[1:]
			if err := onChangeSlashed(validatorIndex, slashed); err != nil {
				return err
			}
		default:
			return ErrUnknownEvent
		}
	}
	return nil
}
