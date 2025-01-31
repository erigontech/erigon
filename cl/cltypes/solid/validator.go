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

package solid

import (
	"encoding/binary"
	"encoding/json"
	"unsafe"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/cl/utils"
)

// PublicKey 48
// WithdrawalCredentials 32
// EffectiveBalance 8
// Slashed 1
// ActivationEligibilityEpoch 8
// ActivationEpoch 8
// ExitEpoch 8
// WithdrawableEpoch 8
//
// total: 121
const validatorSize = 48 + 32 + 8 + 1 + 8 + 8 + 8 + 8

// The Validator type represents an Ethereum 2.0 validator.
// It is stored as a flat buffer, which is a serialized representation of the struct.
// A flat buffer enables efficient read and write operations, as well as memory storage, without the need for serialization or deserialization.
type Validator []byte

func NewValidator() Validator {
	return make([]byte, validatorSize)
}

// NewValidatorFromParameters creates a new Validator object from the provided parameters.
// It is represented as a flat buffer.
func NewValidatorFromParameters(
	PublicKey [48]byte,
	WithdrawalCredentials [32]byte,
	EffectiveBalance uint64,
	Slashed bool,
	ActivationEligibilityEpoch uint64,
	ActivationEpoch uint64,
	ExitEpoch uint64,
	WithdrawableEpoch uint64,
) Validator {
	v := NewValidator()
	v.SetPublicKey(PublicKey)
	v.SetWithdrawalCredentials(WithdrawalCredentials)
	v.SetEffectiveBalance(EffectiveBalance)
	v.SetSlashed(Slashed)
	v.SetActivationEligibilityEpoch(ActivationEligibilityEpoch)
	v.SetActivationEpoch(ActivationEpoch)
	v.SetExitEpoch(ExitEpoch)
	v.SetWithdrawableEpoch(WithdrawableEpoch)
	return v
}

func (v Validator) CopyTo(dst Validator) {
	copy(dst[:], v[:])
}

func (v Validator) HashSSZ() ([32]byte, error) {
	hashBuffer := make([]byte, 8*32)
	if err := v.CopyHashBufferTo(hashBuffer); err != nil {
		return [32]byte{}, err
	}
	hashBuffer = hashBuffer[:(8 * 32)]
	if err := merkle_tree.MerkleRootFromFlatLeaves(hashBuffer, hashBuffer); err != nil {
		return [32]byte{}, err
	}
	return common.BytesToHash(hashBuffer[:length.Hash]), nil
}

func (v Validator) EncodeSSZ(dst []byte) ([]byte, error) {
	return append(dst, v[:]...), nil
}

func (v Validator) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < v.EncodingSizeSSZ() {
		return ssz.ErrLowBufferSize
	}
	copy(v[:], buf)
	return nil
}

func (v Validator) Clone() clonable.Clonable {
	return NewValidator()
}

func (v Validator) EncodingSizeSSZ() int {
	return 121
}

func (v Validator) PublicKey() (o [48]byte) {
	copy(o[:], v[:48])
	return
}

func (v Validator) PublicKeyBytes() (o []byte) {
	return v[:48]
}

func (v Validator) WithdrawalCredentials() (o common.Hash) {
	copy(o[:], v[48:80])
	return
}
func (v Validator) EffectiveBalance() uint64 {
	if utils.IsSysLittleEndian {
		return *(*uint64)(unsafe.Pointer(&v[80]))
	}
	return binary.LittleEndian.Uint64(v[80:88])
}
func (v Validator) Slashed() bool {
	return v[88] != 0
}
func (v Validator) ActivationEligibilityEpoch() uint64 {
	if utils.IsSysLittleEndian {
		return *(*uint64)(unsafe.Pointer(&v[89]))
	}
	return binary.LittleEndian.Uint64(v[89:97])
}
func (v Validator) ActivationEpoch() uint64 {
	if utils.IsSysLittleEndian {
		return *(*uint64)(unsafe.Pointer(&v[97]))
	}
	return binary.LittleEndian.Uint64(v[97:105])
}
func (v Validator) ExitEpoch() uint64 {
	if utils.IsSysLittleEndian {
		return *(*uint64)(unsafe.Pointer(&v[105]))
	}
	return binary.LittleEndian.Uint64(v[105:113])
}
func (v Validator) WithdrawableEpoch() uint64 {
	if utils.IsSysLittleEndian {
		return *(*uint64)(unsafe.Pointer(&v[113]))
	}
	return binary.LittleEndian.Uint64(v[113:121])
}

func (v Validator) CopyHashBufferTo(o []byte) error {
	for i := 0; i < 64; i++ {
		o[i] = 0
	}
	copy(o[:64], v[:48])
	err := merkle_tree.InPlaceRoot(o[:64])
	if err != nil {
		return err
	}
	copy(o[32:64], v[48:80])
	copy(o[64:96], v[80:88])
	o[96] = v[88]
	copy(o[128:160], v[89:97])
	copy(o[160:192], v[97:105])
	copy(o[192:224], v[105:113])
	copy(o[224:256], v[113:121])
	return nil
}

func (v Validator) SetPublicKey(o [48]byte) {
	copy(v[:48], o[:])
}

func (v Validator) SetWithdrawalCredentials(o common.Hash) {
	copy(v[48:80], o[:])
}

func (v Validator) SetEffectiveBalance(i uint64) {
	if utils.IsSysLittleEndian {
		*(*uint64)(unsafe.Pointer(&v[80])) = i
		return
	}
	binary.LittleEndian.PutUint64(v[80:88], i)
}

func (v Validator) SetEffectiveBalanceFromBytes(b []byte) {
	copy(v[80:88], b)
}

func (v Validator) SetSlashed(b bool) {
	if b {
		v[88] = 1
		return
	}
	v[88] = 0
}
func (v Validator) SetActivationEligibilityEpoch(i uint64) {
	if utils.IsSysLittleEndian {
		*(*uint64)(unsafe.Pointer(&v[89])) = i
		return
	}
	binary.LittleEndian.PutUint64(v[89:97], i)
}

func (v Validator) SetActivationEpoch(i uint64) {
	if utils.IsSysLittleEndian {
		*(*uint64)(unsafe.Pointer(&v[97])) = i
		return
	}
	binary.LittleEndian.PutUint64(v[97:105], i)
}

func (v Validator) SetExitEpoch(i uint64) {
	if utils.IsSysLittleEndian {
		*(*uint64)(unsafe.Pointer(&v[105])) = i
		return
	}
	binary.LittleEndian.PutUint64(v[105:113], i)
}

func (v Validator) SetWithdrawableEpoch(i uint64) {
	if utils.IsSysLittleEndian {
		*(*uint64)(unsafe.Pointer(&v[113])) = i
		return
	}
	binary.LittleEndian.PutUint64(v[113:121], i)
}

// Active returns if validator is active for given epoch
func (v Validator) Active(epoch uint64) bool {
	return v.ActivationEpoch() <= epoch && epoch < v.ExitEpoch()
}

func (v Validator) IsSlashable(epoch uint64) bool {
	return !v.Slashed() && (v.ActivationEpoch() <= epoch) && (epoch < v.WithdrawableEpoch())
}

func (v Validator) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		PublicKey                  common.Bytes48 `json:"pubkey"`
		WithdrawalCredentials      common.Hash    `json:"withdrawal_credentials"`
		EffectiveBalance           uint64         `json:"effective_balance,string"`
		Slashed                    bool           `json:"slashed"`
		ActivationEligibilityEpoch uint64         `json:"activation_eligibility_epoch,string"`
		ActivationEpoch            uint64         `json:"activation_epoch,string"`
		ExitEpoch                  uint64         `json:"exit_epoch,string"`
		WithdrawableEpoch          uint64         `json:"withdrawable_epoch,string"`
	}{
		PublicKey:                  v.PublicKey(),
		WithdrawalCredentials:      v.WithdrawalCredentials(),
		EffectiveBalance:           v.EffectiveBalance(),
		Slashed:                    v.Slashed(),
		ActivationEligibilityEpoch: v.ActivationEligibilityEpoch(),
		ActivationEpoch:            v.ActivationEpoch(),
		ExitEpoch:                  v.ExitEpoch(),
		WithdrawableEpoch:          v.WithdrawableEpoch(),
	})
}

func (v *Validator) UnmarshalJSON(input []byte) error {
	var err error
	var tmp struct {
		PublicKey                  common.Bytes48 `json:"pubkey"`
		WithdrawalCredentials      common.Hash    `json:"withdrawal_credentials"`
		EffectiveBalance           uint64         `json:"effective_balance,string"`
		Slashed                    bool           `json:"slashed"`
		ActivationEligibilityEpoch uint64         `json:"activation_eligibility_epoch,string"`
		ActivationEpoch            uint64         `json:"activation_epoch,string"`
		ExitEpoch                  uint64         `json:"exit_epoch,string"`
		WithdrawableEpoch          uint64         `json:"withdrawable_epoch,string"`
	}
	if err = json.Unmarshal(input, &tmp); err != nil {
		return err
	}
	*v = NewValidatorFromParameters(tmp.PublicKey, tmp.WithdrawalCredentials, tmp.EffectiveBalance, tmp.Slashed, tmp.ActivationEligibilityEpoch, tmp.ActivationEpoch, tmp.ExitEpoch, tmp.WithdrawableEpoch)
	return nil
}
