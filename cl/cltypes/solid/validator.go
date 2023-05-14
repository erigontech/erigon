package solid

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
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

type Validator [validatorSize]byte

func NewValidatorFromParameters(
	PublicKey [48]byte,
	WithdrawalCredentials [32]byte,
	EffectiveBalance uint64,
	Slashed bool,
	ActivationEligibilityEpoch uint64,
	ActivationEpoch uint64,
	ExitEpoch uint64,
	WithdrawableEpoch uint64,
) *Validator {
	v := &Validator{}
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

func (v *Validator) CopyTo(dst *Validator) {
	copy(dst[:], v[:])
}

func (v *Validator) EncodeSSZ(dst []byte) []byte {
	buf := dst
	buf = append(buf, v[:]...)
	return buf
}

func (v *Validator) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < v.EncodingSizeSSZ() {
		return ssz.ErrLowBufferSize
	}
	copy(v[:], buf)
	return nil
}

func (v *Validator) EncodingSizeSSZ() int {
	return 121
}

func (v *Validator) PublicKey() (o [48]byte) {
	copy(o[:], v[:48])
	return
}
func (v *Validator) WithdrawalCredentials() (o common.Hash) {
	copy(o[:], v[48:80])
	return
}
func (v *Validator) EffectiveBalance() uint64 {
	return binary.LittleEndian.Uint64(v[80:88])
}
func (v *Validator) Slashed() bool {
	return v[88] != 0
}
func (v *Validator) ActivationEligibilityEpoch() uint64 {
	return binary.LittleEndian.Uint64(v[89:97])
}
func (v *Validator) ActivationEpoch() uint64 {
	return binary.LittleEndian.Uint64(v[97:105])
}
func (v *Validator) ExitEpoch() uint64 {
	return binary.LittleEndian.Uint64(v[105:113])
}
func (v *Validator) WithdrawableEpoch() uint64 {
	return binary.LittleEndian.Uint64(v[113:121])
}

func (v *Validator) CopyHashBufferTo(o []byte) error {
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

func (v *Validator) SetPublicKey(o [48]byte) {
	copy(v[:48], o[:])
	return
}
func (v *Validator) SetWithdrawalCredentials(o common.Hash) {
	copy(v[48:80], o[:])
	return
}
func (v *Validator) SetEffectiveBalance(i uint64) {
	binary.LittleEndian.PutUint64(v[80:88], i)
}
func (v *Validator) SetSlashed(b bool) {
	if b {
		v[88] = 1
		return
	}
	v[88] = 0
}
func (v *Validator) SetActivationEligibilityEpoch(i uint64) {
	binary.LittleEndian.PutUint64(v[89:97], i)
}
func (v *Validator) SetActivationEpoch(i uint64) {
	binary.LittleEndian.PutUint64(v[97:105], i)
}
func (v *Validator) SetExitEpoch(i uint64) {
	binary.LittleEndian.PutUint64(v[105:113], i)
}
func (v *Validator) SetWithdrawableEpoch(i uint64) {
	binary.LittleEndian.PutUint64(v[113:121], i)
}
