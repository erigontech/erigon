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
	"encoding/json"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/cl/utils"
)

const (
	IsCurrentMatchingSourceAttesterBit  = 0x0
	IsPreviousMatchingSourceAttesterBit = 0x1
	IsCurrentMatchingTargetAttesterBit  = 0x2
	IsPreviousMatchingTargetAttesterBit = 0x3
	IsCurrentMatchingHeadAttesterBit    = 0x4
	IsPreviousMatchingHeadAttesterBit   = 0x5
)

const validatorSetCapacityMultiplier = 1.01 // allocate 20% to the validator set when re-allocation is needed.)

// This is all stuff used by phase0 state transition. It makes many operations faster.
type Phase0Data struct {
	// MinInclusionDelay
	MinCurrentInclusionDelayAttestation  *PendingAttestation
	MinPreviousInclusionDelayAttestation *PendingAttestation
}

type ValidatorSet struct {
	*merkle_tree.MerkleTree
	buffer []byte

	l, c int

	// We have phase0 data below
	phase0Data   []Phase0Data
	attesterBits []byte
}

func NewValidatorSet(c int) *ValidatorSet {
	return &ValidatorSet{
		c: c,
	}
}

func NewValidatorSetWithLength(c int, l int) *ValidatorSet {
	return &ValidatorSet{
		c:            c,
		l:            l,
		buffer:       make([]byte, l*validatorSize),
		phase0Data:   make([]Phase0Data, l),
		attesterBits: make([]byte, l),
	}
}

func (v *ValidatorSet) Bytes() []byte {
	return v.buffer[:v.l*validatorSize]
}

func (v *ValidatorSet) expandBuffer(newValidatorSetLength int) {
	size := newValidatorSetLength * validatorSize

	if size <= cap(v.buffer) {
		v.buffer = v.buffer[:size]
		return
	}
	increasedValidatorsCapacity := uint64(float64(newValidatorSetLength)*validatorSetCapacityMultiplier) + 1
	buffer := make([]byte, size, increasedValidatorsCapacity*validatorSize)
	copy(buffer, v.buffer)
	v.buffer = buffer
}

func (v *ValidatorSet) Append(val Validator) {
	offset := v.EncodingSizeSSZ()
	// we are overflowing the buffer? append.
	//if offset+validatorSize >= len(v.buffer) {
	v.expandBuffer(v.l + 1)
	v.phase0Data = append(v.phase0Data, Phase0Data{})
	//}

	copy(v.buffer[offset:], val)
	if v.MerkleTree != nil {
		v.MerkleTree.AppendLeaf()
	}
	v.zeroTreeHash(v.l)

	if v.l >= len(v.phase0Data) {
		for i := len(v.phase0Data); i < v.l+1; i++ {
			v.phase0Data = append(v.phase0Data, Phase0Data{})
		}
	}
	v.phase0Data[v.l] = Phase0Data{} // initialize to empty.

	if v.l >= len(v.attesterBits) {
		for i := len(v.attesterBits); i < v.l+1; i++ {
			v.attesterBits = append(v.attesterBits, 0)
		}
	}
	v.attesterBits[v.l] = 0 // initialize to empty.
	v.l++
}

func (v *ValidatorSet) Cap() int {
	return v.c
}

func (v *ValidatorSet) Length() int {
	return v.l
}

func (v *ValidatorSet) Pop() Validator {
	panic("not implemented")
}

func (v *ValidatorSet) Clear() {
	v.l = 0
	v.attesterBits = v.attesterBits[:0]
	v.MerkleTree = nil
}

func (v *ValidatorSet) Clone() clonable.Clonable {
	return NewValidatorSet(v.c)
}

func (v *ValidatorSet) CopyTo(t *ValidatorSet) {
	t.l = v.l
	t.c = v.c
	offset := v.EncodingSizeSSZ()
	if offset > len(t.buffer) {
		t.expandBuffer(v.l)
		t.attesterBits = make([]byte, len(v.attesterBits))
	}
	if v.MerkleTree != nil {
		if t.MerkleTree == nil {
			t.MerkleTree = &merkle_tree.MerkleTree{}
		}
		v.MerkleTree.CopyInto(t.MerkleTree)

		hashBuffer := make([]byte, 8*32)
		t.MerkleTree.SetComputeLeafFn(func(idx int, out []byte) {
			validator := t.Get(idx)
			if err := validator.CopyHashBufferTo(hashBuffer); err != nil {
				panic(err)
			}
			hashBuffer = hashBuffer[:(8 * 32)]
			if err := merkle_tree.MerkleRootFromFlatLeaves(hashBuffer, out); err != nil {
				panic(err)
			}
		})
	} else {
		t.MerkleTree = nil
	}
	// skip copying (unsupported for phase0)
	t.phase0Data = make([]Phase0Data, v.l)
	copy(t.buffer, v.buffer)
	copy(t.attesterBits, v.attesterBits)
	t.buffer = t.buffer[:v.l*validatorSize]
	t.attesterBits = t.attesterBits[:v.l]
}

func (v *ValidatorSet) DecodeSSZ(buf []byte, _ int) error {
	if len(buf)%validatorSize > 0 {
		return ssz.ErrBufferNotRounded
	}
	v.expandBuffer(len(buf) / validatorSize)
	copy(v.buffer, buf)
	v.MerkleTree = nil
	v.l = len(buf) / validatorSize
	v.phase0Data = make([]Phase0Data, v.l)
	v.attesterBits = make([]byte, v.l)
	return nil
}

func (v *ValidatorSet) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, v.buffer[:v.EncodingSizeSSZ()]...), nil
}

func (v *ValidatorSet) EncodingSizeSSZ() int {
	if v == nil {
		return 0
	}
	return v.l * validatorSize
}

func (*ValidatorSet) Static() bool {
	return false
}

func (v *ValidatorSet) Get(idx int) Validator {
	if idx >= v.l {
		panic("ValidatorSet -- Get: out of bounds")
	}

	return Validator(v.buffer[idx*validatorSize : (idx*validatorSize)+validatorSize])
}

func (v *ValidatorSet) HashSSZ() ([32]byte, error) {
	// generate root list
	if v.MerkleTree == nil {
		v.MerkleTree = &merkle_tree.MerkleTree{}
		cap := uint64(v.c)
		hashBuffer := make([]byte, 8*32)
		v.MerkleTree.Initialize(v.l, merkle_tree.OptimalMaxTreeCacheDepth, func(idx int, out []byte) {
			validator := v.Get(idx)
			if err := validator.CopyHashBufferTo(hashBuffer); err != nil {
				panic(err)
			}
			hashBuffer = hashBuffer[:(8 * 32)]
			if err := merkle_tree.MerkleRootFromFlatLeaves(hashBuffer, out); err != nil {
				panic(err)
			}
		}, &cap)
	}
	lengthRoot := merkle_tree.Uint64Root(uint64(v.l))
	coreRoot := v.MerkleTree.ComputeRoot()
	return utils.Sha256(coreRoot[:], lengthRoot[:]), nil
}

func (v *ValidatorSet) Set(idx int, val Validator) {
	if idx >= v.l {
		panic("ValidatorSet -- Set: out of bounds")
	}
	copy(v.buffer[idx*validatorSize:(idx*validatorSize)+validatorSize], val)
	v.zeroTreeHash(idx)
}

func (v *ValidatorSet) getPhase0(idx int) *Phase0Data {
	if idx >= v.l {
		panic("ValidatorSet -- getPhase0: out of bounds")
	}
	return &v.phase0Data[idx]
}

func (v *ValidatorSet) getAttesterBit(idx int, bit int) bool {
	if idx >= v.l {
		panic("ValidatorSet -- getBit: out of bounds")
	}
	return (v.attesterBits[idx] & (1 << bit)) > 0
}

func (v *ValidatorSet) setAttesterBit(idx int, bit int, val bool) {
	if idx >= v.l {
		panic("ValidatorSet -- getBit: out of bounds")
	}
	if val {
		v.attesterBits[idx] = ((1 << bit) | v.attesterBits[idx])
		return
	}
	v.attesterBits[idx] &= ^(1 << bit)
}

func (v *ValidatorSet) Range(fn func(int, Validator, int) bool) {
	for i := 0; i < v.l; i++ {
		if !fn(i, v.Get(i), v.l) {
			return
		}
	}
}

func (v *ValidatorSet) zeroTreeHash(idx int) {
	if v.MerkleTree != nil {
		v.MerkleTree.MarkLeafAsDirty(idx)
	}
}

func (v *ValidatorSet) IsCurrentMatchingSourceAttester(idx int) bool {
	return v.getAttesterBit(idx, IsCurrentMatchingSourceAttesterBit)
}

func (v *ValidatorSet) IsCurrentMatchingTargetAttester(idx int) bool {
	return v.getAttesterBit(idx, IsCurrentMatchingTargetAttesterBit)
}

func (v *ValidatorSet) IsCurrentMatchingHeadAttester(idx int) bool {
	return v.getAttesterBit(idx, IsCurrentMatchingHeadAttesterBit)
}

func (v *ValidatorSet) IsPreviousMatchingSourceAttester(idx int) bool {
	return v.getAttesterBit(idx, IsPreviousMatchingSourceAttesterBit)
}

func (v *ValidatorSet) IsPreviousMatchingTargetAttester(idx int) bool {
	return v.getAttesterBit(idx, IsPreviousMatchingTargetAttesterBit)
}

func (v *ValidatorSet) IsPreviousMatchingHeadAttester(idx int) bool {
	return v.getAttesterBit(idx, IsPreviousMatchingHeadAttesterBit)
}

func (v *ValidatorSet) MinCurrentInclusionDelayAttestation(idx int) *PendingAttestation {
	return v.getPhase0(idx).MinCurrentInclusionDelayAttestation
}

func (v *ValidatorSet) MinPreviousInclusionDelayAttestation(idx int) *PendingAttestation {
	return v.getPhase0(idx).MinPreviousInclusionDelayAttestation
}

func (v *ValidatorSet) SetIsCurrentMatchingSourceAttester(idx int, val bool) {
	v.setAttesterBit(idx, IsCurrentMatchingSourceAttesterBit, val)
}

func (v *ValidatorSet) SetIsCurrentMatchingTargetAttester(idx int, val bool) {
	v.setAttesterBit(idx, IsCurrentMatchingTargetAttesterBit, val)
}

func (v *ValidatorSet) SetIsCurrentMatchingHeadAttester(idx int, val bool) {
	v.setAttesterBit(idx, IsCurrentMatchingHeadAttesterBit, val)
}

func (v *ValidatorSet) SetIsPreviousMatchingSourceAttester(idx int, val bool) {
	v.setAttesterBit(idx, IsPreviousMatchingSourceAttesterBit, val)
}

func (v *ValidatorSet) SetIsPreviousMatchingTargetAttester(idx int, val bool) {
	v.setAttesterBit(idx, IsPreviousMatchingTargetAttesterBit, val)
}

func (v *ValidatorSet) SetIsPreviousMatchingHeadAttester(idx int, val bool) {
	v.setAttesterBit(idx, IsPreviousMatchingHeadAttesterBit, val)
}

func (v *ValidatorSet) SetMinCurrentInclusionDelayAttestation(idx int, val *PendingAttestation) {
	v.getPhase0(idx).MinCurrentInclusionDelayAttestation = val
}

func (v *ValidatorSet) SetMinPreviousInclusionDelayAttestation(idx int, val *PendingAttestation) {
	v.getPhase0(idx).MinPreviousInclusionDelayAttestation = val
}

func (v *ValidatorSet) SetWithdrawalCredentialForValidatorAtIndex(index int, creds common.Hash) {
	v.zeroTreeHash(index)
	v.Get(index).SetWithdrawalCredentials(creds)
}

func (v *ValidatorSet) SetExitEpochForValidatorAtIndex(index int, epoch uint64) {
	v.zeroTreeHash(index)
	v.Get(index).SetExitEpoch(epoch)
}

func (v *ValidatorSet) SetWithdrawableEpochForValidatorAtIndex(index int, epoch uint64) {
	v.zeroTreeHash(index)
	v.Get(index).SetWithdrawableEpoch(epoch)
}

func (v *ValidatorSet) SetEffectiveBalanceForValidatorAtIndex(index int, balance uint64) {
	v.zeroTreeHash(index)
	v.Get(index).SetEffectiveBalance(balance)
}

func (v *ValidatorSet) SetActivationEpochForValidatorAtIndex(index int, epoch uint64) {
	v.zeroTreeHash(index)
	v.Get(index).SetActivationEpoch(epoch)
}

func (v *ValidatorSet) SetActivationEligibilityEpochForValidatorAtIndex(index int, epoch uint64) {
	v.zeroTreeHash(index)
	v.Get(index).SetActivationEligibilityEpoch(epoch)
}

func (v *ValidatorSet) SetValidatorSlashed(index int, slashed bool) {
	v.zeroTreeHash(index)
	v.Get(index).SetSlashed(slashed)
}

func (v *ValidatorSet) MarshalJSON() ([]byte, error) {
	validators := make([]Validator, v.l)
	for i := 0; i < v.l; i++ {
		validators[i] = v.Get(i)
	}
	return json.Marshal(validators)
}

func (v *ValidatorSet) UnmarshalJSON(data []byte) error {
	var validators []Validator
	if err := json.Unmarshal(data, &validators); err != nil {
		return err
	}
	v.Clear()
	for _, val := range validators {
		v.Append(val)
	}
	return nil
}
