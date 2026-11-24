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
	"errors"
	"fmt"
	"iter"

	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/ssz"
)

const (
	IsCurrentMatchingSourceAttesterBit  = 0x0
	IsPreviousMatchingSourceAttesterBit = 0x1
	IsCurrentMatchingTargetAttesterBit  = 0x2
	IsPreviousMatchingTargetAttesterBit = 0x3
	IsCurrentMatchingHeadAttesterBit    = 0x4
	IsPreviousMatchingHeadAttesterBit   = 0x5
)

// This is all stuff used by phase0 state transition. It makes many operations faster.
type Phase0Data struct {
	// MinInclusionDelay
	MinCurrentInclusionDelayAttestation  *PendingAttestation
	MinPreviousInclusionDelayAttestation *PendingAttestation
}

type ValidatorSet struct {
	list cowList

	// We have phase0 data below
	phase0Data   []Phase0Data
	attesterBits []byte
}

var errValidatorLayout = errors.New("validator set hashed with the wrong layout")

func NewValidatorSet(c int) *ValidatorSet {
	v := &ValidatorSet{}
	v.list.init(validatorSize, c, validatorLeaf)
	return v
}

func NewValidatorSetWithLength(c int, l int) *ValidatorSet {
	v := NewValidatorSet(c)
	v.list.reset(l, nil)
	v.attesterBits = make([]byte, l)
	return v
}

func (v *ValidatorSet) Append(val Validator) {
	v.list.append(val)
	if v.phase0Data != nil {
		v.phase0Data = append(v.phase0Data, Phase0Data{})
	}
	v.attesterBits = append(v.attesterBits[:v.list.l-1], 0)
}

func (v *ValidatorSet) Cap() int {
	return v.list.c
}

func (v *ValidatorSet) Length() int {
	return v.list.l
}

func (v *ValidatorSet) Pop() Validator {
	panic("not implemented")
}

func (v *ValidatorSet) Clear() {
	v.list.clear()
	v.phase0Data = nil
	v.attesterBits = v.attesterBits[:0]
}

func (v *ValidatorSet) ClearPhase0Data() {
	v.phase0Data = nil
}

func (v *ValidatorSet) Clone() clonable.Clonable {
	clone := NewValidatorSet(v.list.c)
	clone.list.layout = v.list.layout
	return clone
}

func (v *ValidatorSet) CopyTo(t *ValidatorSet) {
	v.list.copyTo(&t.list)
	if v.phase0Data == nil {
		t.phase0Data = nil
	} else {
		t.phase0Data = append(t.phase0Data[:0], v.phase0Data...)
	}
	t.attesterBits = append(t.attesterBits[:0], v.attesterBits...)
}

func (v *ValidatorSet) DecodeSSZ(buf []byte, _ int) error {
	if len(buf)%validatorSize > 0 {
		return ssz.ErrBufferNotRounded
	}
	if l := len(buf) / validatorSize; l > v.list.c {
		return fmt.Errorf("validator set length exceeds limit: %d > %d", l, v.list.c)
	}
	v.list.decode(buf)
	v.phase0Data = nil
	v.attesterBits = make([]byte, v.list.l)
	return nil
}

func (v *ValidatorSet) EncodeSSZ(buf []byte) ([]byte, error) {
	return v.list.encode(buf), nil
}

func (v *ValidatorSet) EncodingSizeSSZ() int {
	if v == nil {
		return 0
	}
	return v.list.l * validatorSize
}

func (*ValidatorSet) Static() bool {
	return false
}

func (v *ValidatorSet) Get(idx int) ValidatorView {
	if uint(idx) >= uint(v.list.l) {
		panic("ValidatorSet -- Get: out of bounds")
	}
	return ValidatorView{b: v.list.elem(idx)}
}

func (v *ValidatorSet) HashSSZ() ([32]byte, error) {
	if v.list.l > v.list.c {
		return [32]byte{}, fmt.Errorf("validator set length exceeds limit: %d > %d", v.list.l, v.list.c)
	}
	if v.list.layout.progressive() {
		return [32]byte{}, errValidatorLayout
	}
	return v.hashWithLength()
}

func (v *ValidatorSet) HashSSZProgressive() ([32]byte, error) {
	if !v.list.layout.progressive() {
		return [32]byte{}, errValidatorLayout
	}
	return v.hashWithLength()
}

func (v *ValidatorSet) hashWithLength() ([32]byte, error) {
	core, err := v.list.coreRoot()
	if err != nil {
		return [32]byte{}, err
	}
	lengthRoot := merkle_tree.Uint64Root(uint64(v.list.l))
	return crypto.Sha256(core[:], lengthRoot[:]), nil
}

func (v *ValidatorSet) SetProgressiveHashing(enabled bool) {
	if v == nil {
		return
	}
	v.list.setProgressive(enabled)
}

func (v *ValidatorSet) Set(idx int, val Validator) {
	if uint(idx) >= uint(v.list.l) {
		panic("ValidatorSet -- Set: out of bounds")
	}
	copy(v.list.write(idx), val)
}

func (v *ValidatorSet) write(idx int) Validator {
	if uint(idx) >= uint(v.list.l) {
		panic("ValidatorSet -- write: out of bounds")
	}
	return v.list.write(idx)
}

func (v *ValidatorSet) getPhase0(idx int) Phase0Data {
	if idx < 0 || idx >= v.list.l {
		panic("ValidatorSet -- getPhase0: out of bounds")
	}
	if v.phase0Data == nil {
		return Phase0Data{}
	}
	return v.phase0Data[idx]
}

func (v *ValidatorSet) phase0ForWrite(idx int) *Phase0Data {
	if idx < 0 || idx >= v.list.l {
		panic("ValidatorSet -- phase0ForWrite: out of bounds")
	}
	if v.phase0Data == nil {
		v.phase0Data = make([]Phase0Data, v.list.l)
	}
	return &v.phase0Data[idx]
}

func (v *ValidatorSet) getAttesterBit(idx int, bit int) bool {
	if idx < 0 || idx >= v.list.l {
		panic("ValidatorSet -- getBit: out of bounds")
	}
	return (v.attesterBits[idx] & (1 << bit)) > 0
}

func (v *ValidatorSet) setAttesterBit(idx int, bit int, val bool) {
	if idx < 0 || idx >= v.list.l {
		panic("ValidatorSet -- getBit: out of bounds")
	}
	if val {
		v.attesterBits[idx] = ((1 << bit) | v.attesterBits[idx])
		return
	}
	v.attesterBits[idx] &= ^(1 << bit)
}

// All yields the validators in [from, to) in index order, as of the moment
// iteration enters each chunk. The callback may write the current index; writes
// to other indices may not be visible to later yields. Validators appended
// during iteration are not visited.
func (v *ValidatorSet) All(from, to int) iter.Seq2[int, ValidatorView] {
	if from < 0 || from > to || to > v.list.l {
		panic("ValidatorSet -- All: out of bounds")
	}
	return func(yield func(int, ValidatorView) bool) {
		size := v.list.layout.elemSize
		for i := from; i < to; {
			ci, off := v.list.locate(i)
			count := min(v.list.layout.chunkLeaves()-off, to-i)
			data := v.list.chunks[ci].Load().data
			for j := range count {
				start := (off + j) * size
				if !yield(i+j, ValidatorView{b: data[start : start+size : start+size]}) {
					return
				}
			}
			i += count
		}
	}
}

func (v *ValidatorSet) Range(fn func(int, ValidatorView, int) bool) {
	length := v.list.l
	for i, val := range v.All(0, length) {
		if !fn(i, val, length) {
			return
		}
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
	v.phase0ForWrite(idx).MinCurrentInclusionDelayAttestation = val
}

func (v *ValidatorSet) SetMinPreviousInclusionDelayAttestation(idx int, val *PendingAttestation) {
	v.phase0ForWrite(idx).MinPreviousInclusionDelayAttestation = val
}

func (v *ValidatorSet) SetWithdrawalCredentialForValidatorAtIndex(index int, creds common.Hash) {
	v.write(index).SetWithdrawalCredentials(creds)
}

func (v *ValidatorSet) SetExitEpochForValidatorAtIndex(index int, epoch uint64) {
	v.write(index).SetExitEpoch(epoch)
}

func (v *ValidatorSet) SetWithdrawableEpochForValidatorAtIndex(index int, epoch uint64) {
	v.write(index).SetWithdrawableEpoch(epoch)
}

func (v *ValidatorSet) SetEffectiveBalanceForValidatorAtIndex(index int, balance uint64) {
	v.write(index).SetEffectiveBalance(balance)
}

func (v *ValidatorSet) SetActivationEpochForValidatorAtIndex(index int, epoch uint64) {
	v.write(index).SetActivationEpoch(epoch)
}

func (v *ValidatorSet) SetActivationEligibilityEpochForValidatorAtIndex(index int, epoch uint64) {
	v.write(index).SetActivationEligibilityEpoch(epoch)
}

func (v *ValidatorSet) SetValidatorSlashed(index int, slashed bool) {
	v.write(index).SetSlashed(slashed)
}

func (v *ValidatorSet) MarshalJSON() ([]byte, error) {
	validators := make([]ValidatorView, 0, v.list.l)
	for _, val := range v.All(0, v.list.l) {
		validators = append(validators, val)
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
