package cltypes

import (
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

const validatorSize = 121

type ValidatorSet struct {
	validators []*Validator

	l, c int
}

func NewValidatorSet(c int) *ValidatorSet {
	return &ValidatorSet{
		c: c,
	}
}

func (*ValidatorSet) Static() bool {
	return false
}

func (v *ValidatorSet) EncodeSSZ(buf []byte) (dst []byte, err error) {
	dst = buf
	for _, validator := range v.validators {
		if dst, err = validator.EncodeSSZ(dst); err != nil {
			return
		}
	}
	return
}

func (v *ValidatorSet) DecodeSSZ(buf []byte, version int) (err error) {
	v.validators, err = ssz.DecodeStaticList[*Validator](buf, 0, uint32(len(buf)), validatorSize, uint64(v.c), version)
	v.l = len(v.validators)
	return
}

func (v *ValidatorSet) EncodingSizeSSZ() int {
	return v.l * validatorSize
}

func (v *ValidatorSet) HashSSZ() ([32]byte, error) {
	return merkle_tree.ListObjectSSZRoot(v.validators[:v.l], uint64(v.c))
}

func (v *ValidatorSet) Clone() clonable.Clonable {
	return NewValidatorSet(v.c)
}

func (v *ValidatorSet) Get(index int) *Validator {
	if index >= v.l {
		panic("ValidatorSet: Get out of bounds")
	}
	return v.validators[index]
}

func (v *ValidatorSet) Range(fn func(index int, value *Validator, length int) bool) {
	for idx := 0; idx < v.l; idx++ {
		cont := fn(idx, v.validators[idx], len(v.validators))
		if !cont {
			break
		}
	}
}

func (v *ValidatorSet) Length() int {
	return v.l
}

func (v *ValidatorSet) CopyTo(t solid.IterableSSZ[*Validator]) {
	o := t.(*ValidatorSet)
	o.l = v.l
	o.c = v.c
	for idx := 0; idx < v.l; idx++ {
		if idx >= len(o.validators) {
			o.validators = append(o.validators, &Validator{})
		}
		v.validators[idx].CopyTo(o.validators[idx])
	}
}

func (v *ValidatorSet) Cap() int {
	return v.c
}

func (v *ValidatorSet) Pop() *Validator {
	panic("unimplementedt")
}

func (v *ValidatorSet) Set(index int, n *Validator) {
	if index >= v.l {
		panic("ValidatorSet: Set out of bounds")
	}
	v.validators[index] = n
}

func (v *ValidatorSet) Append(obj *Validator) {
	if v.l >= len(v.validators) {
		v.validators = append(v.validators, obj)
		v.l++
		return
	}
	v.validators[v.l] = obj
	v.l++
}

func (v *ValidatorSet) Clear() {
	v.l = 0
}
