package solid

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
)

const (
	IsCurrentMatchingSourceAttesterBit  = 0x0
	IsPreviousMatchingSourceAttesterBit = 0x1
	IsCurrentMatchingTargetAttesterBit  = 0x2
	IsPreviousMatchingTargetAttesterBit = 0x3
	IsCurrentMatchingHeadAttesterBit    = 0x4
	IsPreviousMatchingHeadAttesterBit   = 0x5
)

const validatorSetCapacityMultiplier = 1.05 // allocate 5% to the validator set when re-allocation is needed.

// This is all stuff used by phase0 state transition. It makes many operations faster.
type Phase0Data struct {
	// MinInclusionDelay
	MinCurrentInclusionDelayAttestation  *PendingAttestation
	MinPreviousInclusionDelayAttestation *PendingAttestation
}

type ValidatorSet struct {
	buffer []byte
	l, c   int

	// We have phase0 data below
	phase0Data   []Phase0Data
	attesterBits []byte

	hashBuf
}

func NewValidatorSet(c int) *ValidatorSet {
	return &ValidatorSet{
		c: c,
	}
}

func (v *ValidatorSet) expandBuffer(size int) {
	if size <= cap(v.buffer) {
		v.buffer = v.buffer[:size]
		return
	}
	buffer := make([]byte, size, int(float64(size)*validatorSetCapacityMultiplier))
	copy(buffer, v.buffer)
	v.buffer = buffer
}

func (v *ValidatorSet) Append(val Validator) {
	offset := v.EncodingSizeSSZ()
	// we are overflowing the buffer? append.
	if offset >= len(v.buffer) {
		v.expandBuffer(offset + validatorSize)
		v.phase0Data = append(v.phase0Data, Phase0Data{})
	}
	copy(v.buffer[offset:], val)
	v.phase0Data[v.l] = Phase0Data{} // initialize to empty.
	v.attesterBits = append(v.attesterBits, 0x0)
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
}

func (v *ValidatorSet) Clone() clonable.Clonable {
	return NewValidatorSet(v.c)
}

func (v *ValidatorSet) CopyTo(set2 IterableSSZ[Validator]) {
	t := set2.(*ValidatorSet)
	t.l = v.l
	t.c = v.c
	offset := v.EncodingSizeSSZ()
	if offset > len(t.buffer) {
		t.expandBuffer(offset)
		t.buffer = append(t.buffer, make([]byte, len(v.buffer)-len(t.buffer))...)
		t.attesterBits = make([]byte, len(v.attesterBits))
	}
	// skip copying (unsupported for phase0)
	t.phase0Data = make([]Phase0Data, t.l)
	copy(t.buffer, v.buffer)
	copy(t.attesterBits, v.attesterBits)
	t.attesterBits = t.attesterBits[:v.l]
}

func (v *ValidatorSet) DecodeSSZ(buf []byte, _ int) error {
	if len(buf)%validatorSize > 0 {
		return ssz.ErrBufferNotRounded
	}
	v.expandBuffer(len(buf))
	copy(v.buffer, buf)
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
	return v.buffer[idx*validatorSize : (idx*validatorSize)+validatorSize]
}

func (v *ValidatorSet) HashSSZ() ([32]byte, error) {
	// generate root list
	v.makeBuf(v.l * length.Hash)
	validatorLeaves := v.buf
	hashBuffer := make([]byte, 8*32)
	depth := getDepth(uint64(v.c))
	lengthRoot := merkle_tree.Uint64Root(uint64(v.l))

	if v.l == 0 {
		return utils.Keccak256(merkle_tree.ZeroHashes[depth][:], lengthRoot[:]), nil
	}
	for i := 0; i < v.l; i++ {
		validator := v.Get(i)
		if err := validator.CopyHashBufferTo(hashBuffer); err != nil {
			return [32]byte{}, err
		}
		hashBuffer = hashBuffer[:(8 * 32)]
		if err := merkle_tree.MerkleRootFromFlatLeaves(hashBuffer, validatorLeaves[i*length.Hash:]); err != nil {
			return [32]byte{}, err
		}
	}
	offset := length.Hash * v.l
	elements := common.Copy(validatorLeaves[:offset])
	for i := uint8(0); i < depth; i++ {
		// Sequential
		if len(elements)%64 != 0 {
			elements = append(elements, merkle_tree.ZeroHashes[i][:]...)
		}
		outputLen := len(elements) / 2
		v.makeBuf(outputLen)
		if err := merkle_tree.HashByteSlice(v.buf, elements); err != nil {
			return [32]byte{}, err
		}
		elements = v.buf
	}
	return utils.Keccak256(elements[:length.Hash], lengthRoot[:]), nil
}

func (v *ValidatorSet) Set(idx int, val Validator) {
	if idx >= v.l {
		panic("ValidatorSet -- Set: out of bounds")
	}
	copy(v.buffer[idx*validatorSize:(idx*validatorSize)+validatorSize], val)
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
