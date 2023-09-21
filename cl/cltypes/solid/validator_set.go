package solid

import (
	"bytes"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
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

const (
	validatorSetCapacityMultiplier = 1.01 // allocate 20% to the validator set when re-allocation is needed.
	validatorTreeCacheGroupLayer   = 3    // It will cache group validatorTreeCacheGroupLayer^2 accordingly
)

// This is all stuff used by phase0 state transition. It makes many operations faster.
type Phase0Data struct {
	// MinInclusionDelay
	MinCurrentInclusionDelayAttestation  *PendingAttestation
	MinPreviousInclusionDelayAttestation *PendingAttestation
}

type ValidatorSet struct {
	buffer          []byte
	treeCacheBuffer []byte

	l, c int

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

func (v *ValidatorSet) expandBuffer(newValidatorSetLength int) {
	size := newValidatorSetLength * validatorSize
	treeCacheSize := getTreeCacheSize(newValidatorSetLength, validatorTreeCacheGroupLayer) * length.Hash

	if size <= cap(v.buffer) {
		v.treeCacheBuffer = v.treeCacheBuffer[:treeCacheSize]
		v.buffer = v.buffer[:size]
		return
	}
	increasedValidatorsCapacity := uint64(float64(newValidatorSetLength)*validatorSetCapacityMultiplier) + 1
	buffer := make([]byte, size, increasedValidatorsCapacity*validatorSize)
	cacheBuffer := make([]byte, treeCacheSize, increasedValidatorsCapacity*length.Hash)
	copy(buffer, v.buffer)
	copy(cacheBuffer, v.treeCacheBuffer)
	v.treeCacheBuffer = cacheBuffer
	v.buffer = buffer
}

func (v *ValidatorSet) Append(val Validator) {
	offset := v.EncodingSizeSSZ()
	// we are overflowing the buffer? append.
	if offset >= len(v.buffer) {
		v.expandBuffer(v.l + 1)
		v.phase0Data = append(v.phase0Data, Phase0Data{})
	}
	v.zeroTreeHash(v.l)
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

func (v *ValidatorSet) CopyTo(t *ValidatorSet) {
	t.l = v.l
	t.c = v.c
	offset := v.EncodingSizeSSZ()
	if offset > len(t.buffer) {
		t.expandBuffer(v.l)
		t.attesterBits = make([]byte, len(v.attesterBits))
	}
	// skip copying (unsupported for phase0)
	t.phase0Data = make([]Phase0Data, t.l)
	copy(t.buffer, v.buffer)
	copy(t.treeCacheBuffer, v.treeCacheBuffer)
	copy(t.attesterBits, v.attesterBits)
	t.attesterBits = t.attesterBits[:v.l]
}

func (v *ValidatorSet) DecodeSSZ(buf []byte, _ int) error {
	if len(buf)%validatorSize > 0 {
		return ssz.ErrBufferNotRounded
	}
	v.expandBuffer(len(buf) / validatorSize)
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

	return Validator(v.buffer[idx*validatorSize : (idx*validatorSize)+validatorSize])
}

func (v *ValidatorSet) HashSSZ() ([32]byte, error) {
	// generate root list
	validatorsLeafChunkSize := convertDepthToChunkSize(validatorTreeCacheGroupLayer)
	hashBuffer := make([]byte, 8*32)
	depth := GetDepth(uint64(v.c))
	lengthRoot := merkle_tree.Uint64Root(uint64(v.l))

	if v.l == 0 {
		return utils.Keccak256(merkle_tree.ZeroHashes[depth][:], lengthRoot[:]), nil
	}

	emptyHashBytes := make([]byte, length.Hash)

	layerBuffer := make([]byte, validatorsLeafChunkSize*length.Hash)
	for i := 0; i < v.l; i += validatorsLeafChunkSize {
		from := uint64(i)
		to := utils.Min64(from+uint64(validatorsLeafChunkSize), uint64(v.l))
		offset := (i / validatorsLeafChunkSize) * length.Hash

		if !bytes.Equal(v.treeCacheBuffer[offset:offset+length.Hash], emptyHashBytes) {
			continue
		}
		for i := from; i < to; i++ {
			validator := v.Get(int(i))
			if err := validator.CopyHashBufferTo(hashBuffer); err != nil {
				return [32]byte{}, err
			}
			hashBuffer = hashBuffer[:(8 * 32)]
			if err := merkle_tree.MerkleRootFromFlatLeaves(hashBuffer, layerBuffer[(i-from)*length.Hash:]); err != nil {
				return [32]byte{}, err
			}
		}
		endOffset := (to - from) * length.Hash
		if err := computeFlatRootsToBuffer(validatorTreeCacheGroupLayer, layerBuffer[:endOffset], v.treeCacheBuffer[offset:]); err != nil {
			return [32]byte{}, err
		}

	}

	offset := length.Hash * ((v.l + validatorsLeafChunkSize - 1) / validatorsLeafChunkSize)
	v.makeBuf(offset)
	copy(v.buf, v.treeCacheBuffer[:offset])
	elements := v.buf
	for i := uint8(validatorTreeCacheGroupLayer); i < depth; i++ {
		// Sequential
		if len(elements)%64 != 0 {
			elements = append(elements, merkle_tree.ZeroHashes[i][:]...)
		}
		outputLen := len(elements) / 2
		if err := merkle_tree.HashByteSlice(elements, elements); err != nil {
			return [32]byte{}, err
		}
		elements = elements[:outputLen]
	}

	return utils.Keccak256(elements[:length.Hash], lengthRoot[:]), nil
}

func computeFlatRootsToBuffer(depth uint8, layerBuffer, output []byte) error {
	for i := uint8(0); i < depth; i++ {
		// Sequential
		if len(layerBuffer)%64 != 0 {
			layerBuffer = append(layerBuffer, merkle_tree.ZeroHashes[i][:]...)
		}
		if err := merkle_tree.HashByteSlice(layerBuffer, layerBuffer); err != nil {
			return err
		}
		layerBuffer = layerBuffer[:len(layerBuffer)/2]
	}

	copy(output, layerBuffer[:length.Hash])
	return nil
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

func (v *ValidatorSet) zeroTreeHash(idx int) {
	iNodeIdx := (idx / (1 << validatorTreeCacheGroupLayer)) * length.Hash
	for i := iNodeIdx; i < iNodeIdx+length.Hash; i++ {
		v.treeCacheBuffer[i] = 0
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

func (v *ValidatorSet) SetWithdrawalCredentialForValidatorAtIndex(index int, creds libcommon.Hash) {
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
