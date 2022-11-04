package cltypes

import (
	ssz "github.com/ferranbt/fastssz"
	"github.com/pkg/errors"
)

const (
	rootLength       = 32
	maxRequestBlocks = 1024
)

// source: https://github.com/prysmaticlabs/prysm/blob/bb0929507227b2e543b67aaf43d3ffd36c62b8fc/beacon-chain/p2p/types/types.go
//
// We need a custom type for the BeaconBlocksByRootRequest because the generated
// code injects an offset that is used to indicate the count of objects in the
// slice, which is not included in the spec.
//
// See https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#beaconblocksbyroot

// BeaconBlocksByRootRequest specifies the blocks by root request type.
type BeaconBlocksByRootRequest [][rootLength]byte

// Just to satisfy the ObjectSSZ interface.
func (r *BeaconBlocksByRootRequest) HashTreeRoot() ([32]byte, error) {
	empty := [32]byte{}
	return empty, nil
}

// MarshalSSZTo marshals the block by roots request with the provided byte slice.
func (r *BeaconBlocksByRootRequest) MarshalSSZTo(dst []byte) ([]byte, error) {
	marshalledObj, err := r.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	return append(dst, marshalledObj...), nil
}

// MarshalSSZ Marshals the block by roots request type into the serialized object.
func (r *BeaconBlocksByRootRequest) MarshalSSZ() ([]byte, error) {
	if len(*r) > maxRequestBlocks {
		return nil, errors.Errorf("beacon block by roots request exceeds max size: %d > %d", len(*r), maxRequestBlocks)
	}
	buf := make([]byte, 0, r.SizeSSZ())
	for _, r := range *r {
		buf = append(buf, r[:]...)
	}
	return buf, nil
}

// SizeSSZ returns the size of the serialized representation.
func (r *BeaconBlocksByRootRequest) SizeSSZ() int {
	return len(*r) * rootLength
}

// UnmarshalSSZ unmarshals the provided bytes buffer into the
// block by roots request object.
func (r *BeaconBlocksByRootRequest) UnmarshalSSZ(buf []byte) error {
	bufLen := len(buf)
	maxLength := maxRequestBlocks * rootLength
	if bufLen > maxLength {
		return errors.Errorf("expected buffer with length of upto %d but received length %d", maxLength, bufLen)
	}
	if bufLen%rootLength != 0 {
		return ssz.ErrIncorrectByteSize
	}
	numOfRoots := bufLen / rootLength
	roots := make([][rootLength]byte, 0, numOfRoots)
	for i := 0; i < numOfRoots; i++ {
		var rt [rootLength]byte
		copy(rt[:], buf[i*rootLength:(i+1)*rootLength])
		roots = append(roots, rt)
	}
	*r = roots
	return nil
}
