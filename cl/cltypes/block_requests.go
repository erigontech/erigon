package cltypes

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/cltypes/ssz"
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
func (r *BeaconBlocksByRootRequest) HashSSZ() ([32]byte, error) {
	empty := [32]byte{}
	return empty, nil
}

// EncodeSSZ Marshals the block by roots request type into the serialized object.
func (r *BeaconBlocksByRootRequest) EncodeSSZ(dst []byte) ([]byte, error) {
	if len(*r) > maxRequestBlocks {
		return nil, fmt.Errorf("beacon block by roots request exceeds max size: %d > %d", len(*r), maxRequestBlocks)
	}
	buf := make([]byte, 0, r.EncodingSizeSSZ())
	for _, r := range *r {
		buf = append(buf, r[:]...)
	}
	return append(dst, buf...), nil
}

// EncodingSizeSSZ returns the size of the serialized representation.
func (r *BeaconBlocksByRootRequest) EncodingSizeSSZ() int {
	return len(*r) * rootLength
}

// DecodeSSZ unmarshals the provided bytes buffer into the
// block by roots request object.
func (r *BeaconBlocksByRootRequest) DecodeSSZ(buf []byte, _ int) error {
	bufLen := len(buf)
	maxLength := maxRequestBlocks * rootLength
	if bufLen > maxLength {
		return fmt.Errorf("expected buffer with length of upto %d but received length %d", maxLength, bufLen)
	}
	if bufLen%rootLength != 0 {
		return ssz.ErrBufferNotRounded
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
