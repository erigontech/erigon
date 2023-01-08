package cltypes

import (
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/common"
	ssz "github.com/prysmaticlabs/fastssz"
)

type Eth1Data struct {
	Root         common.Hash
	BlockHash    common.Hash
	DepositCount uint64
}

// MarshalSSZTo ssz marshals the Eth1Data object to a target array
func (e *Eth1Data) MarshalSSZTo(buf []byte) (dst []byte, err error) {
	dst = buf
	dst = append(dst, e.Root[:]...)
	dst = ssz.MarshalUint64(dst, e.DepositCount)
	dst = append(dst, e.BlockHash[:]...)
	return
}

// MarshalSSZ ssz marshals the Eth1Data object
func (e *Eth1Data) MarshalSSZ() ([]byte, error) {
	buf := make([]byte, 0, common.BlockNumberLength+common.HashLength*2)
	return e.MarshalSSZTo(buf)
}

// UnmarshalSSZ ssz unmarshals the Eth1Data object
func (e *Eth1Data) UnmarshalSSZ(buf []byte) error {
	var err error
	size := uint64(len(buf))
	if size != 72 {
		return ssz.ErrSize
	}

	copy(e.Root[:], buf[0:32])
	e.DepositCount = ssz.UnmarshallUint64(buf[32:40])
	copy(e.BlockHash[:], buf[40:72])

	return err
}

// SizeSSZ returns the ssz encoded size in bytes for the Eth1Data object
func (e *Eth1Data) SizeSSZ() int {
	return common.BlockNumberLength + common.HashLength*2
}

// HashTreeRoot ssz hashes the Eth1Data object
func (e *Eth1Data) HashTreeRoot() ([32]byte, error) {
	leaves := [][32]byte{
		e.Root,
		merkle_tree.Uint64Root(e.DepositCount),
		e.BlockHash,
	}
	return merkle_tree.ArraysRoot(leaves, 4)
}

// HashTreeRootWith ssz hashes the Eth1Data object with a hasher
func (e *Eth1Data) HashTreeRootWith(hh *ssz.Hasher) (err error) {
	var root common.Hash
	root, err = e.HashTreeRoot()
	if err != nil {
		return
	}

	hh.PutBytes(root[:])

	return
}
