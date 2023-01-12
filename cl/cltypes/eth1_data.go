package cltypes

import (
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
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
func (e *Eth1Data) EncodeSSZ(buf []byte) (dst []byte) {
	dst = buf
	dst = append(dst, e.Root[:]...)
	dst = append(dst, ssz_utils.Uint64SSZ(e.DepositCount)...)
	dst = append(dst, e.BlockHash[:]...)
	return
}

// UnmarshalSSZ ssz unmarshals the Eth1Data object
func (e *Eth1Data) DecodeSSZ(buf []byte) error {
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
func (e *Eth1Data) EncodingSizeSSZ() int {
	return common.BlockNumberLength + common.HashLength*2
}

// HashTreeRoot ssz hashes the Eth1Data object
func (e *Eth1Data) HashSSZ() ([32]byte, error) {
	leaves := [][32]byte{
		e.Root,
		merkle_tree.Uint64Root(e.DepositCount),
		e.BlockHash,
	}
	return merkle_tree.ArraysRoot(leaves, 4)
}
