package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/common"
)

type Eth1Data struct {
	Root         libcommon.Hash
	BlockHash    libcommon.Hash
	DepositCount uint64
}

// MarshalSSZTo ssz marshals the Eth1Data object to a target array
func (e *Eth1Data) EncodeSSZ(buf []byte) (dst []byte, err error) {
	dst = buf
	dst = append(dst, e.Root[:]...)
	dst = append(dst, ssz_utils.Uint64SSZ(e.DepositCount)...)
	dst = append(dst, e.BlockHash[:]...)
	return
}

// DecodeSSZ ssz unmarshals the Eth1Data object
func (e *Eth1Data) DecodeSSZ(buf []byte) error {
	var err error
	size := uint64(len(buf))
	if size < 72 {
		return ssz_utils.ErrLowBufferSize
	}

	copy(e.Root[:], buf[0:32])
	e.DepositCount = ssz_utils.UnmarshalUint64SSZ(buf[32:40])
	copy(e.BlockHash[:], buf[40:72])

	return err
}

func (e *Eth1Data) DecodeSSZWithVersion(buf []byte, _ int) error {
	return e.DecodeSSZ(buf)
}

// EncodingSizeSSZ returns the ssz encoded size in bytes for the Eth1Data object
func (e *Eth1Data) EncodingSizeSSZ() int {
	return common.BlockNumberLength + length.Hash*2
}

// HashSSZ ssz hashes the Eth1Data object
func (e *Eth1Data) HashSSZ() ([32]byte, error) {
	leaves := [][32]byte{
		e.Root,
		merkle_tree.Uint64Root(e.DepositCount),
		e.BlockHash,
	}
	return merkle_tree.ArraysRoot(leaves, 4)
}
