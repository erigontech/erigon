package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"

	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
	"github.com/ledgerwatch/erigon/common"
)

type Eth1Data struct {
	Root         libcommon.Hash
	DepositCount uint64
	BlockHash    libcommon.Hash
}

func (e *Eth1Data) Copy() *Eth1Data {
	copied := *e
	return &copied
}

func (e *Eth1Data) Equal(b *Eth1Data) bool {
	return e.BlockHash == b.BlockHash && e.Root == b.Root && b.DepositCount == e.DepositCount
}

// MarshalSSZTo ssz marshals the Eth1Data object to a target array
func (e *Eth1Data) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, e.Root[:], e.DepositCount, e.BlockHash[:])

}

func (e *Eth1Data) DecodeSSZ(buf []byte, _ int) error {
	return ssz2.UnmarshalSSZ(buf, 0, e.Root[:], &e.DepositCount, e.BlockHash[:])
}

// EncodingSizeSSZ returns the ssz encoded size in bytes for the Eth1Data object
func (e *Eth1Data) EncodingSizeSSZ() int {
	return common.BlockNumberLength + length.Hash*2
}

// HashSSZ ssz hashes the Eth1Data object
func (e *Eth1Data) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(e.Root[:], e.DepositCount, e.BlockHash[:])
}

func (e *Eth1Data) Static() bool {
	return true
}
