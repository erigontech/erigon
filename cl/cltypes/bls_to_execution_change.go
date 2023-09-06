package cltypes

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

// Change to EL engine
type BLSToExecutionChange struct {
	ValidatorIndex uint64
	From           libcommon.Bytes48
	To             libcommon.Address
}

func (b *BLSToExecutionChange) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.ValidatorIndex, b.From[:], b.To[:])
}

func (b *BLSToExecutionChange) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.ValidatorIndex, b.From[:], b.To[:])
}

func (b *BLSToExecutionChange) DecodeSSZ(buf []byte, version int) error {
	if len(buf) < b.EncodingSizeSSZ() {
		return fmt.Errorf("[BLSToExecutionChange] err: %s", ssz.ErrLowBufferSize)
	}
	b.ValidatorIndex = ssz.UnmarshalUint64SSZ(buf)
	copy(b.From[:], buf[8:])
	copy(b.To[:], buf[56:])
	return ssz2.UnmarshalSSZ(buf, version, &b.ValidatorIndex, b.From[:], b.To[:])
}

func (*BLSToExecutionChange) EncodingSizeSSZ() int {
	return 76
}

func (*BLSToExecutionChange) Static() bool {
	return true
}

type SignedBLSToExecutionChange struct {
	Message   *BLSToExecutionChange
	Signature libcommon.Bytes96
}

func (s *SignedBLSToExecutionChange) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, s.Message, s.Signature[:])
}

func (s *SignedBLSToExecutionChange) DecodeSSZ(buf []byte, version int) error {
	s.Message = new(BLSToExecutionChange)
	return ssz2.UnmarshalSSZ(buf, version, s.Message, s.Signature[:])
}

func (s *SignedBLSToExecutionChange) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(s.Message, s.Signature[:])
}

func (s *SignedBLSToExecutionChange) EncodingSizeSSZ() int {
	return 96 + s.Message.EncodingSizeSSZ()
}
