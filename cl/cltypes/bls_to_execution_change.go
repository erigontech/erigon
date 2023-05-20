package cltypes

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

// Change to EL engine
type BLSToExecutionChange struct {
	ValidatorIndex uint64
	From           [48]byte
	To             libcommon.Address
}

func (b *BLSToExecutionChange) EncodeSSZ(buf []byte) ([]byte, error) {
	dst := buf
	dst = append(dst, ssz.Uint64SSZ(b.ValidatorIndex)...)
	dst = append(dst, b.From[:]...)
	dst = append(dst, b.To[:]...)
	return dst, nil
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
	return nil
}

func (*BLSToExecutionChange) EncodingSizeSSZ() int {
	return 76
}

type SignedBLSToExecutionChange struct {
	Message   *BLSToExecutionChange
	Signature [96]byte
}

func (s *SignedBLSToExecutionChange) EncodeSSZ(buf []byte) ([]byte, error) {
	dst := buf
	var err error
	if dst, err = s.Message.EncodeSSZ(dst); err != nil {
		return nil, err
	}
	dst = append(dst, s.Signature[:]...)
	return dst, nil
}

func (s *SignedBLSToExecutionChange) DecodeSSZ(buf []byte, version int) error {
	if len(buf) < s.EncodingSizeSSZ() {
		return fmt.Errorf("[SignedBLSToExecutionChange] err: %s", ssz.ErrLowBufferSize)
	}
	s.Message = new(BLSToExecutionChange)
	if err := s.Message.DecodeSSZ(buf, version); err != nil {
		return err
	}
	copy(s.Signature[:], buf[s.Message.EncodingSizeSSZ():])
	return nil
}

func (s *SignedBLSToExecutionChange) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(s.Message, s.Signature[:])
}

func (s *SignedBLSToExecutionChange) EncodingSizeSSZ() int {
	return 96 + s.Message.EncodingSizeSSZ()
}
