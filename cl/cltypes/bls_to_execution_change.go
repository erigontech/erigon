// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package cltypes

import (
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

// Change to EL engine
type BLSToExecutionChange struct {
	ValidatorIndex uint64         `json:"validator_index,string"`
	From           common.Bytes48 `json:"from_bls_pubkey"`
	To             common.Address `json:"to_execution_address"`
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
	Message   *BLSToExecutionChange `json:"message"`
	Signature common.Bytes96        `json:"signature"`
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
