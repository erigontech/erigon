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

package forkchoice

import (
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/ssz"
)

var _ ssz.HashableSSZ = (*ForkChoiceNode)(nil)

// LatestMessage represents the latest message from a validator.
// [Modified in Gloas:EIP7732] Added Slot and PayloadPresent.
type LatestMessage struct {
	Epoch          uint64
	Slot           uint64 // [New in Gloas:EIP7732]
	Root           common.Hash
	PayloadPresent bool // [New in Gloas:EIP7732]
}

// ForkChoiceNode tracks the payload status for a block root in the fork choice store.
// [New in Gloas:EIP7732]
type ForkChoiceNode struct {
	Root          common.Hash
	PayloadStatus cltypes.PayloadStatus
}

func (n *ForkChoiceNode) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, n.Root[:], uint64(n.PayloadStatus))
}

func (n *ForkChoiceNode) DecodeSSZ(buf []byte, version int) error {
	var payloadStatus uint64
	if err := ssz2.UnmarshalSSZ(buf, version, n.Root[:], &payloadStatus); err != nil {
		return err
	}
	n.PayloadStatus = cltypes.PayloadStatus(payloadStatus)
	return nil
}

func (*ForkChoiceNode) EncodingSizeSSZ() int {
	return length.Hash + 8
}

func (*ForkChoiceNode) Static() bool {
	return true
}

func (n *ForkChoiceNode) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(n.Root[:], uint64(n.PayloadStatus))
}

func (n *ForkChoiceNode) Clone() clonable.Clonable {
	return &ForkChoiceNode{
		Root:          n.Root,
		PayloadStatus: n.PayloadStatus,
	}
}
