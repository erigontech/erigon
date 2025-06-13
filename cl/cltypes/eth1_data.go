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
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"

	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

type Eth1Data struct {
	Root         common.Hash `json:"deposit_root"`
	DepositCount uint64      `json:"deposit_count,string"`
	BlockHash    common.Hash `json:"block_hash"`
}

func NewEth1Data() *Eth1Data {
	return &Eth1Data{}
}

func (e *Eth1Data) Copy() *Eth1Data {
	return &Eth1Data{
		Root:         e.Root,
		DepositCount: e.DepositCount,
		BlockHash:    e.BlockHash,
	}
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
	return 8 + length.Hash*2
}

// HashSSZ ssz hashes the Eth1Data object
func (e *Eth1Data) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(e.Root[:], e.DepositCount, e.BlockHash[:])
}

func (e *Eth1Data) Static() bool {
	return true
}
