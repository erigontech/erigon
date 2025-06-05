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
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

// Fork data, contains if we were on bellatrix/alteir/phase0 and transition epoch.
type Fork struct {
	PreviousVersion common.Bytes4 `json:"previous_version"`
	CurrentVersion  common.Bytes4 `json:"current_version"`
	Epoch           uint64        `json:"epoch,string"`
}

func (*Fork) Static() bool {
	return true
}

func (f *Fork) Copy() *Fork {
	return &Fork{
		PreviousVersion: f.PreviousVersion,
		CurrentVersion:  f.CurrentVersion,
		Epoch:           f.Epoch,
	}
}

func (f *Fork) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, f.PreviousVersion[:], f.CurrentVersion[:], f.Epoch)
}

func (f *Fork) DecodeSSZ(buf []byte, _ int) error {
	return ssz2.UnmarshalSSZ(buf, 0, f.PreviousVersion[:], f.CurrentVersion[:], &f.Epoch)

}

func (f *Fork) EncodingSizeSSZ() int {
	return 16
}

func (f *Fork) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(f.PreviousVersion[:], f.CurrentVersion[:], f.Epoch)
}
