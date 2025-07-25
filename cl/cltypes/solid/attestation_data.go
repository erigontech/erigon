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

package solid

import (
	"bytes"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

const AttestationDataSize = 128

// AttestationData contains information about attestantion, including finalized/attested checkpoints.
type AttestationData struct {
	Slot           uint64 `json:"slot,string"`
	CommitteeIndex uint64 `json:"index,string"` // CommitteeIndex will be deprecated and always equal to 0 after Electra
	// LMD GHOST vote
	BeaconBlockRoot common.Hash `json:"beacon_block_root"`
	// FFG vote
	Source Checkpoint `json:"source"`
	Target Checkpoint `json:"target"`
}

func (a *AttestationData) Static() bool {
	return true
}

func (a *AttestationData) EncodingSizeSSZ() int {
	return AttestationDataSize
}

func (a *AttestationData) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, &a.Slot, &a.CommitteeIndex, a.BeaconBlockRoot[:], &a.Source, &a.Target)
}

func (a *AttestationData) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, a.Slot, a.CommitteeIndex, a.BeaconBlockRoot[:], &a.Source, &a.Target)
}

func (a *AttestationData) Clone() clonable.Clonable {
	return &AttestationData{}
}

func (a *AttestationData) HashSSZ() (o [32]byte, err error) {
	return merkle_tree.HashTreeRoot(a.Slot, a.CommitteeIndex, a.BeaconBlockRoot[:], &a.Source, &a.Target)
}

func (a *AttestationData) Equal(other *AttestationData) bool {
	return a.Slot == other.Slot && a.CommitteeIndex == other.CommitteeIndex && bytes.Equal(a.BeaconBlockRoot[:], other.BeaconBlockRoot[:]) &&
		a.Source.Equal(other.Source) && a.Target.Equal(other.Target)
}
