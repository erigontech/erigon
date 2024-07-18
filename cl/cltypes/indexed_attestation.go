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
	"encoding/json"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

/*
 * IndexedAttestation are attestantions sets to prove that someone misbehaved.
 */
type IndexedAttestation struct {
	AttestingIndices *solid.RawUint64List  `json:"attesting_indices"`
	Data             solid.AttestationData `json:"data"`
	Signature        libcommon.Bytes96     `json:"signature"`
}

func NewIndexedAttestation() *IndexedAttestation {
	return &IndexedAttestation{
		AttestingIndices: solid.NewRawUint64List(2048, nil),
		Data:             solid.NewAttestationData(),
	}
}

func (i *IndexedAttestation) Static() bool {
	return false
}

func (i *IndexedAttestation) UnmarshalJSON(buf []byte) error {
	var tmp struct {
		AttestingIndices *solid.RawUint64List  `json:"attesting_indices"`
		Data             solid.AttestationData `json:"data"`
		Signature        libcommon.Bytes96     `json:"signature"`
	}
	tmp.AttestingIndices = solid.NewRawUint64List(2048, nil)
	tmp.Data = solid.NewAttestationData()
	if err := json.Unmarshal(buf, &tmp); err != nil {
		return err
	}
	i.AttestingIndices = tmp.AttestingIndices
	i.Data = tmp.Data
	i.Signature = tmp.Signature
	return nil
}

func (i *IndexedAttestation) EncodeSSZ(buf []byte) (dst []byte, err error) {
	return ssz2.MarshalSSZ(buf, i.AttestingIndices, i.Data, i.Signature[:])
}

// DecodeSSZ ssz unmarshals the IndexedAttestation object
func (i *IndexedAttestation) DecodeSSZ(buf []byte, version int) error {
	i.Data = solid.NewAttestationData()
	i.AttestingIndices = solid.NewRawUint64List(2048, nil)

	return ssz2.UnmarshalSSZ(buf, version, i.AttestingIndices, i.Data, i.Signature[:])
}

// EncodingSizeSSZ returns the ssz encoded size in bytes for the IndexedAttestation object
func (i *IndexedAttestation) EncodingSizeSSZ() int {
	return 228 + i.AttestingIndices.EncodingSizeSSZ()
}

// HashSSZ ssz hashes the IndexedAttestation object
func (i *IndexedAttestation) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(i.AttestingIndices, i.Data, i.Signature[:])
}

func IsSlashableAttestationData(d1, d2 solid.AttestationData) bool {
	return (!d1.Equal(d2) && d1.Target().Epoch() == d2.Target().Epoch()) ||
		(d1.Source().Epoch() < d2.Source().Epoch() && d2.Target().Epoch() < d1.Target().Epoch())
}
