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
	"encoding/json"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

const (
	maxValidatorsPerCommittee  = 2048
	maxCommitteesPerSlot       = 64
	aggregationBitsSizeDeneb   = maxValidatorsPerCommittee
	aggregationBitsSizeElectra = maxCommitteesPerSlot * maxValidatorsPerCommittee
)

// Attestation type represents a statement or confirmation of some occurrence or phenomenon.
type Attestation struct {
	AggregationBits *BitList          `json:"aggregation_bits"`
	Data            *AttestationData  `json:"data"`
	Signature       libcommon.Bytes96 `json:"signature"`
	CommitteeBits   *BitVector        `json:"committee_bits,omitempty"` // Electra EIP-7549
}

// Static returns whether the attestation is static or not. For Attestation, it's always false.
func (*Attestation) Static() bool {
	return false
}

func (a *Attestation) Copy() *Attestation {
	new := &Attestation{}
	new.AggregationBits = a.AggregationBits.Copy()
	new.Data = &AttestationData{}
	*new.Data = *a.Data
	new.Signature = a.Signature
	new.CommitteeBits = a.CommitteeBits.Copy()
	return new
}

// EncodingSizeSSZ returns the size of the Attestation instance when encoded in SSZ format.
func (a *Attestation) EncodingSizeSSZ() (size int) {
	if a.CommitteeBits != nil {
		// Electra case
		return 4 + AttestationDataSize + length.Bytes96 + 4 +
			a.AggregationBits.EncodingSizeSSZ() +
			a.CommitteeBits.EncodingSizeSSZ()
	}
	// Deneb case
	size = AttestationDataSize + length.Bytes96
	if a == nil || a.AggregationBits == nil {
		return
	}
	return size + a.AggregationBits.EncodingSizeSSZ() + 4 // 4 bytes for the length of the size offset
}

// DecodeSSZ decodes the provided buffer into the Attestation instance.
func (a *Attestation) DecodeSSZ(buf []byte, version int) error {
	clversion := clparams.StateVersion(version)
	if clversion.AfterOrEqual(clparams.ElectraVersion) {
		a.AggregationBits = NewBitList(0, aggregationBitsSizeElectra)
		a.Data = &AttestationData{}
		a.CommitteeBits = NewBitVector(maxCommitteesPerSlot)
		return ssz2.UnmarshalSSZ(buf, version, a.AggregationBits, a.Data, a.Signature[:], a.CommitteeBits)
	}

	// Deneb case
	if len(buf) < a.EncodingSizeSSZ() {
		return ssz.ErrLowBufferSize
	}
	a.AggregationBits = NewBitList(0, aggregationBitsSizeDeneb)
	a.Data = &AttestationData{}
	return ssz2.UnmarshalSSZ(buf, version, a.AggregationBits, a.Data, a.Signature[:])
}

// EncodeSSZ encodes the Attestation instance into the provided buffer.
func (a *Attestation) EncodeSSZ(dst []byte) ([]byte, error) {
	if a.CommitteeBits != nil {
		// Electra case
		return ssz2.MarshalSSZ(dst, a.AggregationBits, a.Data, a.Signature[:], a.CommitteeBits)
	}
	return ssz2.MarshalSSZ(dst, a.AggregationBits, a.Data, a.Signature[:])
}

// HashSSZ hashes the Attestation instance using SSZ.
func (a *Attestation) HashSSZ() (o [32]byte, err error) {
	if a.CommitteeBits != nil {
		// Electra case
		return merkle_tree.HashTreeRoot(a.AggregationBits, a.Data, a.Signature[:], a.CommitteeBits)
	}
	return merkle_tree.HashTreeRoot(a.AggregationBits, a.Data, a.Signature[:])
}

// Clone creates a new clone of the Attestation instance.
func (a *Attestation) Clone() clonable.Clonable {
	return &Attestation{}
}

// Implement custom json unmarshalling for Attestation.
func (a *Attestation) UnmarshalJSON(data []byte) error {
	// Unmarshal as normal into a temporary struct
	type tempAttestation struct {
		AggregationBits *BitList          `json:"aggregation_bits"`
		Data            *AttestationData  `json:"data"`
		Signature       libcommon.Bytes96 `json:"signature"`
		CommitteeBits   *BitVector        `json:"committee_bits,omitempty"`
	}

	// For Electra, the committee bits are present in the JSON
	if bytes.Contains(data, []byte("committee_bits")) {
		// Electra case
		var temp tempAttestation
		temp.AggregationBits = NewBitList(0, aggregationBitsSizeElectra)
		temp.CommitteeBits = NewBitVector(maxCommitteesPerSlot)
		if err := json.Unmarshal(data, &temp); err != nil {
			return err
		}
		a.AggregationBits = temp.AggregationBits
		a.Data = temp.Data
		a.Signature = temp.Signature
		a.CommitteeBits = temp.CommitteeBits
	}

	// Deneb case
	var temp tempAttestation
	temp.AggregationBits = NewBitList(0, aggregationBitsSizeDeneb)
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}
	// Copy the temporary struct into the actual struct
	a.AggregationBits = temp.AggregationBits
	a.Data = temp.Data
	a.Signature = temp.Signature
	return nil
}
