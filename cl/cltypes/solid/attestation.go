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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/ssz"
)

const (
	maxValidatorsPerCommittee  = 2048
	aggregationBitsSizeDeneb   = maxValidatorsPerCommittee
	aggregationBitsSizeElectra = 64 * maxValidatorsPerCommittee // mainnet MAX_COMMITTEES_PER_SLOT * MAX_VALIDATORS_PER_COMMITTEE
)

// Attestation type represents a statement or confirmation of some occurrence or phenomenon.
type Attestation struct {
	AggregationBits *BitList         `json:"aggregation_bits"`
	Data            *AttestationData `json:"data"`
	Signature       common.Bytes96   `json:"signature"`
	CommitteeBits   *BitVector       `json:"committee_bits,omitempty"` // Electra EIP-7549
	version         clparams.StateVersion
}

func (a *Attestation) SetVersion(version clparams.StateVersion) {
	a.version = version
}

func (a *Attestation) GetCommitteeIndexFromBits() (uint64, error) {
	bits := a.CommitteeBits.GetOnIndices()
	if len(bits) == 0 {
		return 0, errors.New("no committee bits set in electra attestation")
	}
	return uint64(bits[0]), nil
}

// SetBeaconConfig sets the beacon config for preset-aware hash computation.
// This must be called on Electra attestations before computing HashSSZ when the
// AggregationBits limit needs to match a specific preset (e.g. minimal vs mainnet).
func (a *Attestation) SetBeaconConfig(cfg *clparams.BeaconChainConfig) {
	if a == nil || cfg == nil || a.AggregationBits == nil || a.CommitteeBits == nil {
		return
	}
	a.AggregationBits.SetLimit(int(cfg.MaxCommitteesPerSlot) * maxValidatorsPerCommittee)
}

func (a *Attestation) ValidateForConfig(cfg *clparams.BeaconChainConfig, version clparams.StateVersion) error {
	if a == nil || cfg == nil || a.AggregationBits == nil || a.Data == nil {
		return errors.New("invalid attestation")
	}
	aggregationBitsLimit := maxValidatorsPerCommittee
	if version >= clparams.ElectraVersion {
		aggregationBitsLimit = int(cfg.MaxCommitteesPerSlot) * maxValidatorsPerCommittee
	}
	if a.AggregationBits.Bits() > aggregationBitsLimit {
		return fmt.Errorf("aggregation bits length exceeds limit: %d > %d", a.AggregationBits.Bits(), aggregationBitsLimit)
	}
	a.AggregationBits.SetLimit(aggregationBitsLimit)
	if version < clparams.ElectraVersion {
		if a.CommitteeBits != nil {
			return errors.New("committee bits before Electra")
		}
		return nil
	}
	if a.CommitteeBits == nil {
		return errors.New("missing committee bits after Electra")
	}
	if err := a.CommitteeBits.ValidateSize(int(cfg.MaxCommitteesPerSlot)); err != nil {
		return fmt.Errorf("invalid committee bits: %w", err)
	}
	return nil
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
	copy(new.Signature[:], a.Signature[:])
	new.CommitteeBits = a.CommitteeBits.Copy()
	new.version = a.version
	return new
}

// EncodingSizeSSZ returns the size of the Attestation instance when encoded in SSZ format.
func (a *Attestation) EncodingSizeSSZ() (size int) {
	if a.CommitteeBits != nil {
		// Electra case
		return 4 + AttestationDataSize + length.Bytes96 +
			a.CommitteeBits.EncodingSizeSSZ() +
			a.AggregationBits.EncodingSizeSSZ()
	}
	// Deneb case
	size = AttestationDataSize + length.Bytes96
	if a == nil || a.AggregationBits == nil {
		return
	}
	return size + a.AggregationBits.EncodingSizeSSZ() + 4 // 4 bytes for the length of the size offset
}

// DecodeSSZ infers the committee vector width for preset-agnostic nested decoding; trust boundaries must call ValidateForConfig.
func (a *Attestation) DecodeSSZ(buf []byte, version int) error {
	return a.DecodeSSZWithConfig(buf, version, nil)
}

// DecodeSSZWithConfig decodes the provided buffer using cfg when it is available.
func (a *Attestation) DecodeSSZWithConfig(buf []byte, version int, cfg *clparams.BeaconChainConfig) error {
	clversion := clparams.StateVersion(version)
	a.version = clversion
	if clversion.AfterOrEqual(clparams.ElectraVersion) {
		// The CommitteeBits size depends on MAX_COMMITTEES_PER_SLOT which differs between
		// mainnet (64) and the minimal preset (4). Instead of hardcoding 64, infer the
		// CommitteeBits byte count from the SSZ offset table.
		// Layout: [4-byte offset][AttestationData][Signature][CommitteeBits][AggregationBits]
		const electraFixedHeaderSize = 4 + AttestationDataSize + length.Bytes96
		if len(buf) < electraFixedHeaderSize+1 {
			return ssz.ErrLowBufferSize
		}
		aggrBitsOffset := int(binary.LittleEndian.Uint32(buf[:4]))
		committeeBitsBytes := aggrBitsOffset - electraFixedHeaderSize
		if committeeBitsBytes <= 0 {
			return ssz.ErrLowBufferSize
		}
		committeeBitsLimit := committeeBitsBytes * 8
		if cfg != nil && cfg.MaxCommitteesPerSlot > 0 {
			committeeBitsLimit = int(cfg.MaxCommitteesPerSlot)
			expectedBytes := (committeeBitsLimit + 7) / 8
			if committeeBitsBytes != expectedBytes {
				return fmt.Errorf("invalid committee bits byte length: %d != %d", committeeBitsBytes, expectedBytes)
			}
		}
		aggrBitsLimit := aggregationBitsSizeElectra
		if cfg != nil && cfg.MaxCommitteesPerSlot > 0 {
			aggrBitsLimit = int(cfg.MaxCommitteesPerSlot) * maxValidatorsPerCommittee
		}
		a.AggregationBits = NewBitList(0, aggrBitsLimit)
		a.Data = &AttestationData{}
		a.CommitteeBits = NewBitVector(committeeBitsLimit)
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
	if a.version >= clparams.GloasVersion {
		return a.HashSSZProgressive()
	}
	if a.CommitteeBits != nil {
		// Electra case
		return merkle_tree.HashTreeRoot(a.AggregationBits, a.Data, a.Signature[:], a.CommitteeBits)
	}
	return merkle_tree.HashTreeRoot(a.AggregationBits, a.Data, a.Signature[:])
}

func (a *Attestation) HashSSZProgressive() ([32]byte, error) {
	aggregationBitsRoot, err := a.AggregationBits.HashSSZProgressive()
	if err != nil {
		return [32]byte{}, err
	}
	if a.CommitteeBits != nil {
		return merkle_tree.ProgressiveContainerRootAll(aggregationBitsRoot[:], a.Data, a.Signature[:], a.CommitteeBits)
	}
	return merkle_tree.ProgressiveContainerRootAll(aggregationBitsRoot[:], a.Data, a.Signature[:])
}

// Clone creates a new clone of the Attestation instance.
func (a *Attestation) Clone() clonable.Clonable {
	return &Attestation{}
}

// Implement custom json unmarshalling for Attestation.
func (a *Attestation) UnmarshalJSON(data []byte) error {
	// Unmarshal as normal into a temporary struct
	type tempAttestation struct {
		AggregationBits *BitList         `json:"aggregation_bits"`
		Data            *AttestationData `json:"data"`
		Signature       common.Bytes96   `json:"signature"`
		CommitteeBits   *BitVector       `json:"committee_bits,omitempty"`
	}

	// For Electra, the committee bits are present in the JSON
	if bytes.Contains(data, []byte("committee_bits")) {
		// Electra case — preserve existing limit if SetBeaconConfig was called
		aggrBitsLimit := aggregationBitsSizeElectra
		if a.AggregationBits != nil && a.AggregationBits.Cap() > 0 {
			aggrBitsLimit = a.AggregationBits.Cap()
		}
		var temp tempAttestation
		temp.AggregationBits = NewBitList(0, aggrBitsLimit)
		temp.CommitteeBits = &BitVector{} // UnmarshalJSON self-sizes from the hex bytes
		if err := json.Unmarshal(data, &temp); err != nil {
			return err
		}
		a.AggregationBits = temp.AggregationBits
		a.Data = temp.Data
		a.Signature = temp.Signature
		a.CommitteeBits = temp.CommitteeBits
		return nil
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

// class SingleAttestation(Container):
//
//	committee_index: CommitteeIndex
//	attester_index: ValidatorIndex
//	data: AttestationData
//	signature: BLSSignature
type SingleAttestation struct {
	CommitteeIndex uint64           `json:"committee_index,string"`
	AttesterIndex  uint64           `json:"attester_index,string"`
	Data           *AttestationData `json:"data"`
	Signature      common.Bytes96   `json:"signature"`
}

func (s *SingleAttestation) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, &s.CommitteeIndex, &s.AttesterIndex, s.Data, s.Signature[:])
}

func (s *SingleAttestation) DecodeSSZ(buf []byte, version int) error {
	s.Data = &AttestationData{}
	return ssz2.UnmarshalSSZ(buf, version, &s.CommitteeIndex, &s.AttesterIndex, s.Data, s.Signature[:])
}

func (s *SingleAttestation) EncodingSizeSSZ() (size int) {
	return 8 + 8 + AttestationDataSize + length.Bytes96
}

func (s *SingleAttestation) HashSSZ() (o [32]byte, err error) {
	return merkle_tree.HashTreeRoot(&s.CommitteeIndex, &s.AttesterIndex, s.Data, s.Signature[:])
}

func (s *SingleAttestation) Clone() clonable.Clonable {
	return &SingleAttestation{
		Data: &AttestationData{},
	}
}

func (s *SingleAttestation) Static() bool {
	return true
}

func (s *SingleAttestation) ToAttestation(memberIndexInCommittee int, committeeLen int, maxCommittees int, cfg *clparams.BeaconChainConfig) *Attestation {
	committeeBits := NewBitVector(maxCommittees)
	committeeBits.SetBitAt(int(s.CommitteeIndex), true)
	// flip the bit for the validator and also mark the last bit
	bytes := make([]byte, committeeLen/8+1)
	bytes[memberIndexInCommittee/8] |= 1 << (memberIndexInCommittee % 8)
	bytes[committeeLen/8] |= 1 << (committeeLen % 8)
	aggrBitsLimit := aggregationBitsSizeElectra
	if cfg != nil && cfg.MaxCommitteesPerSlot > 0 {
		aggrBitsLimit = int(cfg.MaxCommitteesPerSlot) * maxValidatorsPerCommittee
	}
	aggregationBits := BitlistFromBytes(bytes, aggrBitsLimit)
	attestation := &Attestation{
		AggregationBits: aggregationBits,
		Data:            s.Data,
		Signature:       s.Signature,
		CommitteeBits:   committeeBits,
	}
	if cfg != nil && cfg.SlotsPerEpoch > 0 && s.Data != nil {
		attestation.SetVersion(cfg.GetCurrentStateVersion(s.Data.Slot / cfg.SlotsPerEpoch))
	}
	return attestation
}

func (s *SingleAttestation) AttestationData() *AttestationData {
	return s.Data
}
