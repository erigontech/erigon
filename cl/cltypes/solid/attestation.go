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
	"encoding/binary"
	"encoding/json"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/common/length"
	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/merkle_tree"
)

const (
	// agg bits offset: 4 bytes
	// attestationData: 128
	// Signature: 96 bytes
	attestationStaticBufferSize = 4 + AttestationDataBufferSize + 96

	// offset is usually always the same
	aggregationBitsOffset = 228
)

// Attestation type represents a statement or confirmation of some occurrence or phenomenon.
type Attestation struct {
	// Statically sized fields (aggregation bits offset, attestation data, and signature)
	staticBuffer [attestationStaticBufferSize]byte
	// Dynamic field to store aggregation bits
	aggregationBitsBuffer []byte
}

// AttestationWithGossipData type represents attestation with the gossip data where it's coming from.
type AttestationWithGossipData struct {
	Attestation *Attestation
	GossipData  *sentinel.GossipData
}

// Static returns whether the attestation is static or not. For Attestation, it's always false.
func (*Attestation) Static() bool {
	return false
}

func (a *Attestation) Copy() *Attestation {
	new := &Attestation{}
	copy(new.staticBuffer[:], a.staticBuffer[:])
	new.aggregationBitsBuffer = make([]byte, len(a.aggregationBitsBuffer))
	copy(new.aggregationBitsBuffer, a.aggregationBitsBuffer)
	return new
}

// NewAttestionFromParameters creates a new Attestation instance using provided parameters
func NewAttestionFromParameters(
	aggregationBits []byte,
	attestationData AttestationData,
	signature [96]byte,
) *Attestation {
	a := &Attestation{}
	binary.LittleEndian.PutUint32(a.staticBuffer[:4], aggregationBitsOffset)
	a.SetAttestationData(attestationData)
	a.SetSignature(signature)
	a.SetAggregationBits(aggregationBits)
	return a
}

func (a Attestation) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		AggregationBits hexutility.Bytes  `json:"aggregation_bits"`
		Signature       libcommon.Bytes96 `json:"signature"`
		Data            AttestationData   `json:"data"`
	}{
		AggregationBits: a.aggregationBitsBuffer,
		Signature:       a.Signature(),
		Data:            a.AttestantionData(),
	})
}

func (a *Attestation) UnmarshalJSON(buf []byte) error {
	var tmp struct {
		AggregationBits hexutility.Bytes  `json:"aggregation_bits"`
		Signature       libcommon.Bytes96 `json:"signature"`
		Data            AttestationData   `json:"data"`
	}
	tmp.Data = NewAttestationData()
	if err := json.Unmarshal(buf, &tmp); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(a.staticBuffer[:4], aggregationBitsOffset)
	a.SetAggregationBits(tmp.AggregationBits)
	a.SetSignature(tmp.Signature)
	a.SetAttestationData(tmp.Data)
	return nil
}

// AggregationBits returns the aggregation bits buffer of the Attestation instance.
func (a *Attestation) AggregationBits() []byte {
	buf := make([]byte, len(a.aggregationBitsBuffer))
	copy(buf, a.aggregationBitsBuffer)
	return buf
}

// SetAggregationBits sets the aggregation bits buffer of the Attestation instance.
func (a *Attestation) SetAggregationBits(bits []byte) {
	buf := make([]byte, len(bits))
	copy(buf, bits)
	a.aggregationBitsBuffer = buf
}

// AttestantionData returns the attestation data of the Attestation instance.
func (a *Attestation) AttestantionData() AttestationData {
	return (AttestationData)(a.staticBuffer[4:132])
}

// Signature returns the signature of the Attestation instance.
func (a *Attestation) Signature() (o [96]byte) {
	copy(o[:], a.staticBuffer[132:228])
	return
}

// SetAttestationData sets the attestation data of the Attestation instance.
func (a *Attestation) SetAttestationData(d AttestationData) {
	copy(a.staticBuffer[4:132], d)
}

// SetSignature sets the signature of the Attestation instance.
func (a *Attestation) SetSignature(signature [96]byte) {
	copy(a.staticBuffer[132:], signature[:])
}

// EncodingSizeSSZ returns the size of the Attestation instance when encoded in SSZ format.
func (a *Attestation) EncodingSizeSSZ() (size int) {
	size = attestationStaticBufferSize
	if a == nil {
		return
	}
	return size + len(a.aggregationBitsBuffer)
}

// DecodeSSZ decodes the provided buffer into the Attestation instance.
func (a *Attestation) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < attestationStaticBufferSize {
		return ssz.ErrLowBufferSize
	}
	copy(a.staticBuffer[:], buf)
	a.aggregationBitsBuffer = libcommon.CopyBytes(buf[aggregationBitsOffset:])
	return nil
}

// EncodeSSZ encodes the Attestation instance into the provided buffer.
func (a *Attestation) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	buf = append(buf, a.staticBuffer[:]...)
	buf = append(buf, a.aggregationBitsBuffer...)
	return buf, nil
}

// CopyHashBufferTo copies the hash buffer of the Attestation instance to the provided byte slice.
func (a *Attestation) CopyHashBufferTo(o []byte) error {
	for i := 0; i < 128; i++ {
		o[i] = 0
	}
	aggBytesRoot, err := merkle_tree.BitlistRootWithLimit(a.AggregationBits(), 2048)
	if err != nil {
		return err
	}
	dataRoot, err := a.AttestantionData().HashSSZ()
	if err != nil {
		return err
	}
	copy(o[:128], a.staticBuffer[132:228])
	if err = merkle_tree.InPlaceRoot(o); err != nil {
		return err
	}
	copy(o[64:], o[:32])
	copy(o[:32], aggBytesRoot[:])
	copy(o[32:64], dataRoot[:])
	return nil
}

// HashSSZ hashes the Attestation instance using SSZ.
// It creates a byte slice `leaves` with a size based on length.Hash,
// then fills this slice with the values from the Attestation's hash buffer.
func (a *Attestation) HashSSZ() (o [32]byte, err error) {
	leaves := make([]byte, length.Hash*4)
	if err = a.CopyHashBufferTo(leaves); err != nil {
		return
	}
	err = merkle_tree.MerkleRootFromFlatLeaves(leaves, o[:])
	return
}

// Clone creates a new clone of the Attestation instance.
// This can be useful for creating copies without changing the original object.
func (a *Attestation) Clone() clonable.Clonable {
	if a == nil {
		return &Attestation{}
	}
	var staticBuffer [attestationStaticBufferSize]byte
	var bitsBuffer []byte
	copy(staticBuffer[:], a.staticBuffer[:])
	if a.aggregationBitsBuffer != nil {
		bitsBuffer = make([]byte, len(a.aggregationBitsBuffer))
		copy(bitsBuffer, a.aggregationBitsBuffer)
	}
	return &Attestation{
		aggregationBitsBuffer: bitsBuffer,
		staticBuffer:          staticBuffer,
	}
}
