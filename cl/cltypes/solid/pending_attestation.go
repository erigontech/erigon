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
	"encoding/json"
	"errors"

	"github.com/erigontech/erigon-lib/common/length"

	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

type PendingAttestation struct {
	AggregationBits *BitList         `json:"aggregation_bits"`
	Data            *AttestationData `json:"attestation_data"`
	InclusionDelay  uint64           `json:"inclusion_delay,string"`
	ProposerIndex   uint64           `json:"proposer_index,string"`
}

func (a *PendingAttestation) EncodingSizeSSZ() (size int) {
	size = 4 + AttestationDataSize + 2*length.BlockNum // 4 bytes for the length of the size offset
	if a == nil || a.AggregationBits == nil {
		return
	}
	return size + a.AggregationBits.EncodingSizeSSZ()
}

func (a *PendingAttestation) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < a.EncodingSizeSSZ() {
		return ssz.ErrLowBufferSize
	}
	a.AggregationBits = NewBitList(0, 2048)
	a.Data = &AttestationData{}
	return ssz2.UnmarshalSSZ(buf, 0, a.AggregationBits, a.Data, &a.InclusionDelay, &a.ProposerIndex)
}

func (a *PendingAttestation) EncodeSSZ(dst []byte) ([]byte, error) {
	if a.Data == nil {
		return nil, errors.New("attestation data is nil")
	}
	return ssz2.MarshalSSZ(dst, a.AggregationBits, a.Data, a.InclusionDelay, a.ProposerIndex)
}

func (a *PendingAttestation) HashSSZ() (o [32]byte, err error) {
	return merkle_tree.HashTreeRoot(a.AggregationBits, a.Data, a.InclusionDelay, a.ProposerIndex)
}

func (*PendingAttestation) Clone() clonable.Clonable {
	return &PendingAttestation{}
}

// Implement custom json unmarshalling for Attestation.
func (p *PendingAttestation) UnmarshalJSON(data []byte) error {
	// Unmarshal as normal into a temporary struct
	type tempPendingAttestation struct {
		AggregationBits *BitList         `json:"aggregation_bits"`
		Data            *AttestationData `json:"attestation_data"`
		InclusionDelay  uint64           `json:"inclusion_delay,string"`
		ProposerIndex   uint64           `json:"proposer_index,string"`
	}
	var temp tempPendingAttestation
	temp.AggregationBits = NewBitList(0, 2048)
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}
	// Copy the temporary struct into the actual struct
	p.AggregationBits = temp.AggregationBits
	p.Data = temp.Data
	p.InclusionDelay = temp.InclusionDelay
	p.ProposerIndex = temp.ProposerIndex
	return nil
}
