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
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

/*
 * AggregateAndProof contains the index of the aggregator, the attestation
 * to be aggregated and the BLS signature of the attestation.
 */
type AggregateAndProof struct {
	AggregatorIndex uint64             `json:"aggregator_index,string"`
	Aggregate       *solid.Attestation `json:"aggregate"`
	SelectionProof  common.Bytes96     `json:"selection_proof"`
}

func (a *AggregateAndProof) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, a.AggregatorIndex, a.Aggregate, a.SelectionProof[:])
}

func (a *AggregateAndProof) Static() bool {
	return false
}

func (a *AggregateAndProof) DecodeSSZ(buf []byte, version int) error {
	a.Aggregate = new(solid.Attestation)
	return ssz2.UnmarshalSSZ(buf, version, &a.AggregatorIndex, a.Aggregate, a.SelectionProof[:])
}

func (a *AggregateAndProof) EncodingSizeSSZ() int {
	return 108 + a.Aggregate.EncodingSizeSSZ()
}

func (a *AggregateAndProof) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(a.AggregatorIndex, a.Aggregate, a.SelectionProof[:])
}

type SignedAggregateAndProof struct {
	Message   *AggregateAndProof `json:"message"`
	Signature common.Bytes96     `json:"signature"`
}

func (a *SignedAggregateAndProof) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, a.Message, a.Signature[:])
}

func (a *SignedAggregateAndProof) DecodeSSZ(buf []byte, version int) error {
	a.Message = new(AggregateAndProof)
	return ssz2.UnmarshalSSZ(buf, version, a.Message, a.Signature[:])
}

func (a *SignedAggregateAndProof) EncodingSizeSSZ() int {
	return 100 + a.Message.EncodingSizeSSZ()
}

func (a *SignedAggregateAndProof) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(a.Message, a.Signature[:])
}

/*
 * SyncAggregate, Determines successful committee, bits shows active participants,
 * and signature is the aggregate BLS signature of the committee.
 */
type SyncAggregate struct {
	SyncCommiteeBits      common.Bytes64 `json:"sync_committee_bits"`
	SyncCommiteeSignature common.Bytes96 `json:"sync_committee_signature"`
}

func NewSyncAggregate() *SyncAggregate {
	return &SyncAggregate{}
}

// return sum of the committee bits
func (agg *SyncAggregate) Sum() int {
	ret := 0
	for i := range agg.SyncCommiteeBits {
		for bit := 1; bit <= 128; bit *= 2 {
			if agg.SyncCommiteeBits[i]&byte(bit) > 0 {
				ret++
			}
		}
	}
	return ret
}

func (agg *SyncAggregate) IsSet(idx uint64) bool {
	if idx >= 2048 {
		return false
	}
	return agg.SyncCommiteeBits[idx/8]&(1<<(idx%8)) > 0
}

func (agg *SyncAggregate) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, agg.SyncCommiteeBits[:], agg.SyncCommiteeSignature[:])
}

func (*SyncAggregate) Static() bool {
	return true
}

func (agg *SyncAggregate) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, agg.SyncCommiteeBits[:], agg.SyncCommiteeSignature[:])
}

func (agg *SyncAggregate) EncodingSizeSSZ() int {
	return 160
}

func (agg *SyncAggregate) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(agg.SyncCommiteeBits[:], agg.SyncCommiteeSignature[:])

}
