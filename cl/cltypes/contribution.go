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
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
)

var _ ssz2.SizedObjectSSZ = (*ContributionAndProof)(nil)
var _ ssz2.SizedObjectSSZ = (*Contribution)(nil)

/*
 * ContributionAndProof contains the index of the aggregator, the attestation
 * to be aggregated and the BLS signature of the attestation.
 */
type ContributionAndProof struct {
	AggregatorIndex uint64         `json:"aggregator_index,string"`
	Contribution    *Contribution  `json:"contribution"`
	SelectionProof  common.Bytes96 `json:"selection_proof"`
}

func (a *ContributionAndProof) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, &a.AggregatorIndex, a.Contribution, a.SelectionProof[:])
}

func (a *ContributionAndProof) Static() bool {
	return true
}

func (a *ContributionAndProof) DecodeSSZ(buf []byte, version int) error {
	a.Contribution = new(Contribution)
	return ssz2.UnmarshalSSZ(buf, version, &a.AggregatorIndex, a.Contribution, a.SelectionProof[:])
}

func (a *ContributionAndProof) EncodingSizeSSZ() int {
	return length.BlockNum + length.Bytes96 + a.Contribution.EncodingSizeSSZ()
}

func (a *ContributionAndProof) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(a.AggregatorIndex, a.Contribution, a.SelectionProof[:])
}

type SignedContributionAndProof struct {
	Message   *ContributionAndProof `json:"message"`
	Signature common.Bytes96        `json:"signature"`
}

func (a *SignedContributionAndProof) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, a.Message, a.Signature[:])
}

func (a *SignedContributionAndProof) DecodeSSZ(buf []byte, version int) error {
	a.Message = new(ContributionAndProof)
	a.Message.Contribution = new(Contribution)
	return ssz2.UnmarshalSSZ(buf, version, a.Message, a.Signature[:])
}

func (a *SignedContributionAndProof) EncodingSizeSSZ() int {
	return length.Bytes96 + a.Message.EncodingSizeSSZ()
	// return 100 + a.Message.EncodingSizeSSZ()
}

func (a *SignedContributionAndProof) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(a.Message, a.Signature[:])
}

// DefaultSyncCommitteeAggregationBitsSize is the mainnet default: 512/4/8 = 16 bytes.
const DefaultSyncCommitteeAggregationBitsSize = 16

type Contribution struct {
	Slot              uint64         `json:"slot,string"`
	BeaconBlockRoot   common.Hash    `json:"beacon_block_root"`
	SubcommitteeIndex uint64         `json:"subcommittee_index,string"`
	AggregationBits   hexutil.Bytes  `json:"aggregation_bits"`
	Signature         common.Bytes96 `json:"signature"`

	aggregationBitsSize int // config-driven; 0 means use DefaultSyncCommitteeAggregationBitsSize
}

// SetAggregationBitsSize overrides the aggregation bits byte length for non-mainnet presets.
func (a *Contribution) SetAggregationBitsSize(size int) {
	a.aggregationBitsSize = size
}

func (a *Contribution) getAggregationBitsSize() int {
	if a.aggregationBitsSize > 0 {
		return a.aggregationBitsSize
	}
	return DefaultSyncCommitteeAggregationBitsSize
}

type ContributionKey struct {
	Slot              uint64      `json:"slot,string"`
	BeaconBlockRoot   common.Hash `json:"beacon_block_root"`
	SubcommitteeIndex uint64      `json:"subcommittee_index,string"`
}

func (a *Contribution) EncodeSSZ(dst []byte) ([]byte, error) {
	if len(a.AggregationBits) == 0 {
		a.AggregationBits = make([]byte, a.getAggregationBitsSize())
	}
	return ssz2.MarshalSSZ(dst, &a.Slot, a.BeaconBlockRoot[:], &a.SubcommitteeIndex, []byte(a.AggregationBits), a.Signature[:])
}

func (a *Contribution) Static() bool {
	return true
}

func (a *Contribution) Copy() *Contribution {
	ret := *a
	ret.AggregationBits = append([]byte{}, a.AggregationBits...)
	return &ret
}

func (a *Contribution) DecodeSSZ(buf []byte, version int) error {
	a.AggregationBits = make([]byte, a.getAggregationBitsSize())
	return ssz2.UnmarshalSSZ(buf, version, &a.Slot, a.BeaconBlockRoot[:], &a.SubcommitteeIndex, []byte(a.AggregationBits), a.Signature[:])
}

func (a *Contribution) EncodingSizeSSZ() int {
	bitsSize := len(a.AggregationBits)
	if bitsSize == 0 {
		bitsSize = a.getAggregationBitsSize()
	}
	return length.BlockNum*2 + length.Hash + length.Bytes96 + bitsSize
}

func (a *Contribution) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(&a.Slot, a.BeaconBlockRoot[:], &a.SubcommitteeIndex, []byte(a.AggregationBits), a.Signature[:])
}

type SyncCommitteeMessage struct {
	Slot            uint64         `json:"slot,string"`
	BeaconBlockRoot common.Hash    `json:"beacon_block_root"`
	ValidatorIndex  uint64         `json:"validator_index,string"`
	Signature       common.Bytes96 `json:"signature"`
}

func (a *SyncCommitteeMessage) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, &a.Slot, a.BeaconBlockRoot[:], &a.ValidatorIndex, a.Signature[:])
}

func (a *SyncCommitteeMessage) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, &a.Slot, a.BeaconBlockRoot[:], &a.ValidatorIndex, a.Signature[:])
}

func (a *SyncCommitteeMessage) EncodingSizeSSZ() int {
	return 144
}

func (a *SyncCommitteeMessage) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(&a.Slot, a.BeaconBlockRoot[:], &a.ValidatorIndex, a.Signature[:])
}

func (a *SyncCommitteeMessage) Static() bool {
	return true
}

func (*SyncCommitteeMessage) Clone() clonable.Clonable {
	return &SyncCommitteeMessage{}
}
