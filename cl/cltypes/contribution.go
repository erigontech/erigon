package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

var _ ssz2.SizedObjectSSZ = (*ContributionAndProof)(nil)
var _ ssz2.SizedObjectSSZ = (*Contribution)(nil)

/*
 * ContributionAndProof contains the index of the aggregator, the attestation
 * to be aggregated and the BLS signature of the attestation.
 */
type ContributionAndProof struct {
	AggregatorIndex uint64            `json:"aggregator_index,string"`
	Contribution    *Contribution     `json:"contribution"`
	SelectionProof  libcommon.Bytes96 `json:"selection_proof"`
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
	Signature libcommon.Bytes96     `json:"signature"`
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

var SyncCommitteeAggregationBitsSize = 16

type Contribution struct {
	Slot              uint64            `json:"slot,string"`
	BeaconBlockRoot   libcommon.Hash    `json:"beacon_block_root"`
	SubcommitteeIndex uint64            `json:"subcommittee_index,string"`
	AggregationBits   hexutility.Bytes  `json:"aggregation_bits"`
	Signature         libcommon.Bytes96 `json:"signature"`
}

type ContributionKey struct {
	Slot              uint64         `json:"slot,string"`
	BeaconBlockRoot   libcommon.Hash `json:"beacon_block_root"`
	SubcommitteeIndex uint64         `json:"subcommittee_index,string"`
}

func (a *Contribution) EncodeSSZ(dst []byte) ([]byte, error) {
	if len(a.AggregationBits) == 0 {
		a.AggregationBits = make([]byte, SyncCommitteeAggregationBitsSize)
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
	a.AggregationBits = make([]byte, SyncCommitteeAggregationBitsSize)
	return ssz2.UnmarshalSSZ(buf, version, &a.Slot, a.BeaconBlockRoot[:], &a.SubcommitteeIndex, []byte(a.AggregationBits), a.Signature[:])
}

func (a *Contribution) EncodingSizeSSZ() int {
	return length.BlockNum*2 + length.Hash + length.Bytes96 + len(a.AggregationBits)
}

func (a *Contribution) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(&a.Slot, a.BeaconBlockRoot[:], &a.SubcommitteeIndex, []byte(a.AggregationBits), a.Signature[:])
}

/*
 * SyncContribution, Determines successful committee, bits shows active participants,
 * and signature is the aggregate BLS signature of the committee.
 */
type SyncContribution struct {
	SyncCommiteeBits      libcommon.Bytes64 `json:"sync_committee_bits"`
	SyncCommiteeSignature libcommon.Bytes96 `json:"signature"`
}

// return sum of the committee bits
func (agg *SyncContribution) Sum() int {
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

func (agg *SyncContribution) IsSet(idx uint64) bool {
	if idx >= 2048 {
		return false
	}
	return agg.SyncCommiteeBits[idx/8]&(1<<(idx%8)) > 0
}

func (agg *SyncContribution) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, append(agg.SyncCommiteeBits[:], agg.SyncCommiteeSignature[:]...)...), nil
}

func (*SyncContribution) Static() bool {
	return true
}

func (agg *SyncContribution) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, agg.SyncCommiteeBits[:], agg.SyncCommiteeSignature[:])
}

func (agg *SyncContribution) EncodingSizeSSZ() int {
	return 160
}

func (agg *SyncContribution) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(agg.SyncCommiteeBits[:], agg.SyncCommiteeSignature[:])

}

type SyncCommitteeMessage struct {
	Slot            uint64            `json:"slot,string"`
	BeaconBlockRoot libcommon.Hash    `json:"beacon_block_root"`
	ValidatorIndex  uint64            `json:"validator_index,string"`
	Signature       libcommon.Bytes96 `json:"signature"`
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
