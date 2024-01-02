package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

/*
 * ContributionAndProof contains the index of the aggregator, the attestation
 * to be aggregated and the BLS signature of the attestation.
 */
type ContributionAndProof struct {
	AggregatorIndex uint64              `json:"aggregator_index,string"`
	SelectionProof  libcommon.Bytes96   `json:"selection_proof"`
	Contribution    *solid.Contribution `json:"contribution"`
}

func (a *ContributionAndProof) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, a.AggregatorIndex, a.Contribution, a.SelectionProof[:])
}

func (a *ContributionAndProof) Static() bool {
	return false
}

func (a *ContributionAndProof) DecodeSSZ(buf []byte, version int) error {
	a.Contribution = new(solid.Contribution)
	return ssz2.UnmarshalSSZ(buf, version, &a.AggregatorIndex, a.Contribution, a.SelectionProof[:])
}

func (a *ContributionAndProof) EncodingSizeSSZ() int {
	return 108 + a.Contribution.EncodingSizeSSZ()
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
	return ssz2.UnmarshalSSZ(buf, version, a.Message, a.Signature[:])
}

func (a *SignedContributionAndProof) EncodingSizeSSZ() int {
	return 100 + a.Message.EncodingSizeSSZ()
}

func (a *SignedContributionAndProof) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(a.Message, a.Signature[:])
}

/*
 * SyncContribution, Determines successfull committee, bits shows active participants,
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
