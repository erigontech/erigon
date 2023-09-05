package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

/*
 * AggregateAndProof contains the index of the aggregator, the attestation
 * to be aggregated and the BLS signature of the attestation.
 */
type AggregateAndProof struct {
	AggregatorIndex uint64
	Aggregate       *solid.Attestation
	SelectionProof  libcommon.Bytes96
}

func (a *AggregateAndProof) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, a.AggregatorIndex, a.Aggregate, a.SelectionProof[:])
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
	Message   *AggregateAndProof
	Signature libcommon.Bytes96
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

/*
 * SyncAggregate, Determines successfull committee, bits shows active participants,
 * and signature is the aggregate BLS signature of the committee.
 */
type SyncAggregate struct {
	SyncCommiteeBits      [64]byte
	SyncCommiteeSignature [96]byte
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

func (agg *SyncAggregate) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, append(agg.SyncCommiteeBits[:], agg.SyncCommiteeSignature[:]...)...), nil
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
