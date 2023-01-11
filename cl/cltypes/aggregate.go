package cltypes

import (
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
)

/*
 * AggregateAndProof contains the index of the aggregator, the attestation
 * to be aggregated and the BLS signature of the attestation.
 */
type AggregateAndProof struct {
	AggregatorIndex uint64
	Aggregate       *Attestation
	SelectionProof  [96]byte
}

func (a *AggregateAndProof) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst

	var err error
	buf = append(buf, ssz_utils.Uint64SSZ(a.AggregatorIndex)...)
	buf = append(buf, ssz_utils.OffsetSSZ(108)...)
	buf = append(buf, a.SelectionProof[:]...)
	buf, err = a.Aggregate.EncodeSSZ(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (a *AggregateAndProof) UnmarshalSSZ(buf []byte) error {
	a.AggregatorIndex = ssz_utils.UnmarshalUint64SSZ(buf)
	if a.Aggregate == nil {
		a.Aggregate = new(Attestation)
	}

	copy(a.SelectionProof[:], buf[12:])
	if err := a.Aggregate.DecodeSSZ(buf[108:]); err != nil {
		return err
	}
	return nil
}

func (a *AggregateAndProof) SizeSSZ() int {
	return 108 + a.Aggregate.EncodingSizeSSZ()
}

type SignedAggregateAndProof struct {
	Message   *AggregateAndProof
	Signature [96]byte
}

func (a *SignedAggregateAndProof) EncodedSSZ(dst []byte) ([]byte, error) {
	buf := dst
	var err error
	buf = append(buf, ssz_utils.OffsetSSZ(100)...)

	buf = append(buf, a.Signature[:]...)
	buf, err = a.Message.EncodeSSZ(buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (a *SignedAggregateAndProof) UnmarshalSSZ(buf []byte) error {
	if a.Message == nil {
		a.Message = new(AggregateAndProof)
	}

	copy(a.Signature[:], buf[4:])

	if err := a.Message.UnmarshalSSZ(buf[100:]); err != nil {
		return err
	}

	return nil
}

func (a *SignedAggregateAndProof) SizeSSZ() int {
	return 100 + a.Message.SizeSSZ()
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

func (agg *SyncAggregate) MarshalSSZ() ([]byte, error) {
	return append(agg.SyncCommiteeBits[:], agg.SyncCommiteeSignature[:]...), nil
}

func (agg *SyncAggregate) UnmarshalSSZ(buf []byte) error {
	copy(agg.SyncCommiteeBits[:], buf)
	copy(agg.SyncCommiteeSignature[:], buf[64:])
	return nil
}

func (agg *SyncAggregate) SizeSSZ() int {
	return 160
}

func (agg *SyncAggregate) HashTreeRoot() ([32]byte, error) {
	var (
		leaves = make([][32]byte, 2)
		err    error
	)
	leaves[0] = utils.Keccak256(agg.SyncCommiteeBits[:])
	leaves[1], err = merkle_tree.SignatureRoot(agg.SyncCommiteeSignature)
	if err != nil {
		return [32]byte{}, err
	}

	return merkle_tree.ArraysRoot(leaves, 2)
}
