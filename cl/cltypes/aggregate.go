package cltypes

import (
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz"
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
	buf = append(buf, ssz.Uint64SSZ(a.AggregatorIndex)...)
	buf = append(buf, ssz.OffsetSSZ(108)...)
	buf = append(buf, a.SelectionProof[:]...)
	buf, err = a.Aggregate.EncodeSSZ(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (a *AggregateAndProof) DecodeSSZ(buf []byte) error {
	a.AggregatorIndex = ssz.UnmarshalUint64SSZ(buf)
	if a.Aggregate == nil {
		a.Aggregate = new(Attestation)
	}

	copy(a.SelectionProof[:], buf[12:])
	if err := a.Aggregate.DecodeSSZ(buf[108:]); err != nil {
		return err
	}
	return nil
}

func (a *AggregateAndProof) EncodingSizeSSZ() int {
	return 108 + a.Aggregate.EncodingSizeSSZ()
}

type SignedAggregateAndProof struct {
	Message   *AggregateAndProof
	Signature [96]byte
}

func (a *SignedAggregateAndProof) EncodedSSZ(dst []byte) ([]byte, error) {
	buf := dst
	var err error
	buf = append(buf, ssz.OffsetSSZ(100)...)

	buf = append(buf, a.Signature[:]...)
	buf, err = a.Message.EncodeSSZ(buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (a *SignedAggregateAndProof) DecodeSSZ(buf []byte) error {
	if a.Message == nil {
		a.Message = new(AggregateAndProof)
	}

	copy(a.Signature[:], buf[4:])

	if err := a.Message.DecodeSSZ(buf[100:]); err != nil {
		return err
	}

	return nil
}
func (a *SignedAggregateAndProof) DecodeSSZWithVersion(buf []byte, _ int) error {
	return a.DecodeSSZ(buf)
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

func (agg *SyncAggregate) EncodeSSZ(buf []byte) []byte {
	return append(buf, append(agg.SyncCommiteeBits[:], agg.SyncCommiteeSignature[:]...)...)
}

func (agg *SyncAggregate) DecodeSSZ(buf []byte) error {
	copy(agg.SyncCommiteeBits[:], buf)
	copy(agg.SyncCommiteeSignature[:], buf[64:])
	return nil
}

func (agg *SyncAggregate) DecodeSSZWithVersion(buf []byte, _ int) error {
	return agg.DecodeSSZ(buf)
}

func (agg *SyncAggregate) EncodingSizeSSZ() int {
	return 160
}

func (agg *SyncAggregate) HashSSZ() ([32]byte, error) {
	var (
		leaves = make([][32]byte, 2)
		err    error
	)
	leaves[0] = utils.Keccak256(agg.SyncCommiteeBits[:32], agg.SyncCommiteeBits[32:])
	leaves[1], err = merkle_tree.SignatureRoot(agg.SyncCommiteeSignature)
	if err != nil {
		return [32]byte{}, err
	}

	return merkle_tree.ArraysRoot(leaves, 2)
}
