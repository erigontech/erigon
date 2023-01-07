package cltypes

import (
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
)

/*
 * AggregateAndProof contains the index of the aggregator, the attestation
 * to be aggregated and the BLS signature of the attestation.
 */
type AggregateAndProof struct {
	AggregatorIndex uint64
	Aggregate       *Attestation
	SelectionProof  [96]byte `ssz-size:"96"`
}

func (a *AggregateAndProof) MarshalSSZ() ([]byte, error) {
	buf := make([]byte, a.SizeSSZ())

	ssz_utils.MarshalUint64SSZ(buf, a.AggregatorIndex)
	ssz_utils.EncodeOffset(buf[8:], 108)

	marshalledAggregate, err := a.Aggregate.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	copy(buf[12:], a.SelectionProof[:])
	copy(buf[108:], marshalledAggregate)
	return buf, nil
}

func (a *AggregateAndProof) UnmarshalSSZ(buf []byte) error {
	a.AggregatorIndex = ssz_utils.UnmarshalUint64SSZ(buf)
	if a.Aggregate == nil {
		a.Aggregate = new(Attestation)
	}

	copy(a.SelectionProof[:], buf[12:])
	if err := a.Aggregate.UnmarshalSSZ(buf[108:]); err != nil {
		return err
	}
	return nil
}

func (a *AggregateAndProof) SizeSSZ() int {
	return 108 + a.Aggregate.SizeSSZ()
}

type SignedAggregateAndProof struct {
	Message   *AggregateAndProof
	Signature [96]byte `ssz-size:"96"`
}

func (a *SignedAggregateAndProof) MarshalSSZ() ([]byte, error) {
	buf := make([]byte, a.SizeSSZ())

	marshalledAggregate, err := a.Message.MarshalSSZ()
	if err != nil {
		return nil, err
	}

	ssz_utils.EncodeOffset(buf, 100)
	copy(buf[4:], a.Signature[:])
	copy(buf[100:], marshalledAggregate)

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
