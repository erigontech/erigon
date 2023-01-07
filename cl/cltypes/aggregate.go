package cltypes

import "github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"

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
	offset := 8
	buf := make([]byte, a.SizeSSZ())
	ssz_utils.MarshalUint64SSZ(buf, a.AggregatorIndex)

	marshalledAggregate, err := a.Aggregate.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	copy(buf[offset:], marshalledAggregate)
	offset += a.Aggregate.SizeSSZ()

	copy(buf[offset:], a.SelectionProof[:])

	return buf, nil
}

func (a *AggregateAndProof) UnmarshalSSZ(buf []byte) error {
	a.AggregatorIndex = ssz_utils.UnmarshalUint64SSZ(buf)

	if a.Aggregate == nil {
		a.Aggregate = new(Attestation)
	}
	if err := a.Aggregate.UnmarshalSSZ(buf[8:]); err != nil {
		return err
	}

	copy(a.SelectionProof[:], buf[8+a.Aggregate.SizeSSZ():])
	return nil
}

func (a *AggregateAndProof) SizeSSZ() int {
	return 104 + a.Aggregate.SizeSSZ()
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

	copy(buf, marshalledAggregate)
	copy(buf[a.Message.SizeSSZ():], a.Signature[:])

	return buf, nil
}

func (a *SignedAggregateAndProof) UnmarshalSSZ(buf []byte) error {

	if a.Message == nil {
		a.Message = new(AggregateAndProof)
	}
	if err := a.Message.UnmarshalSSZ(buf); err != nil {
		return err
	}

	copy(a.Signature[:], buf[a.Message.SizeSSZ():])
	return nil
}

func (a *SignedAggregateAndProof) SizeSSZ() int {
	return 96 + a.Message.SizeSSZ()
}
