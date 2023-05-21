package cltypes

import (
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

type ProposerSlashing struct {
	Header1 *SignedBeaconBlockHeader
	Header2 *SignedBeaconBlockHeader
}

func (p *ProposerSlashing) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.Encode(dst, p.Header1, p.Header2)
}

func (p *ProposerSlashing) DecodeSSZ(buf []byte, version int) error {
	p.Header1 = new(SignedBeaconBlockHeader)
	p.Header2 = new(SignedBeaconBlockHeader)
	if err := p.Header1.DecodeSSZ(buf, version); err != nil {
		return err
	}
	return p.Header2.DecodeSSZ(buf[p.Header1.EncodingSizeSSZ():], version)
}

func (p *ProposerSlashing) EncodingSizeSSZ() int {
	return p.Header1.EncodingSizeSSZ() * 2
}

func (p *ProposerSlashing) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(p.Header1, p.Header2)
}

type AttesterSlashing struct {
	Attestation_1 *IndexedAttestation
	Attestation_2 *IndexedAttestation
}

func (a *AttesterSlashing) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.Encode(dst, a.Attestation_1, a.Attestation_2)
}

func (a *AttesterSlashing) DecodeSSZ(buf []byte, version int) error {
	a.Attestation_1 = new(IndexedAttestation)
	a.Attestation_2 = new(IndexedAttestation)
	attestation2Offset := ssz.DecodeOffset(buf[4:])
	if err := a.Attestation_1.DecodeSSZ(buf[8:attestation2Offset], version); err != nil {
		return err
	}
	return a.Attestation_2.DecodeSSZ(buf[attestation2Offset:], version)
}

func (a *AttesterSlashing) EncodingSizeSSZ() int {
	return 8 + a.Attestation_1.EncodingSizeSSZ() + a.Attestation_2.EncodingSizeSSZ()
}

func (a *AttesterSlashing) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(a.Attestation_1, a.Attestation_2)
}
