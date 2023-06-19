package cltypes

import (
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

type ProposerSlashing struct {
	Header1 *SignedBeaconBlockHeader
	Header2 *SignedBeaconBlockHeader
}

func (p *ProposerSlashing) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, p.Header1, p.Header2)
}

func (p *ProposerSlashing) DecodeSSZ(buf []byte, version int) error {
	p.Header1 = new(SignedBeaconBlockHeader)
	p.Header2 = new(SignedBeaconBlockHeader)
	return ssz2.UnmarshalSSZ(buf, version, p.Header1, p.Header2)
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
	return ssz2.MarshalSSZ(dst, a.Attestation_1, a.Attestation_2)
}

func (a *AttesterSlashing) DecodeSSZ(buf []byte, version int) error {
	a.Attestation_1 = new(IndexedAttestation)
	a.Attestation_2 = new(IndexedAttestation)
	return ssz2.UnmarshalSSZ(buf, version, a.Attestation_1, a.Attestation_2)
}

func (a *AttesterSlashing) EncodingSizeSSZ() int {
	return 8 + a.Attestation_1.EncodingSizeSSZ() + a.Attestation_2.EncodingSizeSSZ()
}

func (a *AttesterSlashing) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(a.Attestation_1, a.Attestation_2)
}
