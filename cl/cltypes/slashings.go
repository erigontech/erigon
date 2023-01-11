package cltypes

import "github.com/ledgerwatch/erigon/cl/merkle_tree"

type ProposerSlashing struct {
	Header1 *SignedBeaconBlockHeader
	Header2 *SignedBeaconBlockHeader
}

func (p *ProposerSlashing) EncodeSSZ(dst []byte) []byte {
	buf := dst
	buf = append(buf, p.Header1.EncodeSSZ(buf)...)
	buf = append(buf, p.Header2.EncodeSSZ(buf)...)
	return buf
}

func (p *ProposerSlashing) DecodeSSZ(buf []byte) error {
	p.Header1 = new(SignedBeaconBlockHeader)
	p.Header2 = new(SignedBeaconBlockHeader)
	if err := p.Header1.DecodeSSZ(buf); err != nil {
		return err
	}
	return p.Header2.DecodeSSZ(buf[p.Header1.EncodingSizeSSZ():])
}

func (p *ProposerSlashing) EncodingSizeSSZ() int {
	return p.Header1.EncodingSizeSSZ() * 2
}

func (p *ProposerSlashing) HashSSZ() ([32]byte, error) {
	root1, err := p.Header1.HashTreeRoot()
	if err != nil {
		return [32]byte{}, err
	}
	root2, err := p.Header2.HashTreeRoot()
	if err != nil {
		return [32]byte{}, err
	}
	return merkle_tree.ArraysRoot([][32]byte{root1, root2}, 2)
}

type AttesterSlashing struct {
	Attestation_1 *IndexedAttestation
	Attestation_2 *IndexedAttestation
}

func (a *AttesterSlashing) EncodeSSZ(dst []byte) []byte {
	buf := dst
	buf = a.Attestation_1.Data.EncodeSSZ(buf)
	buf = a.Attestation_2.Data.EncodeSSZ(buf)
	return buf
}

func (a *AttesterSlashing) DecodeSSZ(buf []byte) error {
	a.Attestation_1 = new(IndexedAttestation)
	a.Attestation_2 = new(IndexedAttestation)
	if err := a.Attestation_1.DecodeSSZ(buf); err != nil {
		return err
	}
	return a.Attestation_2.DecodeSSZ(buf[a.Attestation_1.EncodingSizeSSZ():])
}

func (a *AttesterSlashing) EncodingSizeSSZ() int {
	return a.Attestation_1.EncodingSizeSSZ() + a.Attestation_2.EncodingSizeSSZ()
}

func (a *AttesterSlashing) HashSSZ() ([32]byte, error) {
	root1, err := a.Attestation_1.HashTreeRoot()
	if err != nil {
		return [32]byte{}, err
	}
	root2, err := a.Attestation_2.HashTreeRoot()
	if err != nil {
		return [32]byte{}, err
	}
	return merkle_tree.ArraysRoot([][32]byte{root1, root2}, 2)
}
