package cltypes

import (
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

type ProposerSlashing struct {
	Header1 *SignedBeaconBlockHeader
	Header2 *SignedBeaconBlockHeader
}

func (p *ProposerSlashing) EncodeSSZ(dst []byte) (buf []byte, err error) {
	buf = dst
	if buf, err = p.Header1.EncodeSSZ(buf); err != nil {
		return
	}
	if buf, err = p.Header2.EncodeSSZ(buf); err != nil {
		return
	}
	return
}

func (p *ProposerSlashing) DecodeSSZ(buf []byte) error {
	p.Header1 = new(SignedBeaconBlockHeader)
	p.Header2 = new(SignedBeaconBlockHeader)
	if err := p.Header1.DecodeSSZ(buf); err != nil {
		return err
	}
	return p.Header2.DecodeSSZ(buf[p.Header1.EncodingSizeSSZ():])
}

func (p *ProposerSlashing) DecodeSSZWithVersion(buf []byte, _ int) error {
	return p.DecodeSSZ(buf)
}

func (p *ProposerSlashing) EncodingSizeSSZ() int {
	return p.Header1.EncodingSizeSSZ() * 2
}

func (p *ProposerSlashing) HashSSZ() ([32]byte, error) {
	root1, err := p.Header1.HashSSZ()
	if err != nil {
		return [32]byte{}, err
	}
	root2, err := p.Header2.HashSSZ()
	if err != nil {
		return [32]byte{}, err
	}
	return merkle_tree.ArraysRoot([][32]byte{root1, root2}, 2)
}

type AttesterSlashing struct {
	Attestation_1 *IndexedAttestation
	Attestation_2 *IndexedAttestation
}

func (a *AttesterSlashing) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	offset := 8
	// Write offsets
	buf = append(buf, ssz.OffsetSSZ(uint32(offset))...)
	offset += a.Attestation_1.EncodingSizeSSZ()
	buf = append(buf, ssz.OffsetSSZ(uint32(offset))...)
	// Write the attestations
	var err error
	buf, err = a.Attestation_1.EncodeSSZ(buf)
	if err != nil {
		return nil, err
	}
	buf, err = a.Attestation_2.EncodeSSZ(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (a *AttesterSlashing) DecodeSSZ(buf []byte) error {
	a.Attestation_1 = new(IndexedAttestation)
	a.Attestation_2 = new(IndexedAttestation)
	attestation2Offset := ssz.DecodeOffset(buf[4:])
	if err := a.Attestation_1.DecodeSSZ(buf[8:attestation2Offset]); err != nil {
		return err
	}
	return a.Attestation_2.DecodeSSZ(buf[attestation2Offset:])
}

func (a *AttesterSlashing) DecodeSSZWithVersion(buf []byte, _ int) error {
	return a.DecodeSSZ(buf)
}

func (a *AttesterSlashing) EncodingSizeSSZ() int {
	return 8 + a.Attestation_1.EncodingSizeSSZ() + a.Attestation_2.EncodingSizeSSZ()
}

func (a *AttesterSlashing) HashSSZ() ([32]byte, error) {
	root1, err := a.Attestation_1.HashSSZ()
	if err != nil {
		return [32]byte{}, err
	}
	root2, err := a.Attestation_2.HashSSZ()
	if err != nil {
		return [32]byte{}, err
	}
	return merkle_tree.ArraysRoot([][32]byte{root1, root2}, 2)
}
