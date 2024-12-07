// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package cltypes

import (
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

type ProposerSlashing struct {
	Header1 *SignedBeaconBlockHeader `json:"signed_header_1"`
	Header2 *SignedBeaconBlockHeader `json:"signed_header_2"`
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
	Attestation_1 *IndexedAttestation `json:"attestation_1"`
	Attestation_2 *IndexedAttestation `json:"attestation_2"`
}

func NewAttesterSlashing(version clparams.StateVersion) *AttesterSlashing {
	return &AttesterSlashing{
		Attestation_1: NewIndexedAttestation(version),
		Attestation_2: NewIndexedAttestation(version),
	}
}

func (a *AttesterSlashing) SetVersion(v clparams.StateVersion) {
	a.Attestation_1.SetVersion(v)
	a.Attestation_2.SetVersion(v)
}

func (a *AttesterSlashing) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, a.Attestation_1, a.Attestation_2)
}

func (a *AttesterSlashing) DecodeSSZ(buf []byte, version int) error {
	a.Attestation_1 = NewIndexedAttestation(clparams.StateVersion(version))
	a.Attestation_2 = NewIndexedAttestation(clparams.StateVersion(version))
	return ssz2.UnmarshalSSZ(buf, version, a.Attestation_1, a.Attestation_2)
}

func (a *AttesterSlashing) EncodingSizeSSZ() int {
	return 8 + a.Attestation_1.EncodingSizeSSZ() + a.Attestation_2.EncodingSizeSSZ()
}

func (a *AttesterSlashing) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(a.Attestation_1, a.Attestation_2)
}
