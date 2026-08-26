// Copyright 2026 The Erigon Authors
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
	"github.com/erigontech/erigon/cl/cltypes/solid"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

type GloasBlockContents struct {
	Block                    *BeaconBlock              `json:"block"`
	ExecutionPayloadEnvelope *ExecutionPayloadEnvelope `json:"execution_payload_envelope"`
	KZGProofs                *solid.ListSSZ[*KZGProof] `json:"kzg_proofs"`
	Blobs                    *solid.ListSSZ[*Blob]     `json:"blobs"`
}

func NewGloasBlockContents(cfg *clparams.BeaconChainConfig, slot uint64) *GloasBlockContents {
	maxBlobs := int(cfg.GetBlobParameters(slot / cfg.SlotsPerEpoch).MaxBlobsPerBlock)
	return &GloasBlockContents{
		Block:                    NewBeaconBlock(cfg, clparams.GloasVersion),
		ExecutionPayloadEnvelope: NewExecutionPayloadEnvelope(cfg),
		KZGProofs:                solid.NewStaticListSSZ[*KZGProof](maxBlobs*int(cfg.NumberOfColumns), BYTES_KZG_PROOF),
		Blobs:                    solid.NewStaticListSSZ[*Blob](maxBlobs, int(BYTES_PER_BLOB)),
	}
}

func (b *GloasBlockContents) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.Block, b.ExecutionPayloadEnvelope, b.KZGProofs, b.Blobs)
}

func (b *GloasBlockContents) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, b.Block, b.ExecutionPayloadEnvelope, b.KZGProofs, b.Blobs)
}

func (b *GloasBlockContents) EncodingSizeSSZ() int {
	return 4*4 + b.Block.EncodingSizeSSZ() + b.ExecutionPayloadEnvelope.EncodingSizeSSZ() + b.KZGProofs.EncodingSizeSSZ() + b.Blobs.EncodingSizeSSZ()
}

func (b *GloasBlockContents) Static() bool { return false }

type SignedExecutionPayloadEnvelopeContents struct {
	SignedExecutionPayloadEnvelope *SignedExecutionPayloadEnvelope `json:"signed_execution_payload_envelope"`
	KZGProofs                      *solid.ListSSZ[*KZGProof]       `json:"kzg_proofs"`
	Blobs                          *solid.ListSSZ[*Blob]           `json:"blobs"`
}

func NewSignedExecutionPayloadEnvelopeContents(cfg *clparams.BeaconChainConfig, slot uint64) *SignedExecutionPayloadEnvelopeContents {
	maxBlobs := int(cfg.GetBlobParameters(slot / cfg.SlotsPerEpoch).MaxBlobsPerBlock)
	return &SignedExecutionPayloadEnvelopeContents{
		SignedExecutionPayloadEnvelope: &SignedExecutionPayloadEnvelope{
			Message:   NewExecutionPayloadEnvelope(cfg),
			beaconCfg: cfg,
		},
		KZGProofs: solid.NewStaticListSSZ[*KZGProof](maxBlobs*int(cfg.NumberOfColumns), BYTES_KZG_PROOF),
		Blobs:     solid.NewStaticListSSZ[*Blob](maxBlobs, int(BYTES_PER_BLOB)),
	}
}

func (c *SignedExecutionPayloadEnvelopeContents) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, c.SignedExecutionPayloadEnvelope, c.KZGProofs, c.Blobs)
}

func (c *SignedExecutionPayloadEnvelopeContents) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, c.SignedExecutionPayloadEnvelope, c.KZGProofs, c.Blobs)
}

func (c *SignedExecutionPayloadEnvelopeContents) DecodeSSZStrict(buf []byte, version int) error {
	return ssz2.UnmarshalSSZStrict(buf, version, c.SignedExecutionPayloadEnvelope, c.KZGProofs, c.Blobs)
}

func (c *SignedExecutionPayloadEnvelopeContents) EncodingSizeSSZ() int {
	return 3*4 + c.SignedExecutionPayloadEnvelope.EncodingSizeSSZ() + c.KZGProofs.EncodingSizeSSZ() + c.Blobs.EncodingSizeSSZ()
}

func (c *SignedExecutionPayloadEnvelopeContents) Static() bool { return false }
