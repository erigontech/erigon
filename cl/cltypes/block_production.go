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
	"errors"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
)

// BlindOrExecutionBeaconBlock is a union type that can be either a BlindedBeaconBlock or a BeaconBlock, depending on the context.
// It's an intermediate type used in the block production process.
type BlindOrExecutionBeaconBlock struct {
	Slot          uint64      `json:"-"`
	ProposerIndex uint64      `json:"-"`
	ParentRoot    common.Hash `json:"-"`
	StateRoot     common.Hash `json:"-"`
	// Full body
	BeaconBody *BeaconBody      `json:"-"`
	KzgProofs  []common.Bytes48 `json:"-"`
	Blobs      []*Blob          `json:"-"`
	// Blinded body
	BlindedBeaconBody *BlindedBeaconBody `json:"-"`

	ExecutionValue *big.Int `json:"-"`
	Cfg            *clparams.BeaconChainConfig
}

func (b *BlindOrExecutionBeaconBlock) ToGeneric() GenericBeaconBlock {
	if b.BlindedBeaconBody != nil {
		return b.ToBlinded()
	}
	return b.ToExecution()
}

func (b *BlindOrExecutionBeaconBlock) ToBlinded() *BlindedBeaconBlock {
	return &BlindedBeaconBlock{
		Slot:          b.Slot,
		ProposerIndex: b.ProposerIndex,
		ParentRoot:    b.ParentRoot,
		StateRoot:     b.StateRoot,
		Body:          b.BlindedBeaconBody,
	}
}

func (b *BlindOrExecutionBeaconBlock) ToExecution() *DenebBeaconBlock {
	beaconBlock := &BeaconBlock{
		Slot:          b.Slot,
		ProposerIndex: b.ProposerIndex,
		ParentRoot:    b.ParentRoot,
		StateRoot:     b.StateRoot,
		Body:          b.BeaconBody,
	}
	DenebBeaconBlock := NewDenebBeaconBlock(b.Cfg, b.Version(), b.Slot)
	DenebBeaconBlock.Block = beaconBlock
	for _, kzgProof := range b.KzgProofs {
		proof := KZGProof{}
		copy(proof[:], kzgProof[:])
		DenebBeaconBlock.KZGProofs.Append(&proof)
	}
	for _, blob := range b.Blobs {
		DenebBeaconBlock.Blobs.Append(blob)
	}

	return DenebBeaconBlock
}

func (b *BlindOrExecutionBeaconBlock) MarshalJSON() ([]byte, error) {
	return []byte{}, errors.New("json marshal unsupported for BlindOrExecutionBeaconBlock")
}

func (b *BlindOrExecutionBeaconBlock) UnmarshalJSON(data []byte) error {
	return errors.New("json unmarshal unsupported for BlindOrExecutionBeaconBlock")
}

func (b *BlindOrExecutionBeaconBlock) IsBlinded() bool {
	return b.BlindedBeaconBody != nil
}

func (b *BlindOrExecutionBeaconBlock) GetExecutionValue() *big.Int {
	if b.ExecutionValue == nil {
		return big.NewInt(0)
	}
	return b.ExecutionValue
}

func (b *BlindOrExecutionBeaconBlock) Version() clparams.StateVersion {
	if b.BeaconBody != nil {
		return b.BeaconBody.Version
	}
	if b.BlindedBeaconBody != nil {
		return b.BlindedBeaconBody.Version
	}
	return clparams.Phase0Version
}
