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
	"encoding/json"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
)

type BlindOrExecutionBeaconBlock struct {
	Slot          uint64         `json:"slot,string"`
	ProposerIndex uint64         `json:"proposer_index,string"`
	ParentRoot    libcommon.Hash `json:"parent_root"`
	StateRoot     libcommon.Hash `json:"state_root"`
	// BeaconBody or BlindedBeaconBody with json tag "body"
	BeaconBody        *BeaconBody        `json:"-"`
	BlindedBeaconBody *BlindedBeaconBody `json:"-"`
	ExecutionValue    *big.Int           `json:"-"`
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

func (b *BlindOrExecutionBeaconBlock) ToExecution() *BeaconBlock {
	return &BeaconBlock{
		Slot:          b.Slot,
		ProposerIndex: b.ProposerIndex,
		ParentRoot:    b.ParentRoot,
		StateRoot:     b.StateRoot,
		Body:          b.BeaconBody,
	}
}

func (b *BlindOrExecutionBeaconBlock) MarshalJSON() ([]byte, error) {
	// if b.BeaconBody != nil, then marshal BeaconBody
	// if b.BlindedBeaconBody != nil, then marshal BlindedBeaconBody
	temp := struct {
		Slot          uint64         `json:"slot,string"`
		ProposerIndex uint64         `json:"proposer_index,string"`
		ParentRoot    libcommon.Hash `json:"parent_root"`
		StateRoot     libcommon.Hash `json:"state_root"`
		Body          any            `json:"body"`
	}{
		Slot:          b.Slot,
		ProposerIndex: b.ProposerIndex,
		ParentRoot:    b.ParentRoot,
		StateRoot:     b.StateRoot,
	}
	if b.BeaconBody != nil {
		temp.Body = b.BeaconBody
	} else if b.BlindedBeaconBody != nil {
		temp.Body = b.BlindedBeaconBody
	}
	return json.Marshal(temp)
}

func (b *BlindOrExecutionBeaconBlock) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, b); err != nil {
		return err
	}
	return nil
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
