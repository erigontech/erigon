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
