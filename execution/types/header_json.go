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

package types

import (
	"encoding/json"
	"errors"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

// The hexutil types below are the JSON form of the plain ones Header declares; the ethjson
// tags on Header say which is which, and TestHeaderMarshalFastJSONTo holds the encoder to
// them.
// UnmarshalJSON unmarshals from JSON.
func (h *Header) UnmarshalJSON(input []byte) error {
	type Header struct {
		ParentHash            *common.Hash    `json:"parentHash"       gencodec:"required"`
		UncleHash             *common.Hash    `json:"sha3Uncles"       gencodec:"required"`
		Coinbase              *common.Address `json:"miner"`
		Root                  *common.Hash    `json:"stateRoot"        gencodec:"required"`
		TxHash                *common.Hash    `json:"transactionsRoot" gencodec:"required"`
		ReceiptHash           *common.Hash    `json:"receiptsRoot"     gencodec:"required"`
		Bloom                 *Bloom          `json:"logsBloom"        gencodec:"required"`
		Difficulty            *uint256.Int    `json:"difficulty"       gencodec:"required"`
		Number                *uint256.Int    `json:"number"           gencodec:"required"`
		GasLimit              *hexutil.Uint64 `json:"gasLimit"         gencodec:"required"`
		GasUsed               *hexutil.Uint64 `json:"gasUsed"          gencodec:"required"`
		Time                  *hexutil.Uint64 `json:"timestamp"        gencodec:"required"`
		Extra                 *hexutil.Bytes  `json:"extraData"        gencodec:"required"`
		MixDigest             *common.Hash    `json:"mixHash"`
		Nonce                 *BlockNonce     `json:"nonce"`
		AuRaStep              hexutil.Uint64  `json:"auraStep,omitempty"`
		AuRaSeal              hexutil.Bytes   `json:"auraSeal,omitempty"`
		BaseFee               *uint256.Int    `json:"baseFeePerGas"`
		WithdrawalsHash       *common.Hash    `json:"withdrawalsRoot"`
		BlobGasUsed           *hexutil.Uint64 `json:"blobGasUsed"`
		ExcessBlobGas         *hexutil.Uint64 `json:"excessBlobGas"`
		ParentBeaconBlockRoot *common.Hash    `json:"parentBeaconBlockRoot"`
		RequestsHash          *common.Hash    `json:"requestsHash"`
		BlockAccessListHash   *common.Hash    `json:"blockAccessListHash"`
		SlotNumber            *hexutil.Uint64 `json:"slotNumber"`
	}
	var dec Header
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	if dec.ParentHash == nil {
		return errors.New("missing required field 'parentHash' for Header")
	}
	h.ParentHash = *dec.ParentHash
	if dec.UncleHash == nil {
		return errors.New("missing required field 'sha3Uncles' for Header")
	}
	h.UncleHash = *dec.UncleHash
	if dec.Coinbase != nil {
		h.Coinbase = *dec.Coinbase
	}
	if dec.Root == nil {
		return errors.New("missing required field 'stateRoot' for Header")
	}
	h.Root = *dec.Root
	if dec.TxHash == nil {
		return errors.New("missing required field 'transactionsRoot' for Header")
	}
	h.TxHash = *dec.TxHash
	if dec.ReceiptHash == nil {
		return errors.New("missing required field 'receiptsRoot' for Header")
	}
	h.ReceiptHash = *dec.ReceiptHash
	if dec.Bloom == nil {
		return errors.New("missing required field 'logsBloom' for Header")
	}
	h.Bloom = *dec.Bloom
	if dec.Difficulty == nil {
		return errors.New("missing required field 'difficulty' for Header")
	}
	h.Difficulty = *dec.Difficulty
	if dec.Number == nil {
		return errors.New("missing required field 'number' for Header")
	}
	h.Number = *dec.Number
	if dec.GasLimit == nil {
		return errors.New("missing required field 'gasLimit' for Header")
	}
	h.GasLimit = uint64(*dec.GasLimit)
	if dec.GasUsed == nil {
		return errors.New("missing required field 'gasUsed' for Header")
	}
	h.GasUsed = uint64(*dec.GasUsed)
	if dec.Time == nil {
		return errors.New("missing required field 'timestamp' for Header")
	}
	h.Time = uint64(*dec.Time)
	if dec.Extra == nil {
		return errors.New("missing required field 'extraData' for Header")
	}
	h.Extra = *dec.Extra
	if dec.MixDigest != nil {
		h.MixDigest = *dec.MixDigest
	}
	if dec.Nonce != nil {
		h.Nonce = *dec.Nonce
	}

	h.AuRaStep = uint64(dec.AuRaStep)

	if dec.AuRaSeal != nil {
		h.AuRaSeal = dec.AuRaSeal
	}
	if dec.BaseFee != nil {
		h.BaseFee = dec.BaseFee
	}
	if dec.WithdrawalsHash != nil {
		h.WithdrawalsHash = dec.WithdrawalsHash
	}
	if dec.BlobGasUsed != nil {
		h.BlobGasUsed = (*uint64)(dec.BlobGasUsed)
	}
	if dec.ExcessBlobGas != nil {
		h.ExcessBlobGas = (*uint64)(dec.ExcessBlobGas)
	}
	if dec.ParentBeaconBlockRoot != nil {
		h.ParentBeaconBlockRoot = dec.ParentBeaconBlockRoot
	}
	if dec.RequestsHash != nil {
		h.RequestsHash = dec.RequestsHash
	}
	if dec.BlockAccessListHash != nil {
		h.BlockAccessListHash = dec.BlockAccessListHash
	}
	if dec.SlotNumber != nil {
		h.SlotNumber = (*uint64)(dec.SlotNumber)
	}
	return nil
}
