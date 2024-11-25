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
	"fmt"

	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/core/types"
)

// ETH1Header represents the ethereum 1 header structure CL-side.
type Eth1Header struct {
	ParentHash    libcommon.Hash    `json:"parent_hash"`
	FeeRecipient  libcommon.Address `json:"fee_recipient"`
	StateRoot     libcommon.Hash    `json:"state_root"`
	ReceiptsRoot  libcommon.Hash    `json:"receipts_root"`
	LogsBloom     types.Bloom       `json:"logs_bloom"`
	PrevRandao    libcommon.Hash    `json:"prev_randao"`
	BlockNumber   uint64            `json:"block_number,string"`
	GasLimit      uint64            `json:"gas_limit,string"`
	GasUsed       uint64            `json:"gas_used,string"`
	Time          uint64            `json:"time,string"`
	Extra         *solid.ExtraData  `json:"extra_data"`
	BaseFeePerGas libcommon.Hash    `json:"base_fee_per_gas"`
	// Extra fields
	BlockHash        libcommon.Hash `json:"block_hash"`
	TransactionsRoot libcommon.Hash `json:"transactions_root"`
	WithdrawalsRoot  libcommon.Hash `json:"withdrawals_root"`
	BlobGasUsed      uint64         `json:"blob_gas_used,string"`
	ExcessBlobGas    uint64         `json:"excess_blob_gas,string"`
	// internals
	version clparams.StateVersion
}

// NewEth1Header creates new header with given version.
func NewEth1Header(version clparams.StateVersion) *Eth1Header {
	return &Eth1Header{
		version: version,
		Extra:   solid.NewExtraData(),
	}
}

func (e *Eth1Header) SetVersion(v clparams.StateVersion) {
	e.version = v
}

func (e *Eth1Header) Copy() *Eth1Header {
	copied := *e
	copied.Extra = solid.NewExtraData()
	copied.Extra.SetBytes(e.Extra.Bytes())
	return &copied
}

// Capella converts the header to capella version.
func (e *Eth1Header) Capella() {
	e.version = clparams.CapellaVersion
	e.WithdrawalsRoot = libcommon.Hash{}
}

// Deneb converts the header to deneb version.
func (e *Eth1Header) Deneb() {
	e.version = clparams.DenebVersion
	e.BlobGasUsed = 0
	e.ExcessBlobGas = 0
}

func (e *Eth1Header) IsZero() bool {
	if e.Extra == nil {
		e.Extra = solid.NewExtraData()
	}
	return e.ParentHash == libcommon.Hash{} && e.FeeRecipient == libcommon.Address{} && e.StateRoot == libcommon.Hash{} &&
		e.ReceiptsRoot == libcommon.Hash{} && e.LogsBloom == types.Bloom{} && e.PrevRandao == libcommon.Hash{} && e.BlockNumber == 0 &&
		e.GasLimit == 0 && e.GasUsed == 0 && e.Time == 0 && e.Extra.EncodingSizeSSZ() == 0 && e.BaseFeePerGas == [32]byte{} &&
		e.BlockHash == libcommon.Hash{} && e.TransactionsRoot == libcommon.Hash{} && e.WithdrawalsRoot == libcommon.Hash{} &&
		e.BlobGasUsed == 0 && e.ExcessBlobGas == 0
}

// EncodeSSZ encodes the header in SSZ format.
func (h *Eth1Header) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, h.getSchema()...)
}

// DecodeSSZ decodes given SSZ slice.
func (h *Eth1Header) DecodeSSZ(buf []byte, version int) error {
	h.version = clparams.StateVersion(version)
	if len(buf) < h.EncodingSizeSSZ() {
		return fmt.Errorf("[Eth1Header] err: %s", ssz.ErrLowBufferSize)
	}
	return ssz2.UnmarshalSSZ(buf, version, h.getSchema()...)
}

// EncodingSizeSSZ returns the ssz encoded size in bytes for the Header object
func (h *Eth1Header) EncodingSizeSSZ() int {
	size := 536

	if h.version >= clparams.CapellaVersion {
		size += 32
	}

	if h.version >= clparams.DenebVersion {
		size += 8 * 2 // BlobGasUsed + ExcessBlobGas
	}
	if h.Extra == nil {
		h.Extra = solid.NewExtraData()
	}

	return size + h.Extra.EncodingSizeSSZ()
}

// HashSSZ encodes the header in SSZ tree format.
func (h *Eth1Header) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(h.getSchema()...)
}

func (h *Eth1Header) getSchema() []interface{} {
	s := []interface{}{
		h.ParentHash[:], h.FeeRecipient[:], h.StateRoot[:], h.ReceiptsRoot[:], h.LogsBloom[:],
		h.PrevRandao[:], &h.BlockNumber, &h.GasLimit, &h.GasUsed, &h.Time, h.Extra, h.BaseFeePerGas[:], h.BlockHash[:], h.TransactionsRoot[:],
	}
	if h.version >= clparams.CapellaVersion {
		s = append(s, h.WithdrawalsRoot[:])
	}
	if h.version >= clparams.DenebVersion {
		s = append(s, &h.BlobGasUsed, &h.ExcessBlobGas)
	}
	return s
}

func (h *Eth1Header) Static() bool {
	return false
}

func (h *Eth1Header) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ParentHash       libcommon.Hash    `json:"parent_hash"`
		FeeRecipient     libcommon.Address `json:"fee_recipient"`
		StateRoot        libcommon.Hash    `json:"state_root"`
		ReceiptsRoot     libcommon.Hash    `json:"receipts_root"`
		LogsBloom        types.Bloom       `json:"logs_bloom"`
		PrevRandao       libcommon.Hash    `json:"prev_randao"`
		BlockNumber      uint64            `json:"block_number,string"`
		GasLimit         uint64            `json:"gas_limit,string"`
		GasUsed          uint64            `json:"gas_used,string"`
		Time             uint64            `json:"timestamp,string"`
		Extra            *solid.ExtraData  `json:"extra_data"`
		BaseFeePerGas    string            `json:"base_fee_per_gas"`
		BlockHash        libcommon.Hash    `json:"block_hash"`
		TransactionsRoot libcommon.Hash    `json:"transactions_root"`
		WithdrawalsRoot  libcommon.Hash    `json:"withdrawals_root"`
		BlobGasUsed      uint64            `json:"blob_gas_used,string"`
		ExcessBlobGas    uint64            `json:"excess_blob_gas,string"`
	}{
		ParentHash:       h.ParentHash,
		FeeRecipient:     h.FeeRecipient,
		StateRoot:        h.StateRoot,
		ReceiptsRoot:     h.ReceiptsRoot,
		LogsBloom:        h.LogsBloom,
		PrevRandao:       h.PrevRandao,
		BlockNumber:      h.BlockNumber,
		GasLimit:         h.GasLimit,
		GasUsed:          h.GasUsed,
		Time:             h.Time,
		Extra:            h.Extra,
		BaseFeePerGas:    uint256.NewInt(0).SetBytes32(utils.ReverseOfByteSlice(h.BaseFeePerGas[:])).Dec(),
		BlockHash:        h.BlockHash,
		TransactionsRoot: h.TransactionsRoot,
		WithdrawalsRoot:  h.WithdrawalsRoot,
		BlobGasUsed:      h.BlobGasUsed,
		ExcessBlobGas:    h.ExcessBlobGas,
	})
}

func (h *Eth1Header) UnmarshalJSON(data []byte) error {
	var aux struct {
		ParentHash       libcommon.Hash    `json:"parent_hash"`
		FeeRecipient     libcommon.Address `json:"fee_recipient"`
		StateRoot        libcommon.Hash    `json:"state_root"`
		ReceiptsRoot     libcommon.Hash    `json:"receipts_root"`
		LogsBloom        hexutility.Bytes  `json:"logs_bloom"`
		PrevRandao       libcommon.Hash    `json:"prev_randao"`
		BlockNumber      uint64            `json:"block_number,string"`
		GasLimit         uint64            `json:"gas_limit,string"`
		GasUsed          uint64            `json:"gas_used,string"`
		Time             uint64            `json:"timestamp,string"`
		Extra            hexutility.Bytes  `json:"extra_data"`
		BaseFeePerGas    string            `json:"base_fee_per_gas"`
		BlockHash        libcommon.Hash    `json:"block_hash"`
		TransactionsRoot libcommon.Hash    `json:"transactions_root"`
		WithdrawalsRoot  libcommon.Hash    `json:"withdrawals_root"`
		BlobGasUsed      uint64            `json:"blob_gas_used,string"`
		ExcessBlobGas    uint64            `json:"excess_blob_gas,string"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	extra := solid.NewExtraData()
	extra.SetBytes(aux.Extra)
	h.ParentHash = aux.ParentHash
	h.FeeRecipient = aux.FeeRecipient
	h.StateRoot = aux.StateRoot
	h.ReceiptsRoot = aux.ReceiptsRoot
	h.LogsBloom = types.BytesToBloom(aux.LogsBloom)
	h.PrevRandao = aux.PrevRandao
	h.BlockNumber = aux.BlockNumber
	h.GasLimit = aux.GasLimit
	h.GasUsed = aux.GasUsed
	h.Time = aux.Time
	h.Extra = extra
	tmp := uint256.NewInt(0)
	if err := tmp.SetFromDecimal(aux.BaseFeePerGas); err != nil {
		return err
	}
	tmpBaseFee := tmp.Bytes32()
	copy(h.BaseFeePerGas[:], utils.ReverseOfByteSlice(tmpBaseFee[:]))
	h.BlockHash = aux.BlockHash
	h.TransactionsRoot = aux.TransactionsRoot
	h.WithdrawalsRoot = aux.WithdrawalsRoot
	h.BlobGasUsed = aux.BlobGasUsed
	h.ExcessBlobGas = aux.ExcessBlobGas
	return nil
}
