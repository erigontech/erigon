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

package builder

import (
	"encoding/json"
	"math/big"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

type ExecutionHeader struct {
	Version string              `json:"version"`
	Data    ExecutionHeaderData `json:"data"`
}

type ExecutionHeaderData struct {
	Message   ExecutionHeaderMessage `json:"message"`
	Signature common.Bytes96         `json:"signature"`
}

type ExecutionHeaderMessage struct {
	Header             *cltypes.Eth1Header                    `json:"header"`
	BlobKzgCommitments *solid.ListSSZ[*cltypes.KZGCommitment] `json:"blob_kzg_commitments"`
	ExecutionRequests  *cltypes.ExecutionRequests             `json:"execution_requests,omitempty"`
	Value              string                                 `json:"value"`
	PubKey             common.Bytes48                         `json:"pubkey"`
}

func (h ExecutionHeader) BlockValue() *big.Int {
	value := h.Data.Message.Value
	if value == "" {
		return nil
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			log.Warn("cannot parse block value", "value", value)
			return nil
		}
	}
	blockValue, ok := new(big.Int).SetString(value, 10)
	if !ok {
		log.Warn("cannot parse block value", "value", value)
		return nil
	}
	if blockValue.Sign() < 0 || blockValue.BitLen() > 256 {
		log.Warn("builder block value outside uint256 range", "value", value)
		return nil
	}
	return blockValue
}

type BlindedBlockResponse struct {
	Version string          `json:"version"`
	Data    json.RawMessage `json:"data"`
}
