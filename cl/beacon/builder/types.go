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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
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
	if h.Data.Message.Value == "" {
		return nil
	}
	//blockValue := binary.LittleEndian.Uint64([]byte(h.Data.Message.Value))
	blockValue, ok := new(big.Int).SetString(h.Data.Message.Value, 10)
	if !ok {
		log.Warn("cannot parse block value", "value", h.Data.Message.Value)
	}
	return blockValue
}

type BlindedBlockResponse struct {
	Version string          `json:"version"`
	Data    json.RawMessage `json:"data"`
}
