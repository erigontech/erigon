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

package ethapi

import (
	"encoding/json"
	"errors"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/jsonstream/ethjson"
)

// RPCFrames uses the API's gas field names while types.Frame keeps the fixture encoding.
type RPCFrames []types.Frame

func (frames RPCFrames) MarshalJSON() ([]byte, error) {
	return jsonstream.Marshal(frames)
}

func (frames RPCFrames) MarshalFastJSONTo(s *jsonstream.Stream) error {
	jsonstream.ArrayValue(s, frames, func(s *jsonstream.Stream, frame *types.Frame) {
		s.WriteObjectStart()
		ethjson.Quantity(s, "mode", uint64(frame.Mode))
		ethjson.Quantity(s, "flags", uint64(frame.Flags))
		jsonstream.Text(s, "target", frame.Target)
		ethjson.Quantity(s, "executionGas", frame.Limits.Execution)
		ethjson.Quantity(s, "stateGas", frame.Limits.State)
		ethjson.Quantity256(s, "value", &frame.Value)
		ethjson.Data(s, "data", frame.Data)
		s.WriteObjectEnd()
	})
	return nil
}

func (frames *RPCFrames) UnmarshalJSON(input []byte) error {
	var dec []struct {
		Mode         hexutil.Uint64  `json:"mode"`
		Flags        hexutil.Uint64  `json:"flags"`
		Target       *common.Address `json:"target"`
		ExecutionGas hexutil.Uint64  `json:"executionGas"`
		StateGas     hexutil.Uint64  `json:"stateGas"`
		Value        hexutil.U256    `json:"value"`
		Data         hexutil.Bytes   `json:"data"`
	}
	if err := json.Unmarshal(input, &dec); err != nil {
		return err
	}
	var result RPCFrames
	if dec != nil {
		result = make(RPCFrames, len(dec))
	}
	for i, frame := range dec {
		if frame.Mode > types.FrameSender || frame.Flags >= 8 {
			return errors.New("invalid frame mode or flags")
		}
		result[i] = types.Frame{
			Mode: uint8(frame.Mode), Flags: uint8(frame.Flags), Target: frame.Target,
			Limits: mdgas.MdGas{Execution: uint64(frame.ExecutionGas), State: uint64(frame.StateGas)},
			Value:  uint256.Int(frame.Value), Data: frame.Data,
		}
	}
	*frames = result
	return nil
}
