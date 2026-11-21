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

package jsonrpc

import (
	"bytes"
	"encoding/json"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// LogsResult is the eth_getLogs result. It encodes as the bare array of Logs, written from
// encoded when the logs were encoded ahead: encoded[i] is the JSON object of Logs[i].
type LogsResult struct {
	Logs    types.Logs
	encoded [][]byte
}

func (r LogsResult) MarshalFastJSONTo(s *jsonstream.Stream) error {
	if r.encoded == nil {
		return r.Logs.MarshalFastJSONTo(s)
	}
	s.WriteArrayStart()
	for _, e := range r.encoded {
		s.WriteRawBytes(e)
	}
	s.WriteArrayEnd()
	return nil
}

func (r LogsResult) MarshalJSON() ([]byte, error) {
	if r.encoded == nil {
		return json.Marshal(r.Logs)
	}
	return append(append([]byte{'['}, bytes.Join(r.encoded, []byte{','})...), ']'), nil
}

// headBlockLogs holds all the logs of one block with the JSON object of each, so that any filter
// on the block is answered without reading receipts or encoding again.
type headBlockLogs struct {
	hash    common.Hash
	logs    types.Logs
	encoded [][]byte
}

func newHeadBlockLogs(hash common.Hash, logs types.Logs) (*headBlockLogs, error) {
	encoded := make([][]byte, len(logs))
	for i, l := range logs {
		e, err := jsonstream.Marshal(types.Logs{l})
		if err != nil {
			return nil, err
		}
		encoded[i] = e[1 : len(e)-1]
	}
	return &headBlockLogs{hash: hash, logs: logs, encoded: encoded}, nil
}

// filter returns the logs matching f, in block order, with their JSON.
func (h *headBlockLogs) filter(f *types.LogFilter) LogsResult {
	matched := h.logs.FilterWith(f, 0)
	res := LogsResult{Logs: matched, encoded: make([][]byte, 0, len(matched))}
	for i, j := 0, 0; j < len(matched); i++ {
		if h.logs[i] == matched[j] {
			res.encoded = append(res.encoded, h.encoded[i])
			j++
		}
	}
	return res
}
