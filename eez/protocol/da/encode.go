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

package da

import (
	"bytes"
	"encoding/binary"
	"slices"

	"github.com/erigontech/erigon/common"
)

// Encode lays msgs out as a callData stream: the version byte, then each
// message with shortest length prefixes. It does not check that the stream
// is valid.
func Encode(msgs ...Message) []byte {
	out := []byte{StreamVersion}
	for i := range msgs {
		out = appendMessage(out, &msgs[i])
	}
	return out
}

func appendMessage(out []byte, m *Message) []byte {
	out = append(out, byte(m.Type))
	appendData := func(out []byte) []byte {
		out = binary.AppendUvarint(out, uint64(len(m.Data)))
		return append(out, m.Data...)
	}
	switch m.Type {
	case RollupOperation, InitiateCrossRollupTransaction:
		out = binary.LittleEndian.AppendUint64(out, m.RollupID)
		out = appendData(out)
	case Call, StaticCall:
		out = binary.LittleEndian.AppendUint64(out, m.RollupID)
		out = append(out, m.FromAddress[:]...)
		out = append(out, m.ToAddress[:]...)
		if m.Type == Call {
			v := m.Value.Bytes32()
			slices.Reverse(v[:])
			out = append(out, v[:]...)
		}
		out = binary.LittleEndian.AppendUint64(out, m.Gas)
		out = appendData(out)
	case ReturnSuccess, ReturnFail:
		out = appendData(out)
	}
	return out
}

// Messages returns the stream ParsePayload reads p from: the
// RollupOperation, then one bracket per action with empty tx_data.
func (p *Payload) Messages() []Message {
	msgs := make([]Message, 0, 1+4*len(p.Actions))
	msgs = append(msgs, Message{Type: RollupOperation, RollupID: p.RollupID, Data: p.Operations})
	for i := range p.Actions {
		a := &p.Actions[i]
		ret := Message{Type: ReturnSuccess, Data: a.ReturnData}
		if !a.Success {
			ret.Type = ReturnFail
		}
		call := Message{
			Type:        Call,
			RollupID:    a.TargetRollupID,
			FromRollup:  a.SourceRollupID,
			FromAddress: a.SourceAddress,
			ToAddress:   a.TargetAddress,
			Value:       a.Value,
			Gas:         a.Gas,
			Data:        a.Data,
		}
		msgs = append(msgs,
			Message{Type: InitiateCrossRollupTransaction, RollupID: a.SourceRollupID, Data: []byte{}},
			call, ret,
			Message{Type: FinishCrossRollupTransaction},
		)
	}
	return msgs
}

// Encode returns 0x00 ‖ native_block_span_v0 of s with shortest varints and
// maximal runs, the one encoding of s that DecodeSpan admits.
func (s *Span) Encode() []byte {
	out := []byte{SpanVersionV0}
	out = binary.AppendUvarint(out, uint64(len(s.TxCounts)))
	for _, n := range s.TxCounts {
		out = binary.AppendUvarint(out, uint64(n))
	}
	out = appendRuns(out, s.Beneficiaries, func(a, b common.Address) bool { return a == b }, func(out []byte, a common.Address) []byte {
		return append(out, a[:]...)
	})
	out = appendRuns(out, s.ExtraData, bytes.Equal, func(out []byte, extra []byte) []byte {
		return append(append(out, byte(len(extra))), extra...)
	})
	for _, tx := range s.Transactions {
		out = binary.AppendUvarint(out, uint64(len(tx)))
	}
	for _, tx := range s.Transactions {
		out = append(out, tx...)
	}
	return out
}

func appendRuns[S ~[]T, T any](out []byte, column S, equal func(a, b T) bool, value func([]byte, T) []byte) []byte {
	for i := 0; i < len(column); {
		j := i + 1
		for j < len(column) && equal(column[j], column[i]) {
			j++
		}
		out = binary.AppendUvarint(out, uint64(j-i))
		out = value(out, column[i])
		i = j
	}
	return out
}
