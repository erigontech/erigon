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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/protocol"
)

// encode is a test encoder: the version byte, then msgs as BLOB_FORMAT_SPEC
// lays them out, with shortest length prefixes.
func encode(msgs ...Message) []byte {
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

// packBlobs lays stream bytes out in blobs the way BlobData reads them.
func packBlobs(stream []byte) [][]byte {
	blobs := make([][]byte, (len(stream)+BlobDataSize-1)/BlobDataSize)
	for i := range blobs {
		blob := make([]byte, BlobSize)
		for k, b := range stream[i*BlobDataSize : min(len(stream), (i+1)*BlobDataSize)] {
			blob[32*(k/31)+31-k%31] = b
		}
		blobs[i] = blob
	}
	return blobs
}

var (
	addrA = common.HexToAddress("0xaa")
	addrB = common.HexToAddress("0xbb")
)

// sample follows the context-stack example of BLOB_FORMAT_SPEC §1.2 with a
// Snapshot region, a failed call and a static call added.
func sample() []Message {
	value := uint256.NewInt(0)
	value.SetBytes(common.FromHex("0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"))
	return []Message{
		{Type: RollupOperation, RollupID: 7, Data: []byte{0xaa}},
		{Type: InitiateCrossRollupTransaction, RollupID: 1, Data: []byte{}},
		{Type: Call, RollupID: 2, FromRollup: 1, FromAddress: addrA, ToAddress: addrB, Value: *value, Gas: 21000, Data: []byte{1, 2}},
		{Type: Call, RollupID: 3, FromRollup: 2, FromAddress: addrB, ToAddress: addrA, Gas: 1, Data: bytes.Repeat([]byte{3}, 200)},
		{Type: ReturnSuccess, Data: []byte{4}},
		{Type: Snapshot},
		{Type: Call, RollupID: 4, FromRollup: 2, Data: []byte{}},
		{Type: ReturnFail, Data: []byte{5, 6}},
		{Type: Revert},
		{Type: StaticCall, RollupID: 5, FromRollup: 2, FromAddress: addrA, Gas: 7, Data: []byte{8}},
		{Type: ReturnSuccess, Data: []byte{}},
		{Type: ReturnSuccess, Data: []byte{9}},
		{Type: FinishCrossRollupTransaction},
		{Type: RollupOperation, RollupID: 7, Data: []byte{0xbb}},
	}
}

func TestDecodeCallDataStream(t *testing.T) {
	msgs, err := Decode(nil, encode(sample()...))
	require.NoError(t, err)
	require.Equal(t, sample(), msgs)
}

func TestDecodeBlobStream(t *testing.T) {
	stream := append(encode(sample()...), byte(CloseBlobStream))
	msgs, err := Decode(packBlobs(stream), nil)
	require.NoError(t, err)
	require.Equal(t, sample(), msgs)

	// A message may span blobs; callData continues the stream after the close.
	big := Message{Type: RollupOperation, RollupID: 7, Data: bytes.Repeat([]byte{0xcc}, BlobDataSize)}
	stream = append(encode(big), byte(CloseBlobStream))
	tail := encode(sample()[1:13]...)[1:]
	msgs, err = Decode(packBlobs(stream), tail)
	require.NoError(t, err)
	require.Equal(t, append([]Message{big}, sample()[1:13]...), msgs)
}

// TestDecodeWindow84CallData is T-VEC-4 (stream part) on the devnet batch:
// its callData is a whole stream with one RollupOperation for rollup 1.
func TestDecodeWindow84CallData(t *testing.T) {
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(t, err)
	b, err := protocol.DecodePostAndVerifyBatch(w.PostBatch)
	require.NoError(t, err)
	msgs, err := Decode(nil, b.CallData)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, encode(msgs...), []byte(b.CallData))
	p, err := ParsePayload(msgs)
	require.NoError(t, err)
	require.Equal(t, uint64(1), p.RollupID)
	require.Empty(t, p.Actions)
}

// TestDecodeChiadoBlob decodes the live settlement's blob: the stream sits in
// the blob with CloseBlobStream after one RollupOperation for rollup 1 and
// empty callData. Reading the field elements in any other byte order fails.
func TestDecodeChiadoBlob(t *testing.T) {
	blob, err := testfixtures.LoadChiadoBlob()
	require.NoError(t, err)
	msgs, err := Decode([][]byte{blob}, nil)
	require.NoError(t, err)
	p, err := ParsePayload(msgs)
	require.NoError(t, err)
	require.Equal(t, uint64(1), p.RollupID)
	require.Equal(t, common.FromHex("0x009601ff960196013ed3157239afddf5eed010ea18e86b6d1db9f5e8960100"), p.Operations)
	require.Empty(t, p.Actions)

	data, err := BlobData(blob)
	require.NoError(t, err)
	require.Equal(t, append(encode(msgs...), byte(CloseBlobStream)), data[:43])
	require.Equal(t, make([]byte, BlobDataSize-43), data[43:])
}

func TestDecodeRejects(t *testing.T) {
	op := Message{Type: RollupOperation, RollupID: 1, Data: []byte{1}}
	initiate := Message{Type: InitiateCrossRollupTransaction, RollupID: 1}
	call := Message{Type: Call, RollupID: 2}
	ret := Message{Type: ReturnSuccess}
	finish := Message{Type: FinishCrossRollupTransaction}
	snapshot, revert := Message{Type: Snapshot}, Message{Type: Revert}
	withType := func(t byte) []byte { return []byte{StreamVersion, t} }
	closed := func(stream []byte) [][]byte { return packBlobs(append(stream, byte(CloseBlobStream))) }

	badElement := closed(encode(op))
	badElement[0][32*5] = 1
	// Version, type, rollup id and a three-byte length prefix take 13 bytes.
	full := encode(Message{Type: RollupOperation, Data: make([]byte, BlobDataSize-13)})
	require.Len(t, full, BlobDataSize)
	fullBlob := packBlobs(full)

	for _, c := range []struct {
		name     string
		blobs    [][]byte
		callData []byte
		err      error
	}{
		{"empty", nil, nil, ErrTruncated},
		{"version", nil, []byte{0x01}, ErrVersion},
		{"blob version", packBlobs([]byte{0x01, byte(CloseBlobStream)}), nil, ErrVersion},
		{"reserved type", nil, withType(0), ErrMessageType},
		{"type 11", nil, withType(11), ErrMessageType},
		{"type 255", nil, withType(255), ErrMessageType},
		{"truncated rollup id", nil, encode(op)[:6], ErrTruncated},
		{"truncated data", nil, encode(op)[:len(encode(op))-1], ErrTruncated},
		{"six-byte length", nil, append(encode(op)[:10], 0x80, 0x80, 0x80, 0x80, 0x80, 0x00), ErrLength},
		{"length above uint32", nil, append(encode(op)[:10], 0x80, 0x80, 0x80, 0x80, 0x10), ErrLength},
		{"return without call", nil, encode(initiate, ret), ErrBracket},
		{"finish with open call", nil, encode(initiate, call, finish), ErrBracket},
		{"nested transaction", nil, encode(initiate, initiate), ErrBracket},
		{"revert without snapshot", nil, encode(initiate, revert), ErrBracket},
		{"revert with open call", nil, encode(initiate, snapshot, call, revert), ErrBracket},
		{"return leaves snapshot frame", nil, encode(initiate, call, snapshot, ret), ErrBracket},
		{"finish with open snapshot", nil, encode(initiate, snapshot, finish), ErrBracket},
		{"unclosed transaction", nil, encode(initiate), ErrBracket},
		{"call outside transaction", nil, encode(call), ErrPlacement},
		{"snapshot outside transaction", nil, encode(snapshot), ErrPlacement},
		{"operation inside transaction", nil, encode(initiate, op), ErrPlacement},
		{"close without blobs", nil, append(encode(op), byte(CloseBlobStream)), ErrBlobClose},
		{"blobs without close", packBlobs(encode(op)), nil, ErrMessageType},
		{"full blob without close", fullBlob, nil, ErrBlobClose},
		{"close in callData tail", closed(encode(op)), []byte{byte(CloseBlobStream)}, ErrBlobClose},
		{"field element above 248 bits", badElement, nil, ErrBlobEncoding},
		{"short blob", [][]byte{make([]byte, BlobSize-1)}, nil, ErrBlobEncoding},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := Decode(c.blobs, c.callData)
			require.ErrorIs(t, err, c.err)
		})
	}
}

func TestDecodeAcceptsNonMinimalLength(t *testing.T) {
	stream := encode(Message{Type: RollupOperation, RollupID: 1, Data: []byte{0xab}})
	require.Equal(t, byte(1), stream[10])
	padded := slices.Concat(stream[:10], []byte{0x81, 0x80, 0x80, 0x80, 0x00}, stream[11:])
	msgs, err := Decode(nil, padded)
	require.NoError(t, err)
	require.Equal(t, []byte{0xab}, msgs[0].Data)
}

func TestParsePayload(t *testing.T) {
	value := *uint256.NewInt(42)
	in := Message{Type: Call, RollupID: 1, FromRollup: 0, FromAddress: addrA, ToAddress: addrB, Value: value, Gas: 5, Data: []byte{1}}
	out := Message{Type: Call, RollupID: 0, FromRollup: 1, FromAddress: addrB, ToAddress: addrA, Data: []byte{2}}
	stream := encode(
		Message{Type: RollupOperation, RollupID: 1, Data: []byte{0}},
		Message{Type: InitiateCrossRollupTransaction, RollupID: 0, Data: []byte{0xff}},
		in, Message{Type: ReturnSuccess, Data: []byte{3}}, Message{Type: FinishCrossRollupTransaction},
		Message{Type: InitiateCrossRollupTransaction, RollupID: 1},
		out, Message{Type: ReturnFail, Data: []byte{4}}, Message{Type: FinishCrossRollupTransaction},
	)
	msgs, err := Decode(nil, stream)
	require.NoError(t, err)
	p, err := ParsePayload(msgs)
	require.NoError(t, err)
	require.Equal(t, &Payload{RollupID: 1, Operations: []byte{0}, Actions: []Action{
		{SourceRollupID: 0, TargetRollupID: 1, SourceAddress: addrA, TargetAddress: addrB, Value: value, Gas: 5, Data: []byte{1}, Success: true, ReturnData: []byte{3}},
		{SourceRollupID: 1, TargetRollupID: 0, SourceAddress: addrB, TargetAddress: addrA, Data: []byte{2}, ReturnData: []byte{4}},
	}}, p)
}

func TestParsePayloadRejects(t *testing.T) {
	op := Message{Type: RollupOperation, RollupID: 1}
	initiate := Message{Type: InitiateCrossRollupTransaction}
	call := Message{Type: Call, RollupID: 1}
	static := Message{Type: StaticCall, RollupID: 1}
	ret, finish := Message{Type: ReturnSuccess}, Message{Type: FinishCrossRollupTransaction}
	snapshot, revert := Message{Type: Snapshot}, Message{Type: Revert}
	for name, msgs := range map[string][]Message{
		"empty":                {},
		"bracket first":        {initiate, call, ret, finish},
		"second operation":     {op, op},
		"operation after":      {op, initiate, call, ret, finish, op},
		"static call":          {op, initiate, static, ret, finish},
		"snapshot in bracket":  {op, initiate, snapshot, call, ret, revert, finish},
		"nested call":          {op, initiate, call, call, ret, ret, finish},
		"two calls in bracket": {op, initiate, call, ret, call, ret, finish},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParsePayload(msgs)
			require.ErrorIs(t, err, ErrProfile)
		})
	}
}

func FuzzDecodeCallDataStream(f *testing.F) {
	f.Add(encode(sample()...))
	f.Add(encode(Message{Type: RollupOperation, RollupID: 1, Data: []byte{0}}))
	f.Fuzz(func(t *testing.T, stream []byte) {
		msgs, err := Decode(nil, stream)
		if err != nil {
			return
		}
		again, err := Decode(nil, encode(msgs...))
		require.NoError(t, err)
		require.Equal(t, msgs, again)
	})
}
