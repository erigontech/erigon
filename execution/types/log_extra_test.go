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

package types

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
)

func sampleLog() *Log {
	return &Log{
		Address: common.HexToAddress("0x01"),
		Topics:  []common.Hash{common.HexToHash("0xaa"), common.HexToHash("0xbb")},
		Data:    []byte{1, 2, 3},
	}
}

func TestLog_Copy(t *testing.T) {
	t.Parallel()
	require.Nil(t, (*Log)(nil).Copy())

	orig := sampleLog()
	cp := orig.Copy()
	require.Equal(t, orig, cp)

	// Mutating the original's slices must not affect the copy.
	orig.Topics[0] = common.HexToHash("0xff")
	orig.Data[0] = 9
	require.NotEqual(t, orig.Topics[0], cp.Topics[0])
	require.NotEqual(t, orig.Data[0], cp.Data[0])
}

func TestLogs_Copy(t *testing.T) {
	t.Parallel()
	require.Nil(t, Logs(nil).Copy())

	logs := Logs{sampleLog()}
	cp := logs.Copy()
	require.Len(t, cp, 1)
	require.Equal(t, logs[0], cp[0])

	logs[0].Data[0] = 9
	require.NotEqual(t, logs[0].Data[0], cp[0].Data[0])
}

func TestLogs_ToErigonLogs(t *testing.T) {
	t.Parallel()
	logs := Logs{sampleLog(), sampleLog()}
	el := logs.ToErigonLogs(42)
	require.Len(t, el, 2)
	for i := range el {
		require.Equal(t, uint64(42), uint64(el[i].Timestamp))
		require.Equal(t, logs[i].Address, el[i].Address)
	}
}

func TestToRPCTransactionLog(t *testing.T) {
	t.Parallel()
	log := sampleLog()
	header := &Header{Time: 99}
	rpc := ToRPCTransactionLog(log, header, common.Hash{}, 0)
	require.Equal(t, uint64(99), uint64(rpc.BlockTimestamp))
	require.Equal(t, log.Address, rpc.Address)
	require.Equal(t, log.Topics, rpc.Topics)
}

func TestLogs_ContainingTopics(t *testing.T) {
	t.Parallel()
	addrA := common.HexToAddress("0x0a")
	addrB := common.HexToAddress("0x0b")
	t1 := common.HexToHash("0x11")
	t2 := common.HexToHash("0x22")
	logs := Logs{
		{Address: addrA, Topics: []common.Hash{t1}},
		{Address: addrB, Topics: []common.Hash{t2}},
	}

	// Empty topic map matches any topic; no address filter -> all logs.
	require.Len(t, logs.ContainingTopics(nil, nil, 0), 2)

	// Address filter narrows to one log.
	require.Len(t, logs.ContainingTopics(map[common.Address]struct{}{addrA: {}}, nil, 0), 1)

	// Topic filter returns only logs holding a requested topic.
	got := logs.ContainingTopics(nil, map[common.Hash]struct{}{t2: {}}, 0)
	require.Len(t, got, 1)
	require.Equal(t, addrB, got[0].Address)

	// maxLogs bounds how many logs are scanned/returned.
	require.Len(t, logs.ContainingTopics(nil, nil, 1), 1)
}

func TestLog_RLPRoundTrip(t *testing.T) {
	t.Parallel()
	orig := sampleLog()

	var buf bytes.Buffer
	require.NoError(t, orig.EncodeRLP(&buf))

	var got Log
	require.NoError(t, got.DecodeRLP(rlp.NewStream(&buf, 0)))
	// Only the consensus fields survive the RLP form.
	require.Equal(t, orig.Address, got.Address)
	require.Equal(t, orig.Topics, got.Topics)
	require.Equal(t, []byte(orig.Data), []byte(got.Data))
}

func TestLog_DecodeRLP_Error(t *testing.T) {
	t.Parallel()
	var got Log
	require.Error(t, got.DecodeRLP(rlp.NewStream(bytes.NewReader(nil), 0)))
}

func TestLogForStorage_RLPRoundTrip(t *testing.T) {
	t.Parallel()
	for name, topics := range map[string][]common.Hash{
		"with topics": {common.HexToHash("0xaa"), common.HexToHash("0xbb")},
		"no topics":   {},
	} {
		topics := topics
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			orig := LogForStorage{
				Address: common.HexToAddress("0x01"),
				Topics:  topics,
				Data:    []byte{4, 5, 6},
			}

			var buf bytes.Buffer
			require.NoError(t, orig.EncodeRLP(&buf))

			var got LogForStorage
			require.NoError(t, got.DecodeRLP(rlp.NewStream(&buf, 0)))
			require.Equal(t, orig.Address, got.Address)
			require.Equal(t, orig.Topics, got.Topics)
			require.Equal(t, []byte(orig.Data), []byte(got.Data))
		})
	}
}

func TestLogForStorage_DecodeRLP_Error(t *testing.T) {
	t.Parallel()
	var got LogForStorage
	require.Error(t, got.DecodeRLP(rlp.NewStream(bytes.NewReader(nil), 0)))
}

const validLogJSON = `{"address":"0x0000000000000000000000000000000000000001",` +
	`"topics":[],"data":"0x010203",` +
	`"transactionHash":"0x0000000000000000000000000000000000000000000000000000000000000002"}`

func TestErigonLog_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	var el ErigonLog
	require.NoError(t, el.UnmarshalJSON([]byte(validLogJSON)))
	require.Equal(t, common.HexToAddress("0x01"), el.Address)

	var bad ErigonLog
	require.ErrorContains(t, bad.UnmarshalJSON([]byte(`{"topics":[],"data":"0x"}`)), "address")
	require.Error(t, bad.UnmarshalJSON([]byte("not-json")))
}

func TestRPCLog_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	var rl RPCLog
	require.NoError(t, rl.UnmarshalJSON([]byte(validLogJSON)))
	require.Equal(t, common.HexToAddress("0x01"), rl.Address)

	var bad RPCLog
	require.ErrorContains(t, bad.UnmarshalJSON([]byte(`{"topics":[],"data":"0x"}`)), "address")
	require.Error(t, bad.UnmarshalJSON([]byte("not-json")))
}
