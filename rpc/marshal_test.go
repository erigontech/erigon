//go:build go1.27

package rpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// logsFixture mirrors an eth_getLogs result: many small structs, every field hex.
func logsFixture(n int) types.RPCLogs {
	logs := make(types.RPCLogs, n)
	for i := range logs {
		l := &types.RPCLog{BlockTimestamp: hexutil.Uint64(1_750_000_000)}
		l.Address = common.HexToAddress("0xdac17f958d2ee523a2206206994597c13d831ec7")
		l.Topics = []common.Hash{
			common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
			common.HexToHash("0x0000000000000000000000001f9840a85d5af5bf1d1762f925bdaddc4201f984"),
		}
		l.Data = make(hexutil.Bytes, 32)
		l.BlockNumber = hexutil.Uint64(21_000_000 + i)
		l.TxHash = common.HexToHash("0xb6449d8e167a8826d050afe4c9f07095236ff769a985f02649b1023c2ded2059")
		l.TxIndex = hexutil.Uint(i)
		l.BlockHash = common.HexToHash("0x6804117de2f3e6ee32953e78ced1db7b20214e0d8c745a03b8fecf7cc8ee76ef")
		l.Index = hexutil.Uint(i)
		logs[i] = l
	}
	return logs
}

// marshalInto must append exactly what json.Marshal produces, byte for byte.
func TestMarshalIntoMatchesMarshal(t *testing.T) {
	t.Parallel()

	for name, v := range map[string]any{
		"nil":      nil,
		"empty":    types.RPCLogs{},
		"logs":     logsFixture(3),
		"string":   "plain",
		"escaping": "<script>&\u2028",
		"number":   hexutil.Uint64(0x1f4),
		"bytes":    hexutil.Bytes{0xde, 0xad},
		"hash":     common.HexToHash("0x1234"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			want, err := json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
			for _, prefix := range []string{"", "keep-me"} {
				var buf bytes.Buffer
				buf.WriteString(prefix)
				if err := marshalInto(&buf, v); err != nil {
					t.Fatal(err)
				}
				if buf.String() != prefix+string(want) {
					t.Errorf("prefix %q:\n want %q\n got  %q", prefix, prefix+string(want), buf.String())
				}
			}
		})
	}
}

// boomAt marshals fine until element boom, then fails. Enough elements marshal
// first that the encoder has flushed well past its internal 4KB buffer.
type boomAt struct{ i, boom int }

func (b boomAt) MarshalJSON() ([]byte, error) {
	if b.i == b.boom {
		return nil, errors.New("boom")
	}
	return []byte(`"` + strings.Repeat("x", 64) + `"`), nil
}

// json/v2's MarshalWrite streams, so by the time a value deep in a slice fails
// it has already written the earlier elements. Those bytes must never reach the
// client: writeResponse has to return a JSON-RPC error, not a truncated result.
func TestResponseDiscardsPartialMarshal(t *testing.T) {
	t.Parallel()

	vals := make([]boomAt, 10000)
	for i := range vals {
		vals[i] = boomAt{i: i, boom: 9000}
	}

	s := jsonstream.Get(nil)
	defer jsonstream.Put(s)
	resp := (&jsonrpcMessage{Version: vsn, ID: json.RawMessage("1")}).writeResponse(s, vals)

	if resp == nil || resp.Error == nil {
		t.Fatal("want an error response, got a result")
	}
	if n := len(s.Buffer()); n != 0 {
		t.Errorf("partial marshal leaked %d bytes into the stream", n)
	}
}
