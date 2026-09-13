package rpc

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
)

// Only one marshalInto compiles per toolchain, so both are pinned to the same
// oracle: whatever json.Marshal produces, byte for byte, appended to the buffer.
func TestMarshalIntoMatchesMarshal(t *testing.T) {
	t.Parallel()

	for name, v := range map[string]any{
		"nil":      nil,
		"empty":    types.RPCLogs{},
		"logs":     sampleLogs(3),
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

// sampleLogs mirrors an eth_getLogs result: small structs, every field hex, which
// is what exercises the TextAppender path both implementations rely on.
func sampleLogs(n int) types.RPCLogs {
	logs := make(types.RPCLogs, n)
	for i := range logs {
		l := &types.RPCLog{BlockTimestamp: hexutil.Uint64(1_750_000_000)}
		l.Address = common.HexToAddress("0xdac17f958d2ee523a2206206994597c13d831ec7")
		l.Topics = []common.Hash{common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")}
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
