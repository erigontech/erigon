package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
)

// benchLogs mirrors an eth_getLogs result: many small structs, every field hex.
func benchLogs(n int) types.RPCLogs {
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

func BenchmarkResultMarshal(b *testing.B) {
	for _, n := range []int{1, 64, 4096} {
		logs := benchLogs(n)
		out, err := json.Marshal(logs)
		if err != nil {
			b.Fatal(err)
		}
		size := len(out)

		b.Run(fmt.Sprintf("logs=%d/%dKB/marshal", n, size/1024), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := json.Marshal(logs); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("logs=%d/%dKB/write_fresh", n, size/1024), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var buf bytes.Buffer
				if err := marshalInto(&buf, logs); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("logs=%d/%dKB/write_reused", n, size/1024), func(b *testing.B) {
			b.ReportAllocs()
			var buf bytes.Buffer
			for b.Loop() {
				buf.Reset()
				if err := marshalInto(&buf, logs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
