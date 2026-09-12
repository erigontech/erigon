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
		"logs":     benchLogs(3),
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

// A reused buffer must keep its capacity and not leak the previous response.
func TestMarshalIntoReusesBuffer(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	if err := marshalInto(&buf, benchLogs(8)); err != nil {
		t.Fatal(err)
	}
	big := buf.Cap()

	buf.Reset()
	if err := marshalInto(&buf, hexutil.Uint64(1)); err != nil {
		t.Fatal(err)
	}
	if want := `"0x1"`; buf.String() != want {
		t.Fatalf("want %s, got %s", want, buf.String())
	}
	if buf.Cap() != big {
		t.Errorf("capacity not reused: had %d, now %d", big, buf.Cap())
	}
}
