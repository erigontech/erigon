package rpc

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
)

// Only one marshalAppend compiles per toolchain, so both are pinned to the same
// oracle: whatever json.Marshal produces, byte for byte, appended to dst.
func TestMarshalAppendMatchesMarshal(t *testing.T) {
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
				got, err := marshalAppend([]byte(prefix), v)
				if err != nil {
					t.Fatal(err)
				}
				if string(got) != prefix+string(want) {
					t.Errorf("prefix %q:\n want %q\n got  %q", prefix, prefix+string(want), got)
				}
			}
		})
	}
}

// A reused buffer must not leak the previous response into the next one.
func TestMarshalAppendReusesBuffer(t *testing.T) {
	t.Parallel()

	buf, err := marshalAppend(nil, benchLogs(8))
	if err != nil {
		t.Fatal(err)
	}
	big := cap(buf)

	buf, err = marshalAppend(buf[:0], hexutil.Uint64(1))
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte(`"0x1"`); !bytes.Equal(buf, want) {
		t.Fatalf("want %s, got %s", want, buf)
	}
	if cap(buf) != big {
		t.Errorf("capacity not reused: had %d, now %d", big, cap(buf))
	}
}
