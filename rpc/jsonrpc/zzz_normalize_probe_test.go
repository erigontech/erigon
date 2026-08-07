package jsonrpc

import (
	"testing"

	"github.com/erigontech/erigon/execution/state"
)

func TestZZZNormalizeProbeDump(t *testing.T) {
	state.NormalizeProbeDump("rpc/jsonrpc")
}
