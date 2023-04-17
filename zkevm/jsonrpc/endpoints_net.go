package jsonrpc

import (
	"strconv"

	"github.com/ledgerwatch/erigon/zkevm/encoding"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/types"
)

// NetEndpoints contains implementations for the "net" RPC endpoints
type NetEndpoints struct {
	cfg Config
}

// Version returns the current network id
func (n *NetEndpoints) Version() (interface{}, types.Error) {
	return strconv.FormatUint(n.cfg.ChainID, encoding.Base10), nil
}
