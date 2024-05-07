package rpccfg

import (
	"time"
)

// HTTPTimeouts represents the configuration params for the HTTP RPC server.
type HTTPTimeouts struct {
	// ReadTimeout is the maximum duration for reading the entire
	// request, including the body.
	//
	// Because ReadTimeout does not let Handlers make per-request
	// decisions on each request body's acceptable deadline or
	// upload rate, most users will prefer to use
	// ReadHeaderTimeout. It is valid to use them both.
	ReadTimeout time.Duration

	// WriteTimeout is the maximum duration before timing out
	// writes of the response. It is reset whenever a new
	// request's header is read. Like ReadTimeout, it does not
	// let Handlers make decisions on a per-request basis.
	WriteTimeout time.Duration

	// IdleTimeout is the maximum amount of time to wait for the
	// next request when keep-alives are enabled. If IdleTimeout
	// is zero, the value of ReadTimeout is used. If both are
	// zero, ReadHeaderTimeout is used.
	IdleTimeout time.Duration
}

// DefaultHTTPTimeouts represents the default timeout values used if further
// configuration is not provided.
var DefaultHTTPTimeouts = HTTPTimeouts{
	ReadTimeout:  30 * time.Second,
	WriteTimeout: 30 * time.Minute,
	IdleTimeout:  120 * time.Second,
}

const DefaultEvmCallTimeout = 5 * time.Minute
const DefaultOverlayGetLogsTimeout = 5 * time.Minute
const DefaultOverlayReplayBlockTimeout = 10 * time.Second

var SlowLogBlackList = []string{
	"eth_getBlock", "eth_getBlockByNumber", "eth_getBlockByHash", "eth_blockNumber",
	"erigon_blockNumber", "erigon_getHeaderByNumber", "erigon_getHeaderByHash", "erigon_getBlockByTimestamp",
	"eth_call",
}
