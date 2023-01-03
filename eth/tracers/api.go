package tracers

import (
	"github.com/ledgerwatch/erigon/eth/tracers/logger"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
)

// TraceConfig holds extra parameters to trace functions.
type TraceConfig struct {
	*logger.LogConfig
	Tracer         *string
	Timeout        *string
	Reexec         *uint64
	NoRefunds      *bool // Turns off gas refunds when tracing
	StateOverrides *ethapi.StateOverrides
}
