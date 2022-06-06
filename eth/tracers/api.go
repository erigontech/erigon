package tracers

import (
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/internal/ethapi"
)

// TraceConfig holds extra parameters to trace functions.
type TraceConfig struct {
	*vm.LogConfig
	Tracer         *string
	Timeout        *string
	Reexec         *uint64
	NoRefunds      *bool // Turns off gas refunds when tracing
	StateOverrides *ethapi.StateOverrides
}
