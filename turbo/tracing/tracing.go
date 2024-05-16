package tracing

import (
	"github.com/ledgerwatch/erigon/eth/tracers"
	_ "github.com/ledgerwatch/erigon/eth/tracers/live"

	"github.com/urfave/cli/v2"
)

// SetupTracerCtx performs the tracing setup according to the parameters
// containted in the given urfave context.
func SetupTracerCtx(ctx *cli.Context) (*tracers.Tracer, error) {
	tracerName := ctx.String(VMTraceFlag.Name)
	if tracerName == "" {
		return nil, nil
	}

	cfg := ctx.String(VMTraceJsonConfigFlag.Name)

	return tracers.New(tracerName, &tracers.Context{}, []byte(cfg))
}
