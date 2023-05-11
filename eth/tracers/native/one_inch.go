package native

import (
	"encoding/json"

	"github.com/ledgerwatch/erigon/eth/calltracer"
	"github.com/ledgerwatch/erigon/eth/tracers"
)

func init() {
	register("1inch", newOInchCallTracer)
}

func newOInchCallTracer(ctx *tracers.Context, cfg json.RawMessage) (tracers.Tracer, error) {
	return calltracer.New1InchCallTracer(), nil
}
