package stagedsync

import (
	"context"
	"os"

	"github.com/erigontech/erigon/db/kv"
)

var l1Fold = os.Getenv("L1_FOLD") == "1"

// preRead runs only on a fold worker, which owns its tx and so the probe state of that tx's domains.
func (r *asOfStateReader) preRead(d kv.Domain, plainKey []byte) {
	if !l1Fold || r.workerCtx == nil {
		return
	}
	if p, ok := r.roTx.AggTx().(interface {
		PreReadLatest(context.Context, kv.Domain, []byte)
	}); ok {
		p.PreReadLatest(r.workerCtx, d, plainKey)
	}
}
