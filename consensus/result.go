package consensus

import (
	"context"

	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

type ResultWithContext struct {
	Cancel
	*types.Block
}

type Cancel struct {
	context.Context
	cancel context.CancelFunc
}

func (c *Cancel) CancelFunc() {
	log.Trace("Cancel mining task", "callers", debug.Callers(10))
	c.cancel()
}

func NewCancel(ctxs ...context.Context) Cancel {
	var ctx context.Context
	if len(ctxs) > 0 {
		ctx = ctxs[0]
	} else {
		ctx = context.Background()
	}
	ctx, cancelFn := context.WithCancel(ctx)
	return Cancel{ctx, cancelFn}
}

func StabCancel() Cancel {
	return Cancel{
		context.Background(),
		func() {},
	}
}
