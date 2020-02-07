package consensus

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/core/types"
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
	fmt.Println("### CANCEL", debug.Callers(100))
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
