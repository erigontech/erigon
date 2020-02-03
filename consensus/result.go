package consensus

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/core/types"
)

type ResultWithContext struct {
	Cancel
	*types.Block
}

type Cancel struct {
	context.Context
	context.CancelFunc
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
