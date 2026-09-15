package exec

import (
	"context"

	"github.com/erigontech/erigon/common"
)

type txExecutedKey struct{}

// WithTxExecuted attaches a callback execution calls each time a regular transaction's own execution finishes, with
// the transaction's hash. It is how the caller of a cut round tells a transaction that itself ran long from a round
// that overran for some other reason — a stall in worker shutdown, a commitment fold, the host — after its
// transactions had already executed.
func WithTxExecuted(ctx context.Context, done func(common.Hash)) context.Context {
	return context.WithValue(ctx, txExecutedKey{}, done)
}

// TxExecuted returns the callback attached by WithTxExecuted, or nil.
func TxExecuted(ctx context.Context) func(common.Hash) {
	done, _ := ctx.Value(txExecutedKey{}).(func(common.Hash))
	return done
}
