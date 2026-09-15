package exec

import (
	"context"
	"time"

	"github.com/erigontech/erigon/common"
)

// ResultReporter is a task the worker hands its result to as the run finishes — also when the execution that
// scheduled it has already been cut and no longer reads its results.
type ResultReporter interface {
	TaskDone(result *TxResult)
}

// TxProgress is told each regular transaction's run time — the worker's measurement, TxResult.Duration — as its
// run finishes. It is how the caller of a cut round tells a transaction that itself ran long from a round that
// overran for some other reason: before the transaction ran, or after it had executed.
type TxProgress interface {
	TxExecuted(txHash common.Hash, took time.Duration)
}

type txProgressKey struct{}

// WithTxProgress attaches p to the context execution runs under.
func WithTxProgress(ctx context.Context, p TxProgress) context.Context {
	return context.WithValue(ctx, txProgressKey{}, p)
}

// TxProgressFrom returns the TxProgress attached by WithTxProgress, or nil.
func TxProgressFrom(ctx context.Context) TxProgress {
	p, _ := ctx.Value(txProgressKey{}).(TxProgress)
	return p
}
