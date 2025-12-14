package state

import (
	"context"
)

type ctxKey int

const (
	ckMetrics ctxKey = iota
)

func WithMetrics(ctx context.Context, m *DomainMetrics) context.Context {
	return context.WithValue(ctx, ckMetrics, m)
}

func MetricsValue(ctx context.Context) *DomainMetrics {
	return ctx.Value(ckMetrics).(*DomainMetrics)
}
