package app

import (
	"context"
)

type ctxkey int

const (
	ckLogger ctxkey = iota
)

func WithLogger(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, ckLogger, logger)
}

func CtxLogger(ctx context.Context) Logger {
	if logger, ok := ctx.Value(ckLogger).(Logger); ok {
		return logger
	}

	return applog
}
