package sync

import (
	"context"

	"github.com/ledgerwatch/log/v3"
)

type ctxKey int

const (
	ckLogger ctxKey = iota
	ckTempDir
)

func WithLogger(ctx context.Context, logger log.Logger) context.Context {
	return context.WithValue(ctx, ckLogger, logger)
}

func Logger(ctx context.Context) log.Logger {
	if logger, ok := ctx.Value(ckLogger).(log.Logger); ok {
		return logger
	}

	return log.Root()
}

func WithTempDir(ctx context.Context, tempDir string) context.Context {
	return context.WithValue(ctx, ckTempDir, tempDir)
}

func TempDir(ctx context.Context) string {
	if tempDir, ok := ctx.Value(ckTempDir).(string); ok {
		return tempDir
	}

	return ""
}
