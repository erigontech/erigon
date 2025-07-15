// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package sync

import (
	"context"

	"github.com/erigontech/erigon/erigon-lib/log/v3"
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
