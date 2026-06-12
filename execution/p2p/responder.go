// Copyright 2026 The Erigon Authors
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

package p2p

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
)

// responderTaskQueueSize bounds each responder's pending request queue; serving
// requests is best-effort, so requests arriving faster than we serve them are dropped.
const responderTaskQueueSize = 256

func enqueueResponderTask[T any](logger log.Logger, logPrefix string, tasks chan T, task T) {
	select {
	case tasks <- task:
	default:
		logger.Warn(logPrefix + " task queue is full, dropping request")
	}
}

func processResponderTasks[T any](ctx context.Context, tasks chan T, handle func(ctx context.Context, task T) error, logger log.Logger, logPrefix string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-tasks:
			err := handleResponderTask(ctx, task, handle)
			if err != nil {
				logger.Debug(logPrefix+" could not handle request", "err", err)
			}
		}
	}
}

// handleResponderTask recovers panics so that a malformed request cannot crash
// the node — serving peers is best-effort.
func handleResponderTask[T any](ctx context.Context, task T, handle func(ctx context.Context, task T) error) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}()
	return handle(ctx, task)
}
