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

package utils

import (
	"context"
	"errors"
)

type TaskQueue struct {
	name  string
	queue chan func(context.Context) error

	logFuncProvider func(err error) func(msg string, ctx ...interface{})
}

func NewTaskQueue(
	name string,
	capacity uint,
	logFuncProvider func(err error) func(msg string, ctx ...interface{}),
) *TaskQueue {
	queue := make(chan func(context.Context) error, capacity)

	instance := TaskQueue{
		name,
		queue,
		logFuncProvider,
	}
	return &instance
}

func (queue *TaskQueue) Run(ctx context.Context) {
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			break
		case op := <-queue.queue:
			err := op(ctx)
			if (err != nil) && !errors.Is(err, context.Canceled) {
				logFunc := queue.logFuncProvider(err)
				logFunc("Task failed", "queue", queue.name, "err", err)
			}
		}
	}
}

func (queue *TaskQueue) EnqueueTask(ctx context.Context, op func(context.Context) error) {
	select {
	case <-ctx.Done():
		break
	case queue.queue <- op:
	}
}
