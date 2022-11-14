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
