package utils

import (
	"context"
)

// Job - holds logic to perform some operations during queue execution.
type Job struct {
	Action func() error // A function that should be executed when the job is running.
}

// Run performs job execution.
func (j Job) Run() error {
	err := j.Action()
	if err != nil {
		return err
	}

	return nil
}

// Queue holds name, list of jobs and context with cancel.
type Queue struct {
	jobs   chan Job
	ctx    context.Context
	cancel context.CancelFunc
}

// NewQueue instantiates new queue.
func NewQueue(size int) *Queue {
	ctx, cancel := context.WithCancel(context.Background())

	return &Queue{
		jobs:   make(chan Job, size),
		ctx:    ctx,
		cancel: cancel,
	}
}

// AddJob sends job to the channel.
func (q *Queue) AddJob(job Job) {
	q.jobs <- job
}

func (q *Queue) Stop() {
	q.cancel()
}

// Worker responsible for queue serving.
type Worker struct {
	name    string
	errChan chan error
	queue   *Queue
}

// NewWorker initializes a new Worker.
func NewWorker(name string, errChan chan error, queue *Queue) *Worker {
	return &Worker{
		name,
		errChan,
		queue,
	}
}

// DoWork processes jobs from the queue (jobs channel).
func (w *Worker) DoWork() bool {
	finish := false
	for {
		select {
		case <-w.queue.ctx.Done():
			finish = true
		default:
			finish = false
		}

		select {
		// if job received.
		case job := <-w.queue.jobs:
			err := job.Run()
			if err != nil {
				w.errChan <- err
				return false
				// if context was canceled.
			}
		default:
			if finish {
				return true
			}
		}
	}
}
