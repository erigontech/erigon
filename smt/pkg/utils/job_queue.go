package utils

import (
	"context"
	"sync/atomic"
)

type DB interface {
	InsertHashKey(key NodeKey, value NodeKey) error
	Insert(key NodeKey, value NodeValue12) error
	CollectSmt(key NodeKey, value NodeValue12)
	CollectHashKey(key NodeKey, value NodeKey)
}

type JobResult interface {
	GetError() error
	Save() error
}

type CalcAndPrepareJobResult struct {
	db         DB
	Err        error
	KvMap      map[[4]uint64]NodeValue12
	LeafsKvMap map[[4]uint64][4]uint64
}

func NewCalcAndPrepareJobResult(db DB) *CalcAndPrepareJobResult {
	return &CalcAndPrepareJobResult{
		db:         db,
		KvMap:      make(map[[4]uint64]NodeValue12),
		LeafsKvMap: make(map[[4]uint64][4]uint64),
	}
}

func (r *CalcAndPrepareJobResult) GetError() error {
	return r.Err
}

func (r *CalcAndPrepareJobResult) Save() error {
	for key, value := range r.LeafsKvMap {
		r.db.CollectHashKey(key, value)
	}
	for key, value := range r.KvMap {
		r.db.CollectSmt(key, value)
	}
	return nil
}

// Worker responsible for queue serving.
type Worker struct {
	ctx        context.Context
	name       string
	jobs       chan func() JobResult
	jobResults chan JobResult
	stopped    atomic.Bool
}

// NewWorker initializes a new Worker.
func NewWorker(ctx context.Context, name string, jobQueueSize int) *Worker {
	return &Worker{
		ctx,
		name,
		make(chan func() JobResult, jobQueueSize),
		make(chan JobResult, jobQueueSize),
		atomic.Bool{},
	}
}

func (w *Worker) AddJob(job func() JobResult) {
	w.jobs <- job
}

func (w *Worker) GetJobResultsChannel() chan JobResult {
	return w.jobResults
}

func (w *Worker) Stop() {
	close(w.jobs)
}

// DoWork processes jobs from the queue (jobs channel).
func (w *Worker) DoWork() {
	defer close(w.jobResults)

	for {
		select {
		case <-w.ctx.Done():
			return
		case job, ok := <-w.jobs:
			if !ok {
				return
			}

			jobRes := job()
			w.jobResults <- jobRes
		}
	}
}
