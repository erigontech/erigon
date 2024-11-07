package threading

import (
	"sync"
)

type ParallelExecutor struct {
	jobs []func() error
	wg   sync.WaitGroup
}

// CreateWorkerPool initializes a pool of workers to process tasks.
func NewParallelExecutor() *ParallelExecutor {
	return &ParallelExecutor{}
}

// close work channel and finish
func (wp *ParallelExecutor) Execute() error {
	var errOut error
	for _, job := range wp.jobs {
		wp.wg.Add(1)
		go func(job func() error) {
			defer wp.wg.Done()
			if err := job(); err != nil {
				errOut = err
			}
		}(job)
	}
	wp.wg.Wait()
	return errOut
}

// enqueue work
func (wp *ParallelExecutor) AddWork(f func() error) {
	wp.jobs = append(wp.jobs, f)
}

func ParallellForLoop(numWorkers int, from, to int, f func(int) error) error {
	// divide the work into numWorkers parts
	size := (to - from) / numWorkers
	wp := ParallelExecutor{}
	for i := 0; i < numWorkers; i++ {
		start := from + i*size
		end := start + size
		if i == numWorkers-1 {
			end = to
		}
		wp.AddWork(func() error {
			for j := start; j < end; j++ {
				if err := f(j); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return wp.Execute()
}
