package threading

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

type WorkerPool struct {
	work      chan func() error
	wg        sync.WaitGroup
	atomicErr unsafe.Pointer
}

// CreateWorkerPool initializes a pool of workers to process tasks.
func CreateWorkerPool(numWorkers int) *WorkerPool {
	wp := WorkerPool{
		work: make(chan func() error, 1000),
	}
	for i := 1; i <= numWorkers; i++ {
		go wp.StartWorker()
	}
	return &wp
}

// close work channel and finish
func (wp *WorkerPool) WaitAndClose() {
	// Wait for all workers to finish.
	wp.wg.Wait()
	// Close the task channel to indicate no more tasks will be sent.
	close(wp.work)
}

// Worker is the worker that processes tasks.
func (wp *WorkerPool) StartWorker() {
	for task := range wp.work {
		if err := task(); err != nil {
			atomic.StorePointer(&wp.atomicErr, unsafe.Pointer(&err))
		}
		wp.wg.Done()
	}
}

func (wp *WorkerPool) Error() error {
	errPointer := atomic.LoadPointer(&wp.atomicErr)
	if errPointer == nil {
		return nil
	}
	return *(*error)(errPointer)
}

// enqueue work
func (wp *WorkerPool) AddWork(f func() error) {
	wp.wg.Add(1)
	wp.work <- f
}

func ParallellForLoop(numWorkers int, from, to int, f func(int) error) error {
	// divide the work into numWorkers parts
	size := (to - from) / numWorkers
	wp := CreateWorkerPool(numWorkers)
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
	wp.WaitAndClose()
	return wp.Error()
}
