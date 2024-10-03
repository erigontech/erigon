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

package statechange

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
