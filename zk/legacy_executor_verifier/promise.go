package legacy_executor_verifier

import (
	"fmt"
	"sync"
)

var ErrPromiseCancelled = fmt.Errorf("promise cancelled")

type Promise[T any] struct {
	result    T
	err       error
	wg        sync.WaitGroup
	mutex     sync.Mutex
	task      func() (T, error) // kept only if err != nil
	cancelled bool
}

func NewPromise[T any](task func() (T, error)) *Promise[T] {
	p := &Promise[T]{}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done() // this will be the second defer that is executed when the function retunrs

		result, err := task()
		p.mutex.Lock()
		defer p.mutex.Unlock() // this will be the first defer that is executed when the function retunrs

		if p.cancelled {
			err = ErrPromiseCancelled
		} else {
			p.result = result
			p.err = err
		}

		if err != nil {
			p.task = task
		}
	}()
	return p
}

func (p *Promise[T]) Get() (T, error) {
	p.wg.Wait() // .Wait ensures that all memory operations before .Done are visible after .Wait => no need to lock/unlock the mutex
	return p.result, p.err
}

func (p *Promise[T]) TryGet() (T, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.result, p.err
}

func (p *Promise[T]) Cancel() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.cancelled = true
}
