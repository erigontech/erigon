package legacy_executor_verifier

import "sync"

type Promise[T any] struct {
	result    T
	err       error
	wg        sync.WaitGroup
	mutex     sync.Mutex
	ExtraInfo interface{}
}

func NewPromise[T any](f func() (T, error)) *Promise[T] {
	p := &Promise[T]{}
	p.wg.Add(1)
	go func() {
		result, err := f()
		p.mutex.Lock()
		p.result = result
		p.err = err
		p.mutex.Unlock()
		p.wg.Done()
	}()
	return p
}

func (p *Promise[T]) Get(f func(r T) error) (T, error) {
	p.wg.Wait()
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.err == nil && f != nil {
		return p.result, f(p.result)
	}
	return p.result, p.err
}

func (p *Promise[T]) GetNonBlocking() (T, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.result, p.err
}
