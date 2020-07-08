package common

import (
	"errors"
	"sync"
	"sync/atomic"
)

type WaitGroup struct {
	sync.WaitGroup
	isDone uint32
}

var ErrWaitGroupDone = errors.New("wg is already stopped")

func (wg *WaitGroup) Reset() {
	atomic.StoreUint32(&wg.isDone, 0)
	wg.WaitGroup = sync.WaitGroup{}
}

func (wg *WaitGroup) Add(delta int) error {
	if atomic.LoadUint32(&wg.isDone) == 1 {
		return ErrWaitGroupDone
	}
	wg.WaitGroup.Add(delta)
	return nil
}

func (wg *WaitGroup) Done() {
	wg.WaitGroup.Done()
}

func (wg *WaitGroup) Wait() {
	wg.WaitGroup.Wait()
	atomic.StoreUint32(&wg.isDone, 1)
}
