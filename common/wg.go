package common

import (
	"errors"
	"sync"
)

type WaitGroup struct {
	sync.RWMutex
	sync.WaitGroup
	isDone bool
}

var WaitGroupDone = errors.New("wg is already stopped")

func (wg *WaitGroup) Reset() {
	wg.Lock()
	defer wg.Unlock()
	wg.isDone = false
	wg.WaitGroup = sync.WaitGroup{}
}

func (wg *WaitGroup) Add(delta int) error {
	wg.Lock()
	defer wg.Unlock()
	if wg.isDone {
		return WaitGroupDone
	}
	wg.WaitGroup.Add(delta)
	return nil
}

func (wg *WaitGroup) Done() {
	wg.Lock()
	defer wg.Unlock()
	wg.WaitGroup.Done()
}

func (wg *WaitGroup) Wait() {
	wg.RLock()
	defer wg.RUnlock()
	wg.WaitGroup.Wait()
	wg.isDone = true
}