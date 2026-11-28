package stagedsync

import (
	"sync"

	"github.com/erigontech/erigon/execution/exec"
)

// execWorkerCache keeps the parallel executor's worker contexts between ExecV3 calls,
// so a call reuses their IntraBlockStates instead of building new ones.
type execWorkerCache struct {
	mu   sync.Mutex
	idle []*exec.WorkerContext
}

// take hands out the idle workers when there are n of them, or nil: the caller
// then builds its own. A nil cache never has idle workers.
func (c *execWorkerCache) take(n int) []*exec.WorkerContext {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	idle := c.idle
	c.idle = nil
	c.mu.Unlock()
	if len(idle) == n {
		return idle
	}
	closeWorkers(idle)
	return nil
}

// put parks workers that no task runs on and keeps them for the next take, or closes
// them when the cache is nil or already holds a set.
func (c *execWorkerCache) put(workers []*exec.WorkerContext) {
	if c != nil {
		c.mu.Lock()
		if c.idle == nil {
			for _, w := range workers {
				w.Park()
			}
			c.idle, workers = workers, nil
		}
		c.mu.Unlock()
	}
	closeWorkers(workers)
}

func closeWorkers(workers []*exec.WorkerContext) {
	for _, w := range workers {
		w.Close()
	}
}
