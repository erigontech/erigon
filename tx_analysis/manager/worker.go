package manager

import (
	"fmt"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/tx_analysis/minievm"
)

type task struct {
	header *types.Header
	idx    int // tx index in a list of transactions
	tx     rpctest.EthTransaction
}

func newTask(header *types.Header, idx int, tx rpctest.EthTransaction) *task {
	return &task{
		header: header,
		idx:    idx,
		tx:     tx,
	}
}

type worker struct {
	id     int
	tasks  chan *task
	stop   chan bool // receive signal when entire program is interruped, shuttin down, etc...
	done   chan bool
	wg     *sync.WaitGroup
	errs   int
	report *Report
	sdb    *state.IntraBlockState
}

func newWorker(id int, stop chan bool, wg *sync.WaitGroup, r *Report) *worker {
	// var tasks [][]byte
	// totalTasks := 0
	// mu := &sync.Mutex{}
	// errs := 0
	// conn := &websocket.Conn{}
	return &worker{
		id:     id,
		tasks:  make(chan *task, 100),
		stop:   stop,
		done:   make(chan bool),
		wg:     wg,
		errs:   0,
		report: r,
	}
}

func (w *worker) start() {

	fmt.Printf("Starting a worker with id: %d...\n", w.id)

	active := false
	stop := false
	for {
		select {
		case <-w.stop:
			fmt.Printf("Received STOP signal - worker id: %d. Tasks left: %d\n", w.id, len(w.tasks))
			stop = true
		case t := <-w.tasks:
			active = true
			fmt.Printf("worker-%d received task: { idx: %d }\n", w.id, t.idx)
			minievm.Analize(t.header, t.tx, w.sdb)
			// w.report.add(t.idx, analysis)
		default:
			if stop && len(w.tasks) == 0 {
				w.wg.Done()
				return
			}

			if active && len(w.tasks) == 0 {
				active = false
				w.done <- true
			}

			time.Sleep(time.Millisecond * 50)
		}
	}
}

func (w *worker) assign(t *task) {
	w.tasks <- t
}
