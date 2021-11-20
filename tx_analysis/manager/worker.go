package manager

import (
	"fmt"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/tx_analysis/minievm"
	log_ "github.com/ledgerwatch/log/v3"
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
	report *report
}

func newWorker(id int, stop chan bool, wg *sync.WaitGroup, r *report) *worker {
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

	logger := log_.New()
	db, err := mdbx.NewMDBX(logger).Path("/mnt/mx500_0/goerli/chaindata").Readonly().Open()
	if err != nil {
		fmt.Println(err)

	}
	defer db.Close()

	active := false
	stop := false
	for {
		select {
		case <-w.stop:
			fmt.Printf("Received STOP signal - worker id: %d. Tasks left: %d\n", w.id, len(w.tasks))
			stop = true
		case t := <-w.tasks:
			active = true
			blockN := t.header.Number.Uint64()
			fmt.Printf("worker-%d received task: { block : %d, idx: %d}\n", w.id, blockN, t.idx)
			minievm.Analize(t.header, t.tx)
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
