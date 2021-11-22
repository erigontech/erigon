package manager

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	log_ "github.com/ledgerwatch/log/v3"
)

type supply struct {
	header       *types.Header
	transactions []rpctest.EthTransaction
}

func newSupply(header *types.Header, txs []rpctest.EthTransaction) *supply {
	return &supply{
		header:       header,
		transactions: txs,
	}
}

// Round-robin-ish very basic algo. Keeps track of job assignment order
type roundRobin struct {
	max     int // upper limit
	current int
}

func newRR(workers int) *roundRobin {
	return &roundRobin{max: workers}
}

// get next worker in order
func (rr *roundRobin) next() int {
	if rr.current >= rr.max {
		rr.current = 0
	}

	current := rr.current
	rr.current++
	return current
}

// Receives "supply" from main thread
// Manages task distribution between workers
type Manager struct {
	mu      sync.Mutex
	supply  chan *supply
	workers []*worker
	rr      *roundRobin
	report  *Report
	reports chan<- *Report
}

func New(reports chan<- *Report) *Manager {
	return &Manager{
		mu:      sync.Mutex{},
		supply:  make(chan *supply, 10),
		workers: make([]*worker, 0),
		report:  newReport(),
		reports: reports,
	}
}

func (m *Manager) Start(cancel context.CancelFunc, stop chan bool, ready chan bool, numWorkers int) {

	m.rr = newRR(numWorkers)

	wg := sync.WaitGroup{}

	logger := log_.New()
	db, err := mdbx.NewMDBX(logger).Path("/mnt/mx500_0/goerli/chaindata").Readonly().Open()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.BeginRo(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		ch_stop := make(chan bool) // chanel to notify worker to stop executing
		w := newWorker(i, ch_stop, &wg, m.report)
		m.workers = append(m.workers, w)
	}

	for _, worker := range m.workers {
		go worker.start()
	}

	close(ready) // TODO reconsider this, close this in main tread

	for {

		select {
		case <-stop:

			for _, w := range m.workers {
				w.stop <- true
			}

			wg.Wait()
			cancel()
		case sup := <-m.supply:

			header := sup.header
			blockN := header.Number.Uint64()

			reader := state.NewPlainState(tx, blockN)
			sdb := state.New(reader)

			for _, w := range m.workers {
				w.sdb = sdb.Copy()
			}

			fmt.Printf("\n\nSupply received... block: %d, transactions: %d\n", blockN, len(sup.transactions))
			fmt.Println("Distributing tasks...")

			m.report.reset(len(sup.transactions), header)

			toWaitOn := make([]bool, numWorkers)
			for idx, tx := range sup.transactions {
				next := m.rr.next()
				m.workers[next].assign(newTask(header, idx, tx))
				toWaitOn[next] = true
			}

			// wait for current block's analysis to finish
			for i, w := range m.workers {
				if toWaitOn[i] {
					<-w.done
				}
			}

			// m.report.write()
			m.reports <- m.report
		}
	}
}

func (m *Manager) AddSupply(header *types.Header, txs []rpctest.EthTransaction) {
	s := newSupply(header, txs)
	m.supply <- s
}
