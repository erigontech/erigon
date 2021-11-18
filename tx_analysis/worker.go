package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
)

// Round-robin-ish very basic algo. Keeps track of job assignment order
type round_robin struct {
	max     int
	current int
}

func new_round_robin(max int) *round_robin {
	current := 0
	return &round_robin{max, current}
}

// get next worker in order
func (rr *round_robin) get_next() int {
	if rr.current >= rr.max {
		rr.current = 0
	}

	current := rr.current
	rr.current++
	return current
}

type job struct {
	tx    rpctest.EthTransaction
	block uint64 // block number
}

type worker struct {
	id         int
	queue      []job
	stop       chan bool
	wg         *sync.WaitGroup
	mu         sync.Mutex
	total_jobs int
	errs       int
}

func new_worker(id int, stop chan bool, wg *sync.WaitGroup) *worker {
	// var queue [][]byte
	// total_jobs := 0
	// mu := &sync.Mutex{}
	// errs := 0
	// conn := &websocket.Conn{}
	return &worker{
		id:         id,
		queue:      make([]job, 0, 10),
		stop:       stop,
		wg:         wg,
		mu:         sync.Mutex{},
		total_jobs: 0,
		errs:       0,
	}
}

func (w *worker) start() {

	stop := false
	for {
		select {
		case <-w.stop:
			fmt.Printf("Received STOP signal - worker id: %d. Jobs left: %d\n", w.id, len(w.queue))
			stop = true
		default:
			q_len := len(w.queue)
			if q_len > 0 {
				w.mu.Lock()
				// next_job := w.queue[0]
				w.queue = w.queue[1:]
				w.mu.Unlock()

				// hash := to_struct.Params.Result
				// rawdb.ReadTransaction(*w.tx, hash)
				// fmt.Printf("worker id: %d, got hash: %v\n", w.id, to_struct.Params.Result)
				// if to_struct.Params.Result != "" {
				// 	to_json := json_payload{
				// 		Id:     1,
				// 		Method: "eth_getTransactionByHash",
				// 		Params: []string{to_struct.Params.Result},
				// 	}

				// 	json_bytes, err := json.Marshal(to_json)
				// 	if err != nil {
				// 		fmt.Println("MARSHALL ERR: ", err)
				// 		continue
				// 	}
				// 	err = w.conn.WriteMessage(websocket.TextMessage, json_bytes)
				// 	if err != nil {
				// 		fmt.Println("WRITE MSG ERR: ", err)
				// 		continue
				// 	}

				// 	_, resp, err := w.conn.ReadMessage()
				// 	if err != nil {
				// 		fmt.Println("READ MSG ERR: ", err)
				// 		continue
				// 	}

				// 	fmt.Printf("worker-id: %d, resp: %s\n\n", w.id, resp)
				// }

				w.total_jobs++
				continue
			}
			// time.Sleep(time.Millisecond * 10)

			if stop && q_len == 0 {
				fmt.Printf("%d jobs completed by worker id: %d\n", w.total_jobs, w.id)
				w.wg.Done()
				return
			}

			// dont burn CPU if there is no jobs in queue yet
			time.Sleep(time.Millisecond * 50)
		}
	}
}

func (w *worker) enqueue(_job job) {
	w.mu.Lock()
	w.queue = append(w.queue, _job)
	w.mu.Unlock()
}
