package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/gorilla/websocket"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
)

var (
	PORT    = flag.String("port", "8545", "RPC daemon port number")
	IP      = flag.String("ip", "127.0.0.1", "RPC daemon ip address")
	WORKERS = flag.Int("workers", 4, "number of parallel workers analizing transactions")
)

func main() {

	flag.Parse()

	http_addr := fmt.Sprintf("http://%s:%s", *IP, *PORT)
	ws_addr := fmt.Sprintf("ws://%s:%s", *IP, *PORT)

	client := new_http_client(http_addr)

	conn, _, err := websocket.DefaultDialer.Dial(ws_addr, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	// var wg sync.WaitGroup
	// rr := new_round_robin(*WORKERS)
	// var workers []*worker
	// for i := 0; i < *WORKERS; i++ {
	// 	stop := make(chan bool)
	// 	_worker := new_worker(i, stop, &wg)
	// 	wg.Add(1)
	// 	go _worker.start()
	// 	workers = append(workers, _worker)
	// }

	const template = `{"jsonrpc":"2.0","method":"eth_subscribe","params":["newHeads"],"id":%x}`
	query := fmt.Sprintf(template, 1)

	err = conn.WriteMessage(websocket.TextMessage, []byte(query))
	if err != nil {
		log.Fatalln(err)
	}

	_, _, err = conn.ReadMessage() // skip first response
	if err != nil {
		fmt.Println("Skip error: ", err)
		return
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	is_interrupt := false

	for {

		select {
		case <-interrupt:
			is_interrupt = true
			fmt.Println("Shutting down...")
			// for i := 0; i < *WORKERS; i++ {
			// 	workers[i].stop <- true
			// }
			_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
				websocket.CloseNormalClosure, ""))

			// fmt.Println("Waiting for threads to complete assigned jobs...")
			// fmt.Printf("Total jobs received: %d\n", total_transactions_received)

			// wg.Wait()
			return
		default:
			if !is_interrupt {
				var resp_h json_resp_header
				_, resp, err := conn.ReadMessage()
				if err != nil {
					log.Println("ReadJSON:", err)
					return
				}

				err = json.Unmarshal(resp, &resp_h)
				if err != nil {
					fmt.Println("Unmarshall err: ", err)
					return
				}

				_header := resp_h.Params.Result
				block_number := _header.Number.Uint64()

				var t rpctest.EthBlockByNumber
				err = getBlockByNumber(&client, block_number, &t)
				if err != nil {
					fmt.Println("error block by number: ", err)
					continue
				}

				fmt.Printf("block %d, transactions: %d\n\n",
					block_number, len(t.Result.Transactions))

				// Analize()

				for _, eth_tx := range t.Result.Transactions {
					minievm.Analize(eth_tx)
				}

			}

		}

	}
}
