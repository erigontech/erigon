package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"log"

	"github.com/gorilla/websocket"
	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
	"github.com/ledgerwatch/erigon/tx_analysis/manager"
	"github.com/ledgerwatch/erigon/tx_analysis/utils"
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

	client := utils.NewHTTPClient(http_addr)

	conn, _, err := websocket.DefaultDialer.Dial(ws_addr, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

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
	defer signal.Stop(interrupt)

	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan bool)
	ready := make(chan bool)

	mng := manager.New()
	go mng.Start(cancel, stop, ready, *WORKERS)

	<-ready

	is_interrupt := false
	for {

		select {
		case <-interrupt:
			is_interrupt = true

			fmt.Println("Shutting down...")
			fmt.Println("Closing WebSocket connection...")

			err = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(
				websocket.CloseNormalClosure, ""))

			if err != nil {
				fmt.Println("Error closing connection: ", err)
			}

			stop <- true
		case <-ctx.Done():
			fmt.Println("Done...")
			return
		default:

			if !is_interrupt {

				var resp_h utils.JsonHeaderResp
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

				header := resp_h.Params.Result
				block_number := header.Number.Uint64()

				var t rpctest.EthBlockByNumber
				err = utils.GetBlockByNumber(&client, block_number, &t)
				if err != nil {
					fmt.Println("error block by number: ", err)
					continue
				}

				mng.AddSupply(&header, t.Result.Transactions)
			}

		}

	}
}
