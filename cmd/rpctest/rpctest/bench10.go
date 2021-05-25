package rpctest

import (
	"fmt"
	"net/http"
	"time"
)

func Bench10(tgUrl, gethUrl string, blockFrom uint64, blockTo uint64, recordFile string) error {
	setRoutes(tgUrl, gethUrl)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	var res CallResult
	reqGen := &RequestGenerator{
		client: client,
	}

	reqGen.reqID++

	for bn := blockFrom; bn < blockTo; bn++ {
		var b EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn), &b)
		if res.Err != nil {
			return fmt.Errorf("retrieve block (Erigon) %d: %v", blockFrom, res.Err)
		}
		if b.Error != nil {
			return fmt.Errorf("retrieving block (Erigon): %d %s", b.Error.Code, b.Error.Message)
		}
		for _, tx := range b.Result.Transactions {
			reqGen.reqID++

			var trace EthTxTrace
			res = reqGen.Erigon("debug_traceTransaction", reqGen.traceTransaction(tx.Hash), &trace)
			if res.Err != nil {
				fmt.Printf("Could not trace transaction (Erigon) %s: %v\n", tx.Hash, res.Err)
				print(client, routes[Erigon], reqGen.traceTransaction(tx.Hash))
			}

			if trace.Error != nil {
				fmt.Printf("Error tracing transaction (Erigon): %d %s\n", trace.Error.Code, trace.Error.Message)
			}

			var traceg EthTxTrace
			res = reqGen.Geth("debug_traceTransaction", reqGen.traceTransaction(tx.Hash), &traceg)
			if res.Err != nil {
				print(client, routes[Geth], reqGen.traceTransaction(tx.Hash))
				return fmt.Errorf("trace transaction (geth) %s: %v", tx.Hash, res.Err)
			}
			if traceg.Error != nil {
				return fmt.Errorf("tracing transaction (geth): %d %s", traceg.Error.Code, traceg.Error.Message)
			}
			if res.Err == nil && trace.Error == nil {
				if !compareTraces(&trace, &traceg) {
					return fmt.Errorf("different traces block %d, tx %s", blockFrom, tx.Hash)
				}
			}
			reqGen.reqID++
		}
	}
	return nil
}
