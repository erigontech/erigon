package rpctest

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"
)

func BenchTraceTransaction(erigonUrl, gethUrl string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string) {
	setRoutes(erigonUrl, gethUrl)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	var rec *bufio.Writer
	if recordFile != "" {
		f, err := os.Create(recordFile)
		if err != nil {
			fmt.Printf("Cannot create file %s for recording: %v\n", recordFile, err)
			return
		}
		defer f.Close()
		rec = bufio.NewWriter(f)
		defer rec.Flush()
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
			fmt.Printf("retrieve block (Erigon) %d: %v", blockFrom, res.Err)
			return
		}
		if b.Error != nil {
			fmt.Printf("retrieving block (Erigon): %d %s", b.Error.Code, b.Error.Message)
			return
		}
		for _, tx := range b.Result.Transactions {
			reqGen.reqID++

			request := reqGen.traceTransaction(tx.Hash)
			recording := rec != nil // This flag will be set to false if recording is not to be performed
			res = reqGen.Erigon2("debug_traceTransaction", request)

			if res.Err != nil {
				fmt.Printf("Could not trace transaction (Erigon) %s: %v\n", tx.Hash, res.Err)
				return
			}
			if errVal := res.Result.Get("error"); errVal != nil {
				fmt.Printf("Error tracing transaction (Erigon): %d %s\n", errVal.GetInt("code"), errVal.GetStringBytes("message"))
				return
			}

			if needCompare {
				resg := reqGen.Geth2("debug_traceTransaction", request)
				if resg.Err != nil {
					fmt.Printf("Could not trace transaction (geth) %s: %v\n", tx.Hash, res.Err)
					return
				}
				if errVal := resg.Result.Get("error"); errVal != nil {
					fmt.Printf("Error tracing transaction (geth): %d %s\n", errVal.GetInt("code"), errVal.GetStringBytes("message"))
					return
				}
				if resg.Err == nil && resg.Result.Get("error") == nil {
					if err := compareResults(res.Result, resg.Result); err != nil {
						fmt.Printf("Different traceTransaction block %d, tx %s: %v\n", bn, tx.Hash, err)
						fmt.Printf("\n\nTG response=================================\n%s\n", res.Response)
						fmt.Printf("\n\nG response=================================\n%s\n", resg.Response)
						return
					}
				}
			}
			if recording {
				fmt.Fprintf(rec, "%s\n%s\n\n", request, res.Response)
			}
		}
	}
}
