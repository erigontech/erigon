package rpctest

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/state"
)

var routes map[string]string

// bench1 compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
//
//	use false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Erigon
//
// fullTest - if false - then call only methods which RPCDaemon currently supports
func Bench1(erigonURL, gethURL string, needCompare bool, fullTest bool, blockFrom uint64, blockTo uint64, recordFile string) {
	setRoutes(erigonURL, gethURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	resultsCh := make(chan CallResult, 1000)
	defer close(resultsCh)
	go vegetaWrite(false, []string{"eth_getBlockByNumber", "debug_storageRangeAt"}, resultsCh)

	var res CallResult
	reqGen := &RequestGenerator{
		client: client,
	}

	reqGen.reqID++
	var blockNumber EthBlockNumber
	res = reqGen.Erigon("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	resultsCh <- res
	if res.Err != nil {
		fmt.Printf("Could not get block number: %v\n", res.Err)
		return
	}
	if blockNumber.Error != nil {
		fmt.Printf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
		return
	}
	fmt.Printf("Last block: %d\n", blockNumber.Number)
	accounts := make(map[libcommon.Address]struct{})
	prevBn := blockFrom
	storageCounter := 0
	for bn := blockFrom; bn <= blockTo; bn++ {
		reqGen.reqID++
		var b EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &b)
		resultsCh <- res
		if res.Err != nil {
			fmt.Printf("Could not retrieve block (Erigon) %d: %v\n", bn, res.Err)
			return
		}

		if b.Error != nil {
			fmt.Printf("Error retrieving block (Erigon): %d %s\n", b.Error.Code, b.Error.Message)
		}

		if needCompare {
			var bg EthBlockByNumber
			res = reqGen.Geth("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &bg)
			if res.Err != nil {
				fmt.Printf("Could not retrieve block (geth) %d: %v\n", bn, res.Err)
				return
			}
			if bg.Error != nil {
				fmt.Printf("Error retrieving block (geth): %d %s\n", bg.Error.Code, bg.Error.Message)
				return
			}
			if !compareBlocks(&b, &bg) {
				fmt.Printf("Block difference for %d\n", bn)
				return
			}
		}

		accounts[b.Result.Miner] = struct{}{}

		for i, tx := range b.Result.Transactions {
			accounts[tx.From] = struct{}{}
			if tx.To != nil {
				accounts[*tx.To] = struct{}{}
			}

			if tx.To != nil && tx.Gas.ToInt().Uint64() > 21000 {
				storageCounter++
				if storageCounter == 100 {
					storageCounter = 0
					nextKey := &libcommon.Hash{}
					nextKeyG := &libcommon.Hash{}
					sm := make(map[libcommon.Hash]storageEntry)
					smg := make(map[libcommon.Hash]storageEntry)
					for nextKey != nil {
						var sr DebugStorageRange
						reqGen.reqID++
						res = reqGen.Erigon("debug_storageRangeAt", reqGen.storageRangeAt(b.Result.Hash, i, tx.To, *nextKey), &sr)
						resultsCh <- res
						if res.Err != nil {
							fmt.Printf("Could not get storageRange (Erigon): %s: %v\n", tx.Hash, res.Err)
							return
						}
						if sr.Error != nil {
							fmt.Printf("Error getting storageRange: %d %s\n", sr.Error.Code, sr.Error.Message)
							return
						}

						for k, v := range sr.Result.Storage {
							sm[k] = v
							if v.Key == nil {
								fmt.Printf("%x: %x", k, v)
							}
						}
						nextKey = sr.Result.NextKey

					}

					if needCompare {
						for nextKeyG != nil {
							var srGeth DebugStorageRange
							res = reqGen.Geth("debug_storageRangeAt", reqGen.storageRangeAt(b.Result.Hash, i, tx.To, *nextKeyG), &srGeth)
							resultsCh <- res
							if res.Err != nil {
								fmt.Printf("Could not get storageRange (geth): %s: %v\n", tx.Hash, res.Err)
								return
							}
							if srGeth.Error != nil {
								fmt.Printf("Error getting storageRange (geth): %d %s\n", srGeth.Error.Code, srGeth.Error.Message)
								break
							} else {
								for k, v := range srGeth.Result.Storage {
									smg[k] = v
									if v.Key == nil {
										fmt.Printf("%x: %x", k, v)
									}
								}
								nextKeyG = srGeth.Result.NextKey
							}
						}
						if !compareStorageRanges(sm, smg) {
							fmt.Printf("len(sm) %d, len(smg) %d\n", len(sm), len(smg))
							fmt.Printf("================sm\n")
							printStorageRange(sm)
							fmt.Printf("================smg\n")
							printStorageRange(smg)
							return
						}
					}
				}
			}

			if !fullTest {
				continue // TODO: remove me
			}

			reqGen.reqID++

			var trace EthTxTrace
			res = reqGen.Erigon("debug_traceTransaction", reqGen.traceTransaction(tx.Hash), &trace)
			resultsCh <- res
			if res.Err != nil {
				fmt.Printf("Could not trace transaction (Erigon) %s: %v\n", tx.Hash, res.Err)
				print(client, routes[Erigon], reqGen.traceTransaction(tx.Hash))
			}

			if trace.Error != nil {
				fmt.Printf("Error tracing transaction (Erigon): %d %s\n", trace.Error.Code, trace.Error.Message)
			}

			if needCompare {
				var traceg EthTxTrace
				res = reqGen.Geth("debug_traceTransaction", reqGen.traceTransaction(tx.Hash), &traceg)
				resultsCh <- res
				if res.Err != nil {
					fmt.Printf("Could not trace transaction (geth) %s: %v\n", tx.Hash, res.Err)
					print(client, routes[Geth], reqGen.traceTransaction(tx.Hash))
					return
				}
				if traceg.Error != nil {
					fmt.Printf("Error tracing transaction (geth): %d %s\n", traceg.Error.Code, traceg.Error.Message)
					return
				}
				if res.Err == nil && trace.Error == nil {
					if !compareTraces(&trace, &traceg) {
						fmt.Printf("Different traces block %d, tx %s\n", bn, tx.Hash)
						return
					}
				}
			}
			reqGen.reqID++

			var receipt EthReceipt
			res = reqGen.Erigon("eth_getTransactionReceipt", reqGen.getTransactionReceipt(tx.Hash), &receipt)
			resultsCh <- res
			if res.Err != nil {
				fmt.Printf("Count not get receipt (Erigon): %s: %v\n", tx.Hash, res.Err)
				print(client, routes[Erigon], reqGen.getTransactionReceipt(tx.Hash))
				return
			}
			if receipt.Error != nil {
				fmt.Printf("Error getting receipt (Erigon): %d %s\n", receipt.Error.Code, receipt.Error.Message)
				return
			}
			if needCompare {
				var receiptg EthReceipt
				res = reqGen.Geth("eth_getTransactionReceipt", reqGen.getTransactionReceipt(tx.Hash), &receiptg)
				resultsCh <- res
				if res.Err != nil {
					fmt.Printf("Count not get receipt (geth): %s: %v\n", tx.Hash, res.Err)
					print(client, routes[Geth], reqGen.getTransactionReceipt(tx.Hash))
					return
				}
				if receiptg.Error != nil {
					fmt.Printf("Error getting receipt (geth): %d %s\n", receiptg.Error.Code, receiptg.Error.Message)
					return
				}
				if !compareReceipts(&receipt, &receiptg) {
					fmt.Printf("Different receipts block %d, tx %s\n", bn, tx.Hash)
					print(client, routes[Geth], reqGen.getTransactionReceipt(tx.Hash))
					print(client, routes[Erigon], reqGen.getTransactionReceipt(tx.Hash))
					return
				}
			}
		}
		if !fullTest {
			continue // TODO: remove me
		}
		reqGen.reqID++

		var balance EthBalance
		res = reqGen.Erigon("eth_getBalance", reqGen.getBalance(b.Result.Miner, bn), &balance)
		resultsCh <- res
		if res.Err != nil {
			fmt.Printf("Could not get account balance (Erigon): %v\n", res.Err)
			return
		}
		if balance.Error != nil {
			fmt.Printf("Error getting account balance (Erigon): %d %s", balance.Error.Code, balance.Error.Message)
			return
		}
		if needCompare {
			var balanceg EthBalance
			res = reqGen.Geth("eth_getBalance", reqGen.getBalance(b.Result.Miner, bn), &balanceg)
			resultsCh <- res
			if res.Err != nil {
				fmt.Printf("Could not get account balance (geth): %v\n", res.Err)
				return
			}
			if balanceg.Error != nil {
				fmt.Printf("Error getting account balance (geth): %d %s\n", balanceg.Error.Code, balanceg.Error.Message)
				return
			}
			if !compareBalances(&balance, &balanceg) {
				fmt.Printf("Miner %x balance difference for block %d\n", b.Result.Miner, bn)
				return
			}
		}

		if prevBn < bn && bn%100 == 0 {
			// Checking modified accounts
			reqGen.reqID++
			var mag DebugModifiedAccounts
			res = reqGen.Erigon("debug_getModifiedAccountsByNumber", reqGen.getModifiedAccountsByNumber(prevBn, bn), &mag)
			resultsCh <- res
			if res.Err != nil {
				fmt.Printf("Could not get modified accounts (Erigon): %v\n", res.Err)
				return
			}
			if mag.Error != nil {
				fmt.Printf("Error getting modified accounts (Erigon): %d %s\n", mag.Error.Code, mag.Error.Message)
				return
			}
			fmt.Printf("Done blocks %d-%d, modified accounts: %d\n", prevBn, bn, len(mag.Result))

			page := libcommon.Hash{}.Bytes()
			pageGeth := libcommon.Hash{}.Bytes()

			var accRangeErigon map[libcommon.Address]state.DumpAccount
			var accRangeGeth map[libcommon.Address]state.DumpAccount

			for len(page) > 0 {
				accRangeErigon = make(map[libcommon.Address]state.DumpAccount)
				accRangeGeth = make(map[libcommon.Address]state.DumpAccount)
				var sr DebugAccountRange
				reqGen.reqID++
				res = reqGen.Erigon("debug_accountRange", reqGen.accountRange(bn, page, 256), &sr)
				resultsCh <- res

				if res.Err != nil {
					fmt.Printf("Could not get accountRange (Erigon): %v\n", res.Err)
					return
				}

				if sr.Error != nil {
					fmt.Printf("Error getting accountRange (Erigon): %d %s\n", sr.Error.Code, sr.Error.Message)
					break
				} else {
					page = sr.Result.Next
					for k, v := range sr.Result.Accounts {
						accRangeErigon[k] = v
					}
				}
				if needCompare {
					var srGeth DebugAccountRange
					res = reqGen.Geth("debug_accountRange", reqGen.accountRange(bn, pageGeth, 256), &srGeth)
					resultsCh <- res
					if res.Err != nil {
						fmt.Printf("Could not get accountRange geth: %v\n", res.Err)
						return
					}
					if srGeth.Error != nil {
						fmt.Printf("Error getting accountRange geth: %d %s\n", srGeth.Error.Code, srGeth.Error.Message)
						break
					} else {
						pageGeth = srGeth.Result.Next
						for k, v := range srGeth.Result.Accounts {
							accRangeGeth[k] = v
						}
					}
					if !bytes.Equal(page, pageGeth) {
						fmt.Printf("Different next page keys: %x geth %x", page, pageGeth)
					}
					if !compareAccountRanges(accRangeErigon, accRangeGeth) {
						fmt.Printf("Different in account ranges tx\n")
						return
					}
				}
			}
			prevBn = bn
		}
	}
}

// vegetaWrite (to be run as a goroutine) writing results of server calls into several files:
// results to /$tmp$/erigon_stress_test/results_*.csv
// vegeta format going to files /$tmp$/erigon_stress_test/vegeta_*.txt
func vegetaWrite(enabled bool, methods []string, resultsCh chan CallResult) {
	var err error
	var files map[string]map[string]*os.File
	var vegetaFiles map[string]map[string]*os.File
	if enabled {
		files = map[string]map[string]*os.File{
			Geth:   make(map[string]*os.File),
			Erigon: make(map[string]*os.File),
		}
		vegetaFiles = map[string]map[string]*os.File{
			Geth:   make(map[string]*os.File),
			Erigon: make(map[string]*os.File),
		}
		tmpDir := os.TempDir()
		fmt.Printf("tmp dir is: %s\n", tmpDir)
		dir := filepath.Join(tmpDir, "erigon_stress_test")
		if err = os.MkdirAll(dir, 0770); err != nil {
			panic(err)
		}

		for _, route := range []string{Geth, Erigon} {
			for _, method := range methods {
				file := filepath.Join(dir, "results_"+route+"_"+method+".csv")
				files[route][method], err = os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
				if err != nil {
					panic(err)
				}
			}
		}

		for _, route := range []string{Geth, Erigon} {
			for _, method := range methods {
				file := filepath.Join(dir, "vegeta_"+route+"_"+method+".txt")
				vegetaFiles[route][method], err = os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
				if err != nil {
					panic(err)
				}
			}
		}
	}

	for res := range resultsCh {
		// If not enabled, simply keep draining the results channel
		if enabled {
			if res.Err != nil {
				fmt.Printf("error response. target: %s, err: %s\n", res.Target, res.Err)
			}
			// files with call stats
			if f, ok := files[res.Target][res.Method]; ok {
				row := fmt.Sprintf("%d, %s, %d\n", res.RequestID, res.Method, res.Took.Microseconds())
				if _, err := fmt.Fprint(f, row); err != nil {
					panic(err)
				}
			}

			// vegeta files, write into all target files
			// because if "needCompare" is false - then we don't have responses from Erigon
			// but we still have enough information to build vegeta file for Erigon
			for _, target := range []string{Geth, Erigon} {
				if f, ok := vegetaFiles[target][res.Method]; ok {
					template := `{"method": "POST", "url": "%s", "body": "%s", "header": {"Content-Type": ["application/json"]}}`
					row := fmt.Sprintf(template, routes[target], base64.StdEncoding.EncodeToString([]byte(res.RequestBody)))

					if _, err := fmt.Fprint(f, row+"\n"); err != nil {
						panic(err)
					}
				}
			}
		}
	}
}
