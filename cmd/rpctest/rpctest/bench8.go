package rpctest

import (
	"bufio"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/ledgerwatch/erigon/log"
)

func Bench8(erigonURL, gethURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string) {
	setRoutes(erigonURL, gethURL)
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
	resultsCh := make(chan CallResult, 1000)
	defer close(resultsCh)
	go vegetaWrite(false, []string{"debug_getModifiedAccountsByNumber", "eth_getLogs"}, resultsCh)

	var res CallResult
	reqGen := &RequestGenerator{
		client: client,
	}

	reqGen.reqID++
	var blockNumber EthBlockNumber
	res = reqGen.Erigon("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	if res.Err != nil {
		fmt.Printf("Could not get block number: %v\n", res.Err)
		return
	}
	if blockNumber.Error != nil {
		fmt.Printf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
		return
	}
	fmt.Printf("Last block: %d\n", blockNumber.Number)

	prevBn := blockFrom
	rnd := rand.New(rand.NewSource(42)) // nolint:gosec
	for bn := blockFrom + 100; bn < blockTo; bn += 100 {

		// Checking modified accounts
		reqGen.reqID++
		var mag DebugModifiedAccounts
		res = reqGen.Erigon("debug_getModifiedAccountsByNumber", reqGen.getModifiedAccountsByNumber(prevBn, bn), &mag)
		if res.Err != nil {
			fmt.Printf("Could not get modified accounts (Erigon): %v\n", res.Err)
			return
		}
		if mag.Error != nil {
			fmt.Printf("Error getting modified accounts (Erigon): %d %s\n", mag.Error.Code, mag.Error.Message)
			return
		}
		if res.Err == nil && mag.Error == nil {
			accountSet := extractAccountMap(&mag)
			for account := range accountSet {
				reqGen.reqID++
				startErigon := time.Now()
				request := reqGen.getLogs(prevBn, bn, account)
				recording := rec != nil // This flag will be set to false if recording is not to be performed
				res = reqGen.Erigon2("eth_getLogs", request)
				durationErigon := time.Since(startErigon).Seconds()
				if res.Err != nil {
					fmt.Printf("Could not get logs for account (Erigon) %x: %v\n", account, res.Err)
					return
				}
				if errVal := res.Result.Get("error"); errVal != nil {
					fmt.Printf("Error getting logs for account (Erigon) %x: %d %s\n", account, errVal.GetInt("code"), errVal.GetStringBytes("message"))
					return
				}
				var durationG float64
				if needCompare {
					startG := time.Now()
					resg := reqGen.Geth2("eth_getLogs", request)
					durationG = time.Since(startG).Seconds()
					resultsCh <- res
					if resg.Err != nil {
						fmt.Printf("Could not get logs for account (geth) %x: %v\n", account, resg.Err)
						recording = false
					} else if errValg := resg.Result.Get("error"); errValg != nil {
						fmt.Printf("Error getting logs for account (geth) %x: %d %s\n", account, errValg.GetInt("code"), errValg.GetStringBytes("message"))
						recording = false
					} else {
						if err := compareResults(res.Result, resg.Result); err != nil {
							fmt.Printf("Different logs for account %x and block %d-%d\n", account, prevBn, bn)
							fmt.Printf("\n\nTG response=================================\n%s\n", res.Response)
							fmt.Printf("\n\nG response=================================\n%s\n", resg.Response)
							return
						}
					}
				}
				log.Info("Results", "count", len(res.Result.GetArray("result")), "durationErigon", durationErigon, "durationG", durationG)
				if recording {
					fmt.Fprintf(rec, "%s\n%s\n\n", request, res.Response)
				}
				topics := getTopics(res.Result)
				// All combination of account and one topic
				for _, topic := range topics {
					reqGen.reqID++
					startErigon := time.Now()
					request = reqGen.getLogs1(prevBn, bn+10000, account, topic)
					recording = rec != nil
					res = reqGen.Erigon2("eth_getLogs", request)
					durationErigon := time.Since(startErigon).Seconds()
					if res.Err != nil {
						fmt.Printf("Could not get logs for account (Erigon) %x %x: %v\n", account, topic, res.Err)
						return
					}
					if errVal := res.Result.Get("error"); errVal != nil {
						fmt.Printf("Error getting logs for account (Erigon) %x %x: %d %s\n", account, topic, errVal.GetInt("code"), errVal.GetStringBytes("message"))
						return
					}
					if needCompare {
						startG := time.Now()
						resg := reqGen.Geth2("eth_getLogs", request)
						durationG = time.Since(startG).Seconds()
						resultsCh <- res
						if resg.Err != nil {
							fmt.Printf("Could not get logs for account (geth) %x %x: %v\n", account, topic, resg.Err)
							recording = false
						} else if errValg := resg.Result.Get("error"); errValg != nil {
							fmt.Printf("Error getting logs for account (geth) %x %x: %d %s\n", account, topic, errValg.GetInt("code"), errValg.GetStringBytes("message"))
							recording = false
						} else {
							if err := compareResults(res.Result, resg.Result); err != nil {
								fmt.Printf("Different logs for account %x %x and block %d-%d\n", account, topic, prevBn, bn)
								fmt.Printf("\n\nTG response=================================\n%s\n", res.Response)
								fmt.Printf("\n\nG response=================================\n%s\n", resg.Response)
								return
							}
						}
					}
					if recording {
						fmt.Fprintf(rec, "%s\n%s\n\n", request, res.Response)
					}
					log.Info("Results", "count", len(res.Result.GetArray("result")), "durationErigon", durationErigon, "durationG", durationG)
				}
				// Random combinations of two topics
				if len(topics) >= 2 {
					idx1 := rnd.Int31n(int32(len(topics)))
					idx2 := rnd.Int31n(int32(len(topics) - 1))
					if idx2 >= idx1 {
						idx2++
					}
					reqGen.reqID++
					request = reqGen.getLogs2(prevBn, bn+100000, account, topics[idx1], topics[idx2])
					recording = rec != nil
					res = reqGen.Erigon2("eth_getLogs", request)
					if res.Err != nil {
						fmt.Printf("Could not get logs for account (Erigon) %x %x %x: %v\n", account, topics[idx1], topics[idx2], res.Err)
						return
					}
					if errVal := res.Result.Get("error"); errVal != nil {
						fmt.Printf("Error getting logs for account (Erigon) %x %x %x: %d %s\n", account, topics[idx1], topics[idx2], errVal.GetInt("code"), errVal.GetStringBytes("message"))
						return
					}
					if needCompare {
						resg := reqGen.Geth2("eth_getLogs", request)
						resultsCh <- res
						if resg.Err != nil {
							fmt.Printf("Could not get logs for account (geth) %x %x %x: %v\n", account, topics[idx1], topics[idx2], res.Err)
							recording = false
						} else if errValg := resg.Result.Get("error"); errValg != nil {
							fmt.Printf("Error getting logs for account (geth) %x %x %x: %d %s\n", account, topics[idx1], topics[idx2], errValg.GetInt("code"), errValg.GetStringBytes("message"))
							recording = false
						} else {
							if err := compareResults(res.Result, resg.Result); err != nil {
								fmt.Printf("Different logs for account %x %x %x and block %d-%d\n", account, topics[idx1], topics[idx2], prevBn, bn)
								fmt.Printf("\n\nTG response=================================\n%s\n", res.Response)
								fmt.Printf("\n\nG response=================================\n%s\n", resg.Response)
								return
							}
						}
					}
					if recording {
						fmt.Fprintf(rec, "%s\n%s\n\n", request, res.Response)
					}
					log.Info("Results", "count", len(res.Result.GetArray("result")))
				}
			}
		}
		fmt.Printf("Done blocks %d-%d, modified accounts: %d\n", prevBn, bn, len(mag.Result))
		prevBn = bn
	}
}
