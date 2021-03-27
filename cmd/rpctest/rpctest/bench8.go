package rpctest

import (
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/ledgerwatch/turbo-geth/log"
)

func Bench8(tgURL, gethURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string) {
	setRoutes(tgURL, gethURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
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
	res = reqGen.TurboGeth("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	if res.Err != nil {
		fmt.Printf("Could not get block number: %v\n", res.Err)
		return
	}
	if blockNumber.Error != nil {
		fmt.Printf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
		return
	}
	lastBlock := uint64(blockNumber.Number)
	fmt.Printf("Last block: %d\n", blockNumber.Number)

	prevBn := blockFrom
	rnd := rand.New(rand.NewSource(42)) // nolint:gosec
	for bn := blockFrom; bn <= lastBlock-100000; bn++ {

		if prevBn < bn && bn%100 == 0 {
			// Checking modified accounts
			reqGen.reqID++
			var mag DebugModifiedAccounts
			res = reqGen.TurboGeth("debug_getModifiedAccountsByNumber", reqGen.getModifiedAccountsByNumber(prevBn, bn), &mag)
			if res.Err != nil {
				fmt.Printf("Could not get modified accounts (turbo-geth): %v\n", res.Err)
				return
			}
			if mag.Error != nil {
				fmt.Printf("Error getting modified accounts (turbo-geth): %d %s\n", mag.Error.Code, mag.Error.Message)
				return
			}
			if res.Err == nil && mag.Error == nil {
				accountSet := extractAccountMap(&mag)
				for account := range accountSet {
					reqGen.reqID++
					startTG := time.Now()
					res = reqGen.TurboGeth2("eth_getLogs", reqGen.getLogs(prevBn, bn, account))
					durationTG := time.Since(startTG).Seconds()
					if res.Err != nil {
						fmt.Printf("Could not get logs for account (turbo-geth) %x: %v\n", account, res.Err)
						return
					}
					if errVal := res.Result.Get("error"); errVal != nil {
						fmt.Printf("Error getting logs for account (turbo-geth) %x: %d %s\n", account, errVal.GetInt("code"), errVal.GetStringBytes("message"))
						return
					}
					var durationG float64
					if needCompare {
						startG := time.Now()
						resg := reqGen.Geth2("eth_getLogs", reqGen.getLogs(prevBn, bn, account))
						durationG = time.Since(startG).Seconds()
						resultsCh <- res
						if resg.Err != nil {
							fmt.Printf("Could not get logs for account (geth) %x: %v\n", account, resg.Err)
						} else if errValg := resg.Result.Get("error"); errValg != nil {
							fmt.Printf("Error getting logs for account (geth) %x: %d %s\n", account, errValg.GetInt("code"), errValg.GetStringBytes("message"))
						} else {
							if err := compareResults(res.Result, resg.Result); err != nil {
								fmt.Printf("Different logs for account %x and block %d-%d\n", account, prevBn, bn)
								fmt.Printf("\n\nTG response=================================\n%s\n", res.Response)
								fmt.Printf("\n\nG response=================================\n%s\n", resg.Response)
								return
							}
						}
					}
					log.Info("Results", "count", len(res.Result.GetArray("result")), "durationTG", durationTG, "durationG", durationG)
					topics := getTopics(res.Result)
					// All combination of account and one topic
					for _, topic := range topics {
						reqGen.reqID++
						startTG := time.Now()
						res = reqGen.TurboGeth2("eth_getLogs", reqGen.getLogs1(prevBn, bn+10000, account, topic))
						durationTG := time.Since(startTG).Seconds()
						if res.Err != nil {
							fmt.Printf("Could not get logs for account (turbo-geth) %x %x: %v\n", account, topic, res.Err)
							return
						}
						if errVal := res.Result.Get("error"); errVal != nil {
							fmt.Printf("Error getting logs for account (turbo-geth) %x %x: %d %s\n", account, topic, errVal.GetInt("code"), errVal.GetStringBytes("message"))
							return
						}
						if needCompare {
							var logsg1 EthLogs
							startG := time.Now()
							resg := reqGen.Geth("eth_getLogs", reqGen.getLogs1(prevBn, bn+10000, account, topic), &logsg1)
							durationG = time.Since(startG).Seconds()
							resultsCh <- res
							if resg.Err != nil {
								fmt.Printf("Could not get logs for account (geth) %x %x: %v\n", account, topic, resg.Err)
							} else if errValg := resg.Result.Get("error"); errValg != nil {
								fmt.Printf("Error getting logs for account (geth) %x %x: %d %s\n", account, topic, errValg.GetInt("code"), errValg.GetStringBytes("message"))
							} else {
								if err := compareResults(res.Result, resg.Result); err != nil {
									fmt.Printf("Different logs for account %x %x and block %d-%d\n", account, topic, prevBn, bn)
									fmt.Printf("\n\nTG response=================================\n%s\n", res.Response)
									fmt.Printf("\n\nG response=================================\n%s\n", resg.Response)
									return
								}
							}
						}
						log.Info("Results", "count", len(res.Result.GetArray("result")), "durationTG", durationTG, "durationG", durationG)
					}
					// Random combinations of two topics
					if len(topics) >= 2 {
						idx1 := rnd.Int31n(int32(len(topics)))
						idx2 := rnd.Int31n(int32(len(topics) - 1))
						if idx2 >= idx1 {
							idx2++
						}
						reqGen.reqID++
						res = reqGen.TurboGeth2("eth_getLogs", reqGen.getLogs2(prevBn, bn+100000, account, topics[idx1], topics[idx2]))
						if res.Err != nil {
							fmt.Printf("Could not get logs for account (turbo-geth) %x %x %x: %v\n", account, topics[idx1], topics[idx2], res.Err)
							return
						}
						if errVal := res.Result.Get("error"); errVal != nil {
							fmt.Printf("Error getting logs for account (turbo-geth) %x %x %x: %d %s\n", account, topics[idx1], topics[idx2], errVal.GetInt("code"), errVal.GetStringBytes("message"))
							return
						}
						if needCompare {
							resg := reqGen.Geth2("eth_getLogs", reqGen.getLogs2(prevBn, bn+100000, account, topics[idx1], topics[idx2]))
							resultsCh <- res
							if resg.Err != nil {
								fmt.Printf("Could not get logs for account (geth) %x %x %x: %v\n", account, topics[idx1], topics[idx2], res.Err)
							} else if errValg := resg.Result.Get("error"); errValg != nil {
								fmt.Printf("Error getting logs for account (geth) %x %x %x: %d %s\n", account, topics[idx1], topics[idx2], errValg.GetInt("code"), errValg.GetStringBytes("message"))
							} else {
								if err := compareResults(res.Result, resg.Result); err != nil {
									fmt.Printf("Different logs for account %x %x %x and block %d-%d\n", account, topics[idx1], topics[idx2], prevBn, bn)
									fmt.Printf("\n\nTG response=================================\n%s\n", res.Response)
									fmt.Printf("\n\nG response=================================\n%s\n", resg.Response)
									return
								}
							}
						}
						log.Info("Results", "count", len(res.Result.GetArray("result")))
					}
				}
			}
			fmt.Printf("Done blocks %d-%d, modified accounts: %d\n", prevBn, bn, len(mag.Result))
			prevBn = bn
		}
	}
}
